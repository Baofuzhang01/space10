from utils import AES_Encrypt, enc, generate_captcha_key, verify_param
import json
import requests
import re
import time
import logging
import datetime
import os
from urllib.parse import urlparse, parse_qs, unquote
from urllib3.exceptions import InsecureRequestWarning
from requests.adapters import HTTPAdapter
from .time_utils import get_beijing_date, parse_times_range, resolve_request_day

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, use system environment variables instead


def _should_save_captcha_debug_images() -> bool:
    """是否保存验证码图片到本地，默认关闭。"""
    return os.getenv("SAVE_CAPTCHA_DEBUG_IMAGES", "0").lower() in {"1", "true", "yes", "on"}


def _get_tulingcloud_config():
    """从环境变量或 config.json 获取图灵云配置。
    
    优先级：
    1. 环境变量（GitHub Actions）
    2. config.json（本地开发）
    
    返回:
        (username, password, model_id) 元组，如果未配置则返回空字符串
    """
    # 优先从环境变量读取
    username = os.getenv("TULINGCLOUD_USERNAME", "")
    password = os.getenv("TULINGCLOUD_PASSWORD", "")
    model_id = os.getenv("TULINGCLOUD_MODEL_ID", "")
    
    # 如果环境变量中没有，尝试从 config.json 读取
    if not all([username, password, model_id]):
        try:
            config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                    tuling_config = config.get("tulingcloud", {})
                    username = username or tuling_config.get("username", "")
                    password = password or tuling_config.get("password", "")
                    model_id = model_id or tuling_config.get("model_id", "")
        except Exception as e:
            logging.debug(f"Failed to read tulingcloud config from config.json: {e}")
    
    return username, password, model_id


def get_date(day_offset: int = 0):
    """基于北京时间获取日期字符串，避免时区混乱。"""
    return get_beijing_date(day_offset)


class CredentialRejectedError(RuntimeError):
    """超星明确拒绝登录凭证时抛出，要求外层立即终止程序。"""


class OfficeTraceHTTPAdapter(HTTPAdapter):
    """在 send() 层记录 office.chaoxing.com 请求的连接池信息。"""

    def __init__(self, owner, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.owner = owner

    @staticmethod
    def _snapshot_pool(pool, url: str) -> dict:
        parsed = urlparse(str(url or ""))
        return {
            "pool_key": f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else "",
            "pool_id": hex(id(pool)) if pool is not None else "",
            "num_connections": getattr(pool, "num_connections", None),
            "num_requests": getattr(pool, "num_requests", None),
        }

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        trace_context = getattr(self.owner, "_connection_trace_context", None)
        should_trace = bool(
            trace_context and "office.chaoxing.com" in str(getattr(request, "url", ""))
        )

        before_pool = None
        before_state = self._snapshot_pool(None, getattr(request, "url", ""))
        if should_trace:
            try:
                before_pool = self.get_connection_with_tls_context(
                    request, verify, proxies=proxies, cert=cert
                )
                before_state = self._snapshot_pool(before_pool, request.url)
            except Exception as e:
                before_state["error"] = str(e)

        response = super().send(
            request,
            stream=stream,
            timeout=timeout,
            verify=verify,
            cert=cert,
            proxies=proxies,
        )

        if should_trace:
            response_pool = (
                getattr(response.raw, "_pool", None)
                or getattr(response.raw, "_connection", None)
                or before_pool
            )
            after_state = self._snapshot_pool(response_pool, request.url)
            trace = {
                "kind": trace_context.get("kind", ""),
                "method": getattr(request, "method", ""),
                "url": getattr(request, "url", ""),
                "status_code": getattr(response, "status_code", None),
                "before": before_state,
                "after": after_state,
            }
            self.owner._record_office_request_trace(trace)

        return response


class reserve:
    def __init__(
        self,
        sleep_time=0.2,
        max_attempt=50,
        enable_slider=False,
        enable_textclick=False,
        reserve_next_day=False,
        reserve_day_offset=None,
    ):
        self.login_page = (
            "https://passport2.chaoxing.com/mlogin?loginType=1&newversion=true&fid="
        )
        self.api_urls = {
            "seatengine": {
                "select": (
                    "https://office.chaoxing.com/front/third/apps/seatengine/select?"
                    "id={roomId}&day={day}&backLevel=2&seatId={seatPageId}&fidEnc={fidEnc}"
                ),
                "submit": "https://office.chaoxing.com/data/apps/seatengine/submit",
                "seat": "https://office.chaoxing.com/data/apps/seatengine/getusedtimes",
                "code": "https://office.chaoxing.com/front/third/apps/seatengine/code",
            },
            "seat": {
                "select": (
                    "https://office.chaoxing.com/front/third/apps/seat/select?"
                    "id={roomId}&day={day}&backLevel=2&seatId={seatPageId}&fidEnc={fidEnc}"
                ),
                "submit": "https://office.chaoxing.com/data/apps/seat/submit",
                "seat": "https://office.chaoxing.com/data/apps/seat/getusedtimes",
                "code": "https://office.chaoxing.com/front/third/apps/seat/code",
            },
        }
        self.api_family = "seatengine"
        self.url = ""
        self.submit_url = ""
        self.seat_url = ""
        self.code_url = ""
        self.login_url = "https://passport2.chaoxing.com/fanyalogin"
        self.token = ""
        self.success_times = 0
        self.fail_dict = []
        self.submit_msg = []
        self.last_submit_result = None
        self.requests = requests.session()
        self._office_trace_adapter = OfficeTraceHTTPAdapter(self)
        self.requests.mount("https://office.chaoxing.com/", self._office_trace_adapter)
        self.request_timeout = (
            float(os.getenv("CX_CONNECT_TIMEOUT", "3.05")),
            float(os.getenv("CX_READ_TIMEOUT", "5")),
        )
        self.fast_probe_timeout = (
            float(os.getenv("CX_FAST_PROBE_CONNECT_TIMEOUT", "0.36")),
            float(os.getenv("CX_FAST_PROBE_READ_TIMEOUT", "0.36")),
        )
        self.request_attempts = max(1, int(os.getenv("CX_REQUEST_ATTEMPTS", "3")))
        self.request_retry_delay = float(os.getenv("CX_REQUEST_RETRY_DELAY", "0.2"))
        self.token_fetch_retry_delay = float(
            os.getenv("CX_TOKEN_FETCH_RETRY_DELAY", "0.005")
        )
        self.headers = {
            "Referer": "https://office.chaoxing.com/",
            "Host": "captcha.chaoxing.com",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        }
        self.login_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "accept-encoding": "gzip, deflate, br, zstd",
            "cache-control": "no-cache",
            "Connection": "keep-alive",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.3 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1 wechatdevtools/1.05.2109131 MicroMessenger/8.0.5 Language/zh_CN webview/16364215743155638",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Host": "passport2.chaoxing.com",
        }

        self.sleep_time = sleep_time
        self.max_attempt = max_attempt
        self.enable_slider = enable_slider
        self.enable_textclick = enable_textclick
        self.reserve_next_day = reserve_next_day
        self.reserve_day_offset = reserve_day_offset
        self._captcha_context = {}
        self._connection_trace_context = None
        self._warm_request_trace = {}
        preferred_family = str(os.getenv("CX_SEAT_API_MODE", "seat")).strip().lower()
        if preferred_family == "auto":
            preferred_family = "seatengine"
        self._set_api_family(preferred_family if preferred_family in self.api_urls else "seatengine")
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    def _set_api_family(self, family: str):
        family = str(family or "").strip().lower()
        if family not in self.api_urls:
            family = "seatengine"
        urls = self.api_urls[family]
        self.api_family = family
        self.url = urls["select"]
        self.submit_url = urls["submit"]
        self.seat_url = urls["seat"]
        self.code_url = urls["code"]

    def _alternate_api_family(self, family: str | None = None) -> str:
        current = str(family or self.api_family or "seatengine").strip().lower()
        return "seat" if current == "seatengine" else "seatengine"

    def _build_select_url_for_family(
        self,
        family: str,
        roomid: str,
        day: str,
        seat_page_id: str | None,
        fid_enc: str | None,
    ) -> str:
        return self.api_urls[family]["select"].format(
            roomId=roomid,
            day=str(day),
            seatPageId=seat_page_id or "",
            fidEnc=fid_enc or "",
        )

    def _get_select_url_candidates(self, url: str) -> list[tuple[str, str]]:
        urls = []
        current_family = self.api_family
        raw = str(url or "")
        if raw:
            detected = current_family
            if "/seatengine/" in raw:
                detected = "seatengine"
            elif "/apps/seat/" in raw or "/data/apps/seat/" in raw:
                detected = "seat"
            urls.append((detected, raw))

        for family in [self.api_family, self._alternate_api_family(self.api_family)]:
            candidate = self.api_urls[family]["select"]
            if not any(existing_url == candidate for _, existing_url in urls):
                urls.append((family, candidate))
        return urls

    def _submit_with_fallback(self, parm: dict, *, request_name: str):
        families = [self.api_family, self._alternate_api_family(self.api_family)]
        tried = set()

        for family in families:
            submit_url = self.api_urls[family]["submit"]
            if submit_url in tried:
                continue
            tried.add(submit_url)
            try:
                response = self._post(
                    url=submit_url,
                    data=parm,
                    verify=False,
                    request_name=request_name,
                )
            except requests.exceptions.RequestException as e:
                logging.warning(f"Seat submit request failed via {family}: {e}")
                continue

            html = response.content.decode("utf-8")
            try:
                data = json.loads(html)
            except ValueError as e:
                logging.warning(
                    f"Failed to parse seat submit response via {family}: {e}; body={html[:200]}"
                )
                continue

            if family != self.api_family:
                logging.info(f"Seat submit fallback switched API family to {family}")
                self._set_api_family(family)
            return data

        return None

    def set_captcha_context(
        self,
        *,
        roomid: str | None = None,
        seat_num: str | None = None,
        day: str | None = None,
        seat_page_id: str | None = None,
        fid_enc: str | None = None,
    ):
        self._captcha_context = {
            "roomid": str(roomid or "").strip(),
            "seat_num": str(seat_num or "").strip(),
            "day": str(day or "").strip(),
            "seat_page_id": str(seat_page_id or "").strip(),
            "fid_enc": str(fid_enc or "").strip(),
        }

    def _build_captcha_referer(self):
        ctx = self._captcha_context or {}
        roomid = ctx.get("roomid", "")
        seat_num = ctx.get("seat_num", "")
        day = ctx.get("day", "")
        seat_page_id = ctx.get("seat_page_id", "")
        fid_enc = ctx.get("fid_enc", "")

        params = [("id", roomid), ("seatNum", seat_num)]
        if day:
            params.append(("day", day))
        if seat_page_id:
            params.append(("seatId", seat_page_id))
        if fid_enc:
            params.append(("fidEnc", fid_enc))

        query = "&".join(
            f"{key}={value}"
            for key, value in params
            if value
        )
        referer = self.code_url or self.api_urls[self.api_family]["code"]
        if query:
            referer = f"{referer}?{query}"
        logging.debug(f"Using captcha referer: {referer}")
        return referer

    @staticmethod
    def _is_terminal_submit_failure(msg: str) -> bool:
        return "已有预约" in msg or "已被占用" in msg

    @staticmethod
    def _get_token_page_msg(url_like: str = "") -> str:
        parsed = urlparse(str(url_like or ""))
        msg_values = parse_qs(parsed.query).get("msg", [])
        for value in msg_values:
            decoded = unquote(str(value or ""))
            if decoded:
                return decoded
        return ""

    @classmethod
    def _is_token_page_not_open(
        cls,
        response_url: str = "",
        *,
        status_code: int | None = None,
        location: str = "",
    ) -> bool:
        raw_url = str(response_url or "")
        raw_location = str(location or "")
        msg = cls._get_token_page_msg(raw_location) or cls._get_token_page_msg(raw_url)

        if status_code in {301, 302, 303, 307, 308} and "当前区域未到开放预约时间" in msg:
            return True

        return (
            "当前区域未到开放预约时间" in msg
            or "msg=%E5%BD%93%E5%89%8D%E5%8C%BA%E5%9F%9F%E6%9C%AA%E5%88%B0%E5%BC%80%E6%94%BE%E9%A2%84%E7%BA%A6%E6%97%B6%E9%97%B4" in raw_location
            or "msg=%E5%BD%93%E5%89%8D%E5%8C%BA%E5%9F%9F%E6%9C%AA%E5%88%B0%E5%BC%80%E6%94%BE%E9%A2%84%E7%BA%A6%E6%97%B6%E9%97%B4" in raw_url
        )

    @staticmethod
    def _extract_submit_enc(html: str) -> str:
        """从页面 HTML 中提取 submit_enc。"""
        token_matches = re.findall(
            r'(?:id|name)\s*=\s*["\']submit_enc["\'][^>]*?value\s*=\s*["\'](.*?)["\']',
            html or "",
        )
        return token_matches[0] if token_matches else ""

    def _record_office_request_trace(self, trace: dict):
        """记录发送层连接追踪信息。"""
        kind = str(trace.get("kind", "")).strip()
        before = trace.get("before", {}) or {}
        after = trace.get("after", {}) or {}

        if kind == "warm":
            self._warm_request_trace = trace
            logging.info(
                "[warm] 连接追踪：连接池=%s，池对象=%s，发送前连接数=%s，请求数=%s，发送后连接数=%s，请求数=%s，状态码=%s",
                after.get("pool_key") or before.get("pool_key") or "未知",
                after.get("pool_id") or before.get("pool_id") or "未知",
                before.get("num_connections"),
                before.get("num_requests"),
                after.get("num_connections"),
                after.get("num_requests"),
                trace.get("status_code"),
            )
            return

        if kind == "first_fast_probe":
            logging.info(
                "第一枪轻探测连接复用：%s",
                self._describe_first_probe_reuse_from_trace(trace),
            )

    def _describe_first_probe_reuse_from_trace(self, probe_trace: dict) -> str:
        """基于发送层 trace 判断第一枪是否复用了预热连接。"""
        warm_trace = self._warm_request_trace or {}
        warm_after = warm_trace.get("after", {}) or {}
        probe_before = probe_trace.get("before", {}) or {}
        probe_after = probe_trace.get("after", {}) or {}

        pool_key = (
            probe_after.get("pool_key")
            or probe_before.get("pool_key")
            or warm_after.get("pool_key")
            or "未知"
        )
        warm_pool_id = warm_after.get("pool_id") or ""
        probe_pool_id = probe_after.get("pool_id") or probe_before.get("pool_id") or ""
        warm_conn = warm_after.get("num_connections")
        warm_req = warm_after.get("num_requests")
        probe_before_conn = probe_before.get("num_connections")
        probe_before_req = probe_before.get("num_requests")
        probe_after_conn = probe_after.get("num_connections")
        probe_after_req = probe_after.get("num_requests")

        if warm_pool_id and probe_pool_id and warm_pool_id != probe_pool_id:
            return (
                f"连接池={pool_key}，是否复用=否，原因=预热池对象 {warm_pool_id}"
                f" 与第一枪池对象 {probe_pool_id} 不同；预热后连接数/请求数={warm_conn}/{warm_req}"
                f"，第一枪发送前={probe_before_conn}/{probe_before_req}，发送后={probe_after_conn}/{probe_after_req}"
            )

        if all(isinstance(v, int) for v in [warm_conn, warm_req, probe_after_conn, probe_after_req]):
            if (
                probe_after_conn == warm_conn
                and probe_after_req == warm_req + 1
                and warm_conn >= 1
            ):
                return (
                    f"连接池={pool_key}，是否复用=是，原因=预热后连接数保持 {warm_conn}，"
                    f"请求数 {warm_req}->{probe_after_req}；池对象={probe_pool_id or warm_pool_id or '未知'}"
                )
            if isinstance(probe_before_conn, int) and probe_after_conn > probe_before_conn:
                return (
                    f"连接池={pool_key}，是否复用=否，原因=第一枪发送时连接数 {probe_before_conn}->{probe_after_conn}"
                    f" 增加；池对象={probe_pool_id or warm_pool_id or '未知'}"
                )
            if probe_after_conn == warm_conn and probe_after_req > warm_req:
                return (
                    f"连接池={pool_key}，是否复用=是，原因=预热后连接数保持 {warm_conn}，"
                    f"请求数 {warm_req}->{probe_after_req}；池对象={probe_pool_id or warm_pool_id or '未知'}"
                )

        return (
            f"连接池={pool_key}，是否复用=未知，原因=发送层计数仍不足以确认；"
            f"预热后池对象/连接数/请求数={warm_pool_id or '未知'}/{warm_conn}/{warm_req}，"
            f"第一枪发送前={probe_before_conn}/{probe_before_req}，发送后={probe_after_conn}/{probe_after_req}，"
            f"第一枪池对象={probe_pool_id or '未知'}"
        )

    def should_skip_followup_submit(self) -> bool:
        if not isinstance(self.last_submit_result, dict):
            return False
        msg = str(self.last_submit_result.get("msg", ""))
        return self._is_terminal_submit_failure(msg)

    def _request_with_retry(
        self,
        method,
        url,
        *,
        request_name="request",
        attempts=None,
        retry_delay=None,
        **kwargs,
    ):
        attempts = attempts if attempts is not None else self.request_attempts
        retry_delay = (
            retry_delay if retry_delay is not None else self.request_retry_delay
        )
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.request_timeout

        last_exc = None
        for attempt in range(1, attempts + 1):
            try:
                return self.requests.request(method=method, url=url, **kwargs)
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                if attempt >= attempts:
                    logging.error(
                        f"{request_name} failed after {attempts} attempt(s): {exc}"
                    )
                    raise
                logging.warning(
                    f"{request_name} failed on attempt {attempt}/{attempts}: {exc}; "
                    f"retrying in {retry_delay:.2f}s"
                )
                time.sleep(retry_delay)

        raise last_exc

    def _get(self, url, **kwargs):
        return self._request_with_retry("GET", url, **kwargs)

    def _post(self, url, **kwargs):
        return self._request_with_retry("POST", url, **kwargs)

    def probe_not_open_fast(self, url, *, log_connection_reuse: bool = False):
        """轻量探测选座页是否仍处于“未开放”状态。

        只发起一次 GET。
        若最终页面明确是“未开放”，则立即返回并继续下轮探测；
        若判断为已开放，则直接尝试复用本次响应 HTML 中的 submit_enc。

        返回:
            {
                "is_not_open": bool,
                "token": str,
                "value": str,
            }
        """
        if log_connection_reuse:
            self._connection_trace_context = {"kind": "first_fast_probe"}
        try:
            response = self._get(
                url=url,
                verify=False,
                allow_redirects=False,
                timeout=self.fast_probe_timeout,
                attempts=1,
                request_name="seat page fast not-open probe",
            )
        except requests.exceptions.RequestException as e:
            logging.warning(
                f"Fast not-open probe failed for {url}: {e}; "
                "treat as open and switch to formal token fetch"
            )
            return {"is_not_open": False, "token": "", "value": ""}
        finally:
            if log_connection_reuse:
                self._connection_trace_context = None

        response_url = getattr(response, "url", "")
        status_code = getattr(response, "status_code", None)
        location = response.headers.get("Location", "")
        if self._is_token_page_not_open(
            response_url=response_url,
            status_code=status_code,
            location=location,
        ):
            response.close()
            return {"is_not_open": True, "token": "", "value": ""}

        html = response.content.decode("utf-8", errors="ignore")
        response.close()
        token = self._extract_submit_enc(html)
        if token:
            return {"is_not_open": False, "token": token, "value": token}
        return {"is_not_open": False, "token": "", "value": ""}

    # login and page token
    def _get_page_token(
        self,
        url,
        require_value: bool = False,
        method: str = "GET",
        data=None,
        not_open_retry_until=None,
        not_open_retry_interval: float | None = None,
    ):
        """从页面提取提交用的 token。

        新版页面只有一个隐藏字段 submit_enc，不再有单独的 algorithm。
        实测行为是：submit_enc 既作为页面 token，也作为 enc 算法的"算法值"。
        因此这里直接用 submit_enc 作为两者。

        参数:
            url: seatengine/select 页面地址
            require_value: 是否返回算法值（即 submit_enc 本身）
            method: "GET" 或 "POST"，允许按前端实现切换请求方式
            data: 当使用 POST 时提交的表单数据
            not_open_retry_until: 若页面返回“未到开放时间”，持续重试到该时刻
            not_open_retry_interval: “未到开放时间”重试间隔（秒）
        """
        last_html = ""
        attempt = 0
        not_open_retry_interval = (
            float(not_open_retry_interval)
            if not_open_retry_interval is not None
            else self.token_fetch_retry_delay
        )

        last_response_url = ""

        url_candidates = self._get_select_url_candidates(url)

        for candidate_family, candidate_url in url_candidates:
            attempt = 0
            while True:
                attempt += 1
                try:
                    if method.upper() == "POST":
                        response = self._post(
                            url=candidate_url,
                            data=data or {},
                            verify=False,
                            request_name="seat page token fetch",
                        )
                    else:
                        response = self._get(
                            url=candidate_url,
                            verify=False,
                            request_name="seat page token fetch",
                        )
                except requests.exceptions.RequestException as e:
                    logging.warning(f"Failed to fetch seat page token from {candidate_url}: {e}")
                    break

                final_url = getattr(response, "url", "")
                last_response_url = final_url

                html = response.content.decode("utf-8", errors="ignore")
                last_html = html

                token = self._extract_submit_enc(html)
                if token:
                    algorithm_value = token if require_value else ""
                    if candidate_family != self.api_family:
                        logging.info(
                            f"Get page token fallback switched API family to {candidate_family}"
                        )
                        self._set_api_family(candidate_family)
                    if attempt > 1:
                        logging.info(
                            f"Get page token from {candidate_url} succeeded on retry attempt {attempt}: {token}"
                        )
                    return token, algorithm_value

                not_open_yet = self._is_token_page_not_open(response_url=final_url)
                page_msg = self._get_token_page_msg(final_url)
                if not_open_retry_until is not None:
                    now = (
                        datetime.datetime.now(not_open_retry_until.tzinfo)
                        if getattr(not_open_retry_until, "tzinfo", None)
                        else datetime.datetime.now()
                    )
                    if now < not_open_retry_until:
                        remaining_s = max(
                            0.0, (not_open_retry_until - now).total_seconds()
                        )
                        sleep_s = min(not_open_retry_interval, remaining_s)
                        if not_open_yet:
                            logging.warning(
                                f"Get page token from {candidate_url} hit not-open-yet page on retry "
                                f"{attempt}; keep refreshing for {remaining_s:.3f}s more "
                                f"(sleep {sleep_s:.3f}s, msg={page_msg})"
                            )
                        else:
                            logging.warning(
                                f"Get page token from {candidate_url} returned no submit_enc on retry "
                                f"{attempt}; keep refreshing for {remaining_s:.3f}s more "
                                f"(sleep {sleep_s:.3f}s)"
                            )
                        if sleep_s > 0:
                            time.sleep(sleep_s)
                        continue
                    logging.warning(
                        f"Get page token from {candidate_url} stop refreshing after retry {attempt}: "
                        f"reached retry deadline {not_open_retry_until}"
                    )
                break

        # 取不到 token 时：
        # 1. 控制台打印部分页面内容
        # 2. 将完整 HTML 保存到 html_debug 目录，方便你用浏览器打开对比前端结构
        snippet = last_html[:500].replace("\n", " ")
        if self._is_token_page_not_open(response_url=last_response_url):
            logging.error(
                f"Failed to get token from {url}: page stayed in not-open-yet state after "
                f"{attempt} retries, html snippet: {snippet}..."
            )
        else:
            logging.error(f"Failed to get token from {url}, html snippet: {snippet}...")
        try:
            debug_dir = os.path.join(os.path.dirname(__file__), "..", "html_debug")
            os.makedirs(debug_dir, exist_ok=True)
            ts = int(time.time() * 1000)
            filename = os.path.join(debug_dir, f"{self.api_family}_{ts}.html")
            with open(filename, "w", encoding="utf-8") as f:
                f.write(last_html)
            logging.error(f"Full HTML of seat page saved to {filename}")
        except Exception as e:
            logging.warning(f"Failed to save debug HTML for seat page: {e}")
        return "", ""

    def warm_connection(self, url):
        """预热 TCP+TLS 连接池。

        发送一次和获取 token 完全相同的真实 GET 请求，结果直接丢弃。
        requests.Session 底层使用 urllib3 连接池，相同 host 的后续请求可复用已建立的连接，
        跳过 TCP 三次握手 + TLS 协商，节省约 100-200ms。
        """
        self._connection_trace_context = {"kind": "warm"}
        try:
            logging.info(f"[warm] Start connection pre-warm request via {url}")
            self._get(
                url=url,
                verify=False,
                timeout=5,
                attempts=1,
                request_name="[warm] connection pre-warm",
            )
        except Exception:
            pass
        finally:
            self._connection_trace_context = None

    def get_login_status(self, attempts=None):
        self.requests.headers = self.login_headers
        self._get(
            url=self.login_page,
            verify=False,
            request_name="login page bootstrap",
            attempts=attempts,
        )

    def login(self, username, password, attempts=None):
        username = AES_Encrypt(username)
        password = AES_Encrypt(password)
        parm = {
            "fid": -1,
            "uname": username,
            "password": password,
            "refer": "http%3A%2F%2Foffice.chaoxing.com%2Ffront%2Fthird%2Fapps%2Fseat%2Fcode%3Fid%3D4219%26seatNum%3D380",
            "t": True,
        }
        response = self._post(
            url=self.login_url,
            params=parm,
            verify=False,
            request_name="Chaoxing login submit",
            attempts=attempts,
        )
        try:
            obj = response.json()
        except ValueError as e:
            logging.error(f"Failed to parse Chaoxing login response: {e}")
            return (False, "invalid login response")
        if obj["status"]:
            logging.info(f"User {username} login successfully")
            return (True, "")
        else:
            logging.info(
                f"User {username} login failed. Please check you password and username! "
            )
            return (False, obj["msg2"])

    @staticmethod
    def _is_fatal_login_rejection(msg: str) -> bool:
        msg = str(msg or "")
        fatal_markers = (
            "账号密码错误",
            "密码错误",
            "用户名或密码错误",
            "错误输入",
            "被冻结",
        )
        return any(marker in msg for marker in fatal_markers)

    def bootstrap_login(self, username, password, attempts=None):
        """建立登录态；遇到短暂网络错误时返回 False，由外层继续调度。"""
        try:
            self.get_login_status(attempts=attempts)
            success, msg = self.login(username, password, attempts=attempts)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to bootstrap login session for {username}: {e}")
            return False

        if not success:
            if self._is_fatal_login_rejection(msg):
                raise CredentialRejectedError(
                    f"Login rejected for {username}: {msg}"
                )
            logging.warning(f"Login bootstrap rejected for {username}: {msg}")
            return False

        self.requests.headers.update({"Host": "office.chaoxing.com"})
        return True

    # extra: get roomid
    def roomid(self, encode):
        url = f"https://office.chaoxing.com/data/apps/seat/room/list?cpage=1&pageSize=100&firstLevelName=&secondLevelName=&thirdLevelName=&deptIdEnc={encode}"
        json_data = self._get(url=url, request_name="room list fetch").content.decode("utf-8")
        try:
            ori_data = json.loads(json_data)
        except ValueError as e:
            logging.error(f"Failed to parse room list response: {e}")
            return
        for i in ori_data["data"]["seatRoomList"]:
            info = f'{i["firstLevelName"]}-{i["secondLevelName"]}-{i["thirdLevelName"]} id为：{i["id"]}'
            print(info)

    # solve captcha

    def resolve_captcha(self, captcha_type="slide"):
        """统一的验证码求解入口。
        
        参数:
            captcha_type: "slide"（滑块）或 "textclick"（选字）
        """
        if captcha_type == "slide":
            return self._resolve_slide_captcha()
        elif captcha_type == "textclick":
            return self._resolve_textclick_captcha()
        else:
            logging.error(f"Unknown captcha type: {captcha_type}")
            return ""

    def _resolve_slide_captcha(self):
        """滑块验证码求解。"""
        logging.info(f"Start to resolve slide captcha token")
        captcha_token, bg, tp = self.get_slide_captcha_data()
        if not captcha_token or not bg or not tp:
            logging.warning("Failed to get slide captcha payload")
            return ""
        logging.info(f"Successfully get prepared captcha_token {captcha_token}")
        logging.info(f"Captcha Image URL-small {tp}, URL-big {bg}")
        x = self.x_distance(bg, tp)
        if x is None:
            logging.warning("Failed to download or parse slide captcha images")
            return ""
        logging.info(f"Successfully calculate the captcha distance {x}")

        return self._submit_captcha("slide", captcha_token, [{"x": x}])

    def _resolve_textclick_captcha(self):
        """选字验证码求解。"""
        logging.info("Start to resolve textclick captcha token")
        captcha_token, image_url, target_text = self.get_textclick_captcha_data()
        if not captcha_token or not image_url or not target_text:
            logging.warning("Failed to get textclick captcha payload")
            return ""
        logging.info(f"Successfully get prepared captcha_token {captcha_token}")
        logging.info(f"Target text raw: {target_text}")
        
        positions = self._recognize_textclick_positions(image_url, target_text)
        if not positions:
            logging.warning("Failed to recognize text positions")
            return ""
        
        logging.info(f"Successfully recognize positions: {positions}")
        
        return self._submit_captcha("textclick", captcha_token, positions)

    @staticmethod
    def _parse_textclick_target_chars(target_text: str):
        """尽量稳健地从题面里提取需要按顺序点击的文字。"""
        raw = str(target_text or "").strip()
        if not raw:
            return []

        quote_patterns = [
            r'"([^"]+)"',
            r"“([^”]+)”",
            r"‘([^’]+)’",
            r"「([^」]+)」",
            r"『([^』]+)』",
        ]
        for pattern in quote_patterns:
            matches = [m.strip() for m in re.findall(pattern, raw) if m and m.strip()]
            if matches:
                chars = []
                for part in matches:
                    chars.extend([c for c in part if c.strip()])
                if chars:
                    return chars

        normalized = raw
        for phrase in [
            "请按顺序点击",
            "请依次点击",
            "请顺序点击",
            "依次点击",
            "顺序点击",
            "点击",
            "文字",
            "汉字",
            "字符",
            "下列",
            "以下",
            "图中",
            "图片中",
            "请点击",
            ":",
            "：",
        ]:
            normalized = normalized.replace(phrase, " ")
        normalized = re.sub(r"[\s,，.。;；、\-\[\]\(\)]+", " ", normalized)
        chars = re.findall(r"[\u3400-\u4dbf\u4e00-\u9fff]", normalized)
        return chars

    def _submit_captcha(self, captcha_type, captcha_token, click_array):
        """统一的验证码提交逻辑。
        
        参数:
            captcha_type: "slide" 或 "textclick"
            captcha_token: 验证码 token
            click_array: [{"x": x}] 或 [{"x": x1, "y": y1}, ...]
        """
        params = {
            "callback": "jQuery33109180509737430778_1716381333117",
            "captchaId": "42sxgHoTPTKbt0uZxPJ7ssOvtXr3ZgZ1",
            "type": captcha_type,
            "token": captcha_token,
            "textClickArr": json.dumps(click_array),
            "coordinate": json.dumps([]),
            "runEnv": "10",
            "version": "1.1.20" if captcha_type == "textclick" else "1.1.18",
            "_": int(time.time() * 1000),
        }
        logging.debug(f"Submit captcha params: {params}")
        try:
            response = self._get(
                f"https://captcha.chaoxing.com/captcha/check/verification/result",
                params=params,
                headers=self.headers,
                request_name=f"{captcha_type} captcha submit",
            )
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to submit {captcha_type} captcha: {e}")
            return ""
        text = response.text.replace(
            "jQuery33109180509737430778_1716381333117(", ""
        ).replace(")", "")
        try:
            data = json.loads(text)
        except ValueError as e:
            logging.error(f"Failed to parse {captcha_type} captcha response: {e}")
            return ""
        logging.info(f"Successfully resolve the captcha token: {data}")
        if not data.get("result"):
            logging.warning(f"{captcha_type} captcha server rejected click array: {click_array}")
        try:
            validate_val = json.loads(data["extraData"])["validate"]
            return validate_val
        except (KeyError, ValueError) as e:
            logging.info("Can't load validate value. Maybe server return mistake.")
            return ""

    def get_textclick_captcha_data(self):
        """获取选字验证码数据。"""
        url = "https://captcha.chaoxing.com/captcha/get/verification/image"
        timestamp = int(time.time() * 1000)
        capture_key, token = generate_captcha_key(timestamp, captcha_type="textclick")
        referer = self._build_captcha_referer()
        params = {
            "callback": "jQuery33107685004390294206_1716461324846",
            "captchaId": "42sxgHoTPTKbt0uZxPJ7ssOvtXr3ZgZ1",
            "type": "textclick",
            "version": "1.1.20",
            "captchaKey": capture_key,
            "token": token,
            "referer": referer,
            "_": timestamp,
            "d": "a",
            "b": "a",
        }
        try:
            response = self._get(
                url=url,
                params=params,
                headers=self.headers,
                request_name="textclick captcha fetch",
            )
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch textclick captcha data: {e}")
            return "", "", ""
        content = response.text

        data = content.replace(
            "jQuery33107685004390294206_1716461324846(", ""
        ).replace(")", "")
        try:
            data = json.loads(data)
        except ValueError as e:
            logging.error(f"Failed to parse textclick captcha payload: {e}")
            return "", "", ""
        captcha_token = data["token"]
        vo = data.get("imageVerificationVo", {})
        
        image_url = vo.get("originImage") or vo.get("shadeImage") or ""
        target_text = vo.get("context") or data.get("clickText") or ""
        logging.info(
            "Fetched textclick captcha payload: "
            f"image_url={'yes' if image_url else 'no'}, "
            f"target_text={target_text!r}"
        )
        
        return captcha_token, image_url, target_text

    def _recognize_textclick_positions(self, image_url, target_text):
        """识别选字验证码中的文字位置。
        
        使用图灵云打码平台进行OCR识别。
        
        参数:
            image_url: 验证码图片 URL
            target_text: 需要点击的文字（格式如：'"朝" "阳" "系"'）
            
        返回:
            按目标文字顺序的坐标列表：[{"x": x1, "y": y1}, {"x": x2, "y": y2}, {"x": x3, "y": y3}]
        """
        try:
            import urllib.request
            import os
            import time as _time
            from utils.tulingcloud_ocr import TulingCloudOCR
        except ImportError as e:
            logging.error(f"Missing required modules: {e}")
            return None
        
        # 下载验证码图片
        try:
            headers = {
                "Referer": "https://office.chaoxing.com/",
                "User-Agent": self.headers["User-Agent"],
            }
            req = urllib.request.Request(image_url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                img_bytes = response.read()
        except Exception as e:
            logging.error(f"Failed to download captcha image: {e}")
            return None
        
        # 可选调试：默认不落盘，避免影响关键路径时延
        if _should_save_captcha_debug_images():
            try:
                ts = int(_time.time() * 1000)
                debug_dir = os.path.join(os.path.dirname(__file__), "..", "captcha_debug")
                os.makedirs(debug_dir, exist_ok=True)
                img_path = os.path.join(debug_dir, f"textclick_{ts}.jpg")
                with open(img_path, "wb") as f:
                    f.write(img_bytes)
                logging.debug(f"Saved textclick captcha image to {img_path}")
            except Exception as e:
                logging.debug(f"Failed to save captcha image: {e}")
        
        # 使用图灵云打码平台进行OCR识别
        try:
            # 从环境变量或 config.json 读取图灵云凭证
            tuling_username, tuling_password, tuling_model_id = _get_tulingcloud_config()
            
            if not all([tuling_username, tuling_password, tuling_model_id]):
                logging.error("TulingCloud credentials not properly configured")
                logging.error("Set TULINGCLOUD_USERNAME, TULINGCLOUD_PASSWORD, TULINGCLOUD_MODEL_ID in env or config.json")
                return None
            
            logging.debug(f"TulingCloud config - username: {tuling_username[:6]}..., model_id: {tuling_model_id}")
            
            ocr = TulingCloudOCR(
                username=tuling_username,
                password=tuling_password,
                model_id=tuling_model_id
            )
            
            # 调用打码平台进行OCR识别
            ocr_result = ocr.recognize_textclick(img_bytes)
            
            if not ocr_result:
                logging.warning("TulingCloud failed to recognize text")
                return None
            
            if isinstance(ocr_result, dict):
                recognized_text = ocr_result.get("text", "")
                coordinates = ocr_result.get("coordinates")
                raw_ocr_result = ocr_result.get("raw_result")
            else:
                recognized_text = ocr_result
                coordinates = None
                raw_ocr_result = None
            
            if not recognized_text:
                logging.warning("TulingCloud returned empty text")
                return None
            
            logging.info(f"TulingCloud recognized text: {recognized_text}")
            logging.info(f"Target text to find: {target_text}")
            if raw_ocr_result is not None:
                logging.debug(f"TulingCloud raw_result: {raw_ocr_result}")
            
            if not coordinates:
                logging.error(f"TulingCloud did not return coordinates")
                return None
            
            target_chars = self._parse_textclick_target_chars(target_text)
            if not target_chars:
                logging.warning(
                    f"Could not parse target characters from textclick prompt: {target_text!r}"
                )
                return None
            
            logging.info(f"Parsed target characters: {target_chars}")
            
            recognized_chars = [char for char in str(recognized_text) if char.strip()]
            normalized_coordinates = []
            for idx, coord in enumerate(coordinates):
                if not isinstance(coord, dict):
                    continue
                normalized_coord = dict(coord)
                if not normalized_coord.get("text") and idx < len(recognized_chars):
                    normalized_coord["text"] = recognized_chars[idx]
                normalized_coordinates.append(normalized_coord)

            coordinates = normalized_coordinates
            logging.info(f"Normalized OCR coordinates: {coordinates}")

            result_positions = []
            used_indices = set()
            
            for target_char in target_chars:
                found = False
                for idx, coord in enumerate(coordinates):
                    recognized_char = str(coord.get("text") or "")
                    if (
                        recognized_char == target_char
                        and idx not in used_indices
                        and coord.get("x") is not None
                        and coord.get("y") is not None
                    ):
                        result_positions.append({
                            "x": int(coord["x"]),
                            "y": int(coord["y"]),
                        })
                        used_indices.add(idx)
                        logging.info(
                            f"Matched target '{target_char}' with OCR item #{idx}: {coord}"
                        )
                        found = True
                        break
                
                if not found:
                    logging.warning(f"Target character '{target_char}' not found in recognized text '{recognized_text}'")
                    logging.warning(f"Discarding this captcha recognition, will retry with new captcha")
                    return None
            
            if len(result_positions) == len(target_chars):
                logging.info(f"Final positions for target {target_chars}: {result_positions}")
                return result_positions
            else:
                logging.error(f"Could not find all target characters. Found {len(result_positions)}/{len(target_chars)}")
                return None
            
        except Exception as e:
            logging.error(f"FateADM recognition failed: {e}")
            import traceback
            logging.debug(traceback.format_exc())
            return None

    def get_slide_captcha_data(self):
        url = "https://captcha.chaoxing.com/captcha/get/verification/image"
        timestamp = int(time.time() * 1000)
        capture_key, token = generate_captcha_key(timestamp)
        referer = self._build_captcha_referer()
        params = {
            "callback": f"jQuery33107685004390294206_1716461324846",
            "captchaId": "42sxgHoTPTKbt0uZxPJ7ssOvtXr3ZgZ1",
            "type": "slide",
            "version": "1.1.18",
            "captchaKey": capture_key,
            "token": token,
            "referer": referer,
            "_": timestamp,
            "d": "a",
            "b": "a",
        }
        try:
            response = self._get(
                url=url,
                params=params,
                headers=self.headers,
                request_name="slide captcha fetch",
            )
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed to fetch slide captcha data: {e}")
            return "", "", ""
        content = response.text

        data = content.replace(
            "jQuery33107685004390294206_1716461324846(", ")"
        ).replace(")", "")
        try:
            data = json.loads(data)
        except ValueError as e:
            logging.error(f"Failed to parse slide captcha payload: {e}")
            return "", "", ""
        captcha_token = data["token"]
        bg = data["imageVerificationVo"]["shadeImage"]
        tp = data["imageVerificationVo"]["cutoutImage"]
        return captcha_token, bg, tp

    def x_distance(self, bg, tp):
        import numpy as np
        import cv2
        import os
        import time as _time

        def cut_slide(slide):
            slider_array = np.frombuffer(slide, np.uint8)
            slider_image = cv2.imdecode(slider_array, cv2.IMREAD_UNCHANGED)
            slider_part = slider_image[:, :, :3]
            mask = slider_image[:, :, 3]
            mask[mask != 0] = 255
            x, y, w, h = cv2.boundingRect(mask)
            cropped_image = slider_part[y : y + h, x : x + w]
            return cropped_image

        c_captcha_headers = {
            "Referer": "https://office.chaoxing.com/",
            "Host": "captcha-b.chaoxing.com",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        }

        def _download_image_with_retry(url: str, label: str, max_retries: int = 3, timeout_s: float = 5.0):
            for attempt in range(1, max_retries + 1):
                started = _time.time()
                try:
                    resp = self._get(
                        url,
                        headers=c_captcha_headers,
                        timeout=timeout_s,
                        attempts=1,
                        request_name=f"{label} image download",
                    )
                    resp.raise_for_status()
                    elapsed = _time.time() - started
                    if elapsed > timeout_s:
                        logging.warning(
                            f"{label} image download exceeded {timeout_s}s on attempt {attempt} "
                            f"({elapsed:.3f}s), retrying"
                        )
                        continue
                    return resp.content
                except requests.exceptions.Timeout:
                    logging.warning(
                        f"{label} image download timeout (> {timeout_s}s), retry {attempt}/{max_retries}"
                    )
                except Exception as e:
                    logging.warning(
                        f"{label} image download failed on retry {attempt}/{max_retries}: {e}"
                    )
            return None

        bg_bytes = _download_image_with_retry(bg, "Background")
        tp_bytes = _download_image_with_retry(tp, "Puzzle")
        if not bg_bytes or not tp_bytes:
            logging.error("Slide captcha image download failed after retries")
            return None

        # 可选调试：默认不落盘，避免频繁 IO 和日志噪音
        if _should_save_captcha_debug_images():
            try:
                ts = int(_time.time() * 1000)
                debug_dir = os.path.join(os.path.dirname(__file__), "..", "captcha_debug")
                os.makedirs(debug_dir, exist_ok=True)
                bg_path = os.path.join(debug_dir, f"bg_{ts}.jpg")
                tp_path = os.path.join(debug_dir, f"tp_{ts}.png")
                with open(bg_path, "wb") as f:
                    f.write(bg_bytes)
                with open(tp_path, "wb") as f:
                    f.write(tp_bytes)
                logging.debug(f"Saved captcha images to {bg_path} and {tp_path}")
            except Exception as e:
                logging.debug(f"Failed to save captcha images: {e}")

        bg_img = cv2.imdecode(np.frombuffer(bg_bytes, np.uint8), cv2.IMREAD_COLOR)
        tp_img = cut_slide(tp_bytes)
        bg_edge = cv2.Canny(bg_img, 100, 200)
        tp_edge = cv2.Canny(tp_img, 100, 200)
        bg_pic = cv2.cvtColor(bg_edge, cv2.COLOR_GRAY2RGB)
        tp_pic = cv2.cvtColor(tp_edge, cv2.COLOR_GRAY2RGB)
        res = cv2.matchTemplate(bg_pic, tp_pic, cv2.TM_CCOEFF_NORMED)
        _, _, _, max_loc = cv2.minMaxLoc(res)
        tl = max_loc
        return tl[0]

    def _build_submit_payload(
        self,
        times,
        roomid,
        seatid,
        captcha="",
        *,
        use_custom_day=False,
    ):
        normalized_times = parse_times_range(times)
        day = resolve_request_day(
            normalized_times,
            self.reserve_next_day,
            use_custom_day=use_custom_day,
            reserve_day_offset=self.reserve_day_offset,
        )
        parm = {
            "roomId": roomid,
            "startTime": normalized_times[0],
            "endTime": normalized_times[1],
            "day": day,
            "seatNum": seatid,
            "captcha": captcha,
            "wyToken": "",
        }
        logging.info(
            "submit parameter resolved: raw_times=%s, use_custom_day=%s, resolved_day=%s, submit_param=%s",
            times,
            bool(use_custom_day),
            day,
            parm,
        )
        return normalized_times, day, parm

    def submit(
        self,
        times,
        roomid,
        seatid,
        action,
        endtime_hms: str | None = None,
        fidEnc: str | None = None,
        seat_page_id: str | None = None,
        use_custom_day=False,
    ):
        """提交预约。

        关键点：为了模拟手动“刷新页面再提交”，这里每次尝试前都会重新访问
        seatengine/select 页面，获取当下最新的 submit_enc 作为 token/algorithm。

        参数:
            times: [startTime, endTime]
            roomid: 房间 id
            seatid: 座位号列表
            action: 是否为 action 场景（保留原逻辑使用）
            endtime_hms: 结束时间（北京时间 HH:MM:SS），用于 GitHub Actions 提前停止
            fidEnc: 对应前端 URL 中的 fidEnc 参数（例如 "dac916902610d220"）
            seat_page_id: 对应前端 URL 中的 seatId 参数（例如 "3308"）
        """
        normalized_times = parse_times_range(times)
        request_day = resolve_request_day(
            normalized_times,
            self.reserve_next_day,
            use_custom_day=use_custom_day,
            reserve_day_offset=self.reserve_day_offset,
        )
        
        # 每次调用 submit 时重置 max_attempt，确保每个配置都有充足的重试机会
        original_max_attempt = self.max_attempt

        for seat in seatid:
            # 为每个座位重置尝试次数
            self.max_attempt = original_max_attempt
            suc = False
            while ~suc and self.max_attempt > 0:
                # 如果配置了结束时间，并且在 GitHub Actions 模式下，达到或超过结束时间就立刻停止循环
                if endtime_hms and action:
                    beijing_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
                    current_hms = beijing_now.strftime("%H:%M:%S")
                    if current_hms >= endtime_hms:
                        logging.info(
                            f"[submit] Current Beijing time {current_hms} >= ENDTIME {endtime_hms}, stop submit loop"
                        )
                        return suc

                # 使用 seatengine/select 页面获取 submit_enc，相当于手动刷新选座页
                page_url = self.url.format(
                    roomId=roomid,
                    day=request_day,
                    seatPageId=seat_page_id or "",
                    fidEnc=fidEnc or "",
                )
                self.set_captcha_context(
                    roomid=roomid,
                    seat_num=seat,
                    day=request_day,
                    seat_page_id=seat_page_id,
                    fid_enc=fidEnc,
                )
                # seatengine/select 页面在前端是通过 GET 打开的，这里也使用 GET，
                # 否则可能拿到的是错误页或不包含 submit_enc 的内容。
                token, value = self._get_page_token(
                    page_url,
                    require_value=True,
                    method="GET",
                )
                logging.info(f"Get token from {page_url}: {token}")
                # 如果没有拿到 token，通常说明当前会话已失效或页面结构有变，
                # 不再继续本轮提交，交给外层重新登录/重试。
                if not token:
                    logging.warning(
                        "No submit_enc token fetched, break current submit loop and retry with new session"
                    )
                    break

                # 根据开关决定使用哪种验证码（两种验证码可以同时开启）
                captcha = ""
                if self.enable_slider:
                    captcha = self.resolve_captcha("slide")
                    logging.info(f"Slider captcha token: {captcha}")
                elif self.enable_textclick:
                    captcha = self.resolve_captcha("textclick")
                    logging.info(f"Textclick captcha token: {captcha}")
                suc = self.get_submit(
                    self.submit_url,
                    times=normalized_times,
                    token=token,
                    roomid=roomid,
                    seatid=seat,
                    captcha=captcha,
                    action=action,
                    value=value,
                    use_custom_day=use_custom_day,
                )
                if suc:
                    return suc
                time.sleep(self.sleep_time)
                self.max_attempt -= 1
        return suc

    def get_submit(
        self,
        url,
        times,
        token,
        roomid,
        seatid,
        captcha="",
        action=False,
        value="",
        use_custom_day=False,
    ):
        # 与前端保持一致：提交 roomId/startTime/endTime/day/seatNum/captcha/wyToken，再计算 enc
        # 按前端逻辑：wyToken 仅在开启网易风控时由 wyRiskObj.getToken() 生成；
        # 常规情况下为空字符串，这里保持一致，不再把 submit_enc 当作 wyToken 传给后端。
        normalized_times, _, parm = self._build_submit_payload(
            times,
            roomid,
            seatid,
            captcha,
            use_custom_day=use_custom_day,
        )
        # 使用页面上的 submit_enc（value）作为算法值生成 enc
        parm["enc"] = verify_param(parm, value)
        logging.info(f"submit enc: {parm['enc']}")

        # 按前端行为采用表单提交（POST body），并关闭证书验证以避免告警
        data = self._submit_with_fallback(parm, request_name="seat submit")
        if data is None:
            return False
        self.last_submit_result = data
        self.submit_msg.append(normalized_times[0] + "~" + normalized_times[1] + ":  " + str(data))
        logging.info(data)

        # 特殊处理：服务器返回 302 错误码（"您在页面停留过久，本次操作安全验证已超时。请刷新后再提交预约(代码:302)"）
        # 实际抢座过程中，这类返回往往已经完成了预约，只是前端要求用户刷新页面。
        msg = str(data.get("msg", ""))
        if not data.get("success") and "代码:302" in msg:
            logging.warning(
                "Server returned timeout code 302, treat this as success according to script preference."
            )
            return True

        return data.get("success", False)

    def burst_submit_once(
        self,
        times,
        roomid,
        seatid,
        captcha,
        token,
        value,
        use_custom_day=False,
    ):
        """单次提交，返回完整响应 dict，用于 1.8 秒高频窗口内的逻辑判断。

        注意：这里沿用新的 enc 生成方式，token 仅作为前端算法值 value 的来源，
        不再直接作为提交字段发送给后端。
        """
        normalized_times, _, parm = self._build_submit_payload(
            times,
            roomid,
            seatid,
            captcha,
            use_custom_day=use_custom_day,
        )
        parm["enc"] = verify_param(parm, value)
        data = self._submit_with_fallback(parm, request_name="[burst] seat submit")
        if data is None:
            return {"success": False, "msg": "submit request failed on all API families"}
        self.last_submit_result = data
        self.submit_msg.append(normalized_times[0] + "~" + normalized_times[1] + ":  " + str(data))
        logging.info(data)
        return data
