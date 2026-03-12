import json
import time
import argparse
import os
import logging
import datetime
import threading
from zoneinfo import ZoneInfo

# 统一日志时间为北京时间，方便在 GitHub Actions 日志中查看
# 精确到毫秒，格式示例：2026-01-22 19:16:59.123 [Asia/Shanghai] - INFO - ...
class BeijingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        """始终将日志时间格式化为北京时间。"""
        dt = datetime.datetime.fromtimestamp(record.created, ZoneInfo("Asia/Shanghai"))
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


_formatter = BeijingFormatter(
    fmt="%(asctime)s.%(msecs)03d [Asia/Shanghai] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_handler = logging.StreamHandler()
_handler.setFormatter(_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_handler])


def _beijing_now() -> datetime.datetime:
    """获取北京时间（带时区信息）。"""
    return datetime.datetime.now(ZoneInfo("Asia/Shanghai"))


from utils import reserve, get_user_credentials


def _now(action: bool) -> datetime.datetime:
    """获取当前逻辑时间。

    为了在 GitHub Actions 日志中时间统一可读：
    - 本地模式(action=False): 使用本地系统时间；1111
    - GitHub Actions(action=True): 使用北京时间(Asia/Shanghai)。
    """
    if action:
        return _beijing_now()
    return datetime.datetime.now()


# 日志时间：保留 3 位毫秒，和日志头部保持一致
get_log_time = lambda action: _now(action).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
# 逻辑比较时间：只用到当天的时分秒
get_hms = lambda action: _now(action).strftime("%H:%M:%S")
get_current_dayofweek = lambda action: _now(action).strftime("%A")


def _format_seat_number(seat_num: int) -> str:
    """将座位号格式化为三位数字符串，如 1 -> '001', 43 -> '043'"""
    return f"{seat_num:03d}"


SLEEPTIME = 0.1  # 每次抢座的间隔（减少到0.05秒以加快速度）
ENDTIME = "20:00:40"  # 根据学校的预约座位时间+1min即可

ENABLE_SLIDER = False  # 是否有滑块验证（调试阶段先关闭）
ENABLE_TEXTCLICK = False  # 是否有选字验证码（需要图灵云打码平台）
MAX_ATTEMPT = 1
RESERVE_NEXT_DAY = True  # 预约明天而不是今天的


# 是否在每一轮主循环中都重新登录。
# True：每一轮都会重新创建会话并登录（原有行为）；
# False：每个账号只在第一次需要时登录一次，后续循环复用同一个会话。
RELOGIN_EVERY_LOOP = True


def _get_beijing_target_from_endtime() -> datetime.datetime:
    """根据 ENDTIME 计算目标时间（北京时间，当天 ENDTIME 减 40 秒）。"""
    today = _beijing_now().date()
    h, m, s = map(int, ENDTIME.split(":"))
    end_dt = datetime.datetime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=h,
        minute=m,
        second=s,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    return end_dt - datetime.timedelta(seconds=40)
    # return end_dt - datetime.timedelta(minutes=1)  # ENDTIME 前 1 分钟（60秒）

def _burst_shot_worker(
    index, offset_ms, target_dt, s, token_url,
    times, roomid, seatid, captcha, action, results,
    pre_token="", pre_value=""
):
    """定时连发（极限型）的单次提交工作线程。

    在 target_dt + offset_ms 时刻提交预约，结果写入 results[index]。
    若传入 pre_token/pre_value（主线程预取），则直接使用，跳过 GET 请求；
    否则在发射时刻现场获取（有网络延迟）。
    """
    fire_dt = target_dt + datetime.timedelta(milliseconds=offset_ms)
    while _beijing_now() < fire_dt:
        time.sleep(0.001)

    logging.info(
        f"[burst] Shot {index + 1} firing at {_beijing_now()} (target_dt + {offset_ms}ms)"
    )

    if pre_token:
        token, value = pre_token, pre_value
        logging.info(f"[burst] Shot {index + 1} using pre-fetched token: {token}")
    else:
        token, value = s._get_page_token(token_url, require_value=True)
        if not token:
            logging.error(f"[burst] Shot {index + 1} failed to get page token")
            results[index] = False
            return
        logging.info(f"[burst] Shot {index + 1} fetched token on-the-fly: {token}")

    result = s.get_submit(
        url=s.submit_url,
        times=times,
        token=token,
        roomid=roomid,
        seatid=seatid,
        captcha=captcha,
        action=action,
        value=value,
    )
    results[index] = result
    logging.info(f"[burst] Shot {index + 1} result: {result}")


def strategic_first_attempt(
    users,
    usernames: str | None,
    passwords: str | None,
    action: bool,
    target_dt: datetime.datetime,
    success_list=None,
    sessions=None,
):
    """只在第一次调用时使用的“有策略抢座”。

    - 在目标时间前 2 分钟左右开始（由 Actions 的 cron 控制）；
    - 目标时间前 20 秒：预先获取页面 token / algorithm value；
    - 目标时间前 12 秒：预先完成滑块并拿到 validate；
    - 目标时间到达瞬间：直接调用 get_submit 提交一次；
    - 之后的重试逻辑仍交给原有 while 循环和 login_and_reserve。
    """
    if success_list is None:
        success_list = [False] * len(users)

    now = _beijing_now()
    # 如果已经过了目标时间，直接退回到普通逻辑由外层处理
    if now >= target_dt:
        return success_list

    # 等到“目标时间前若干秒”附近再开始策略流程，由 cron 提前少量时间启动
    thirty_before = target_dt - datetime.timedelta(seconds=STRATEGY_LOGIN_LEAD_SECONDS)
    while _beijing_now() < thirty_before:
        time.sleep(0.5)

    usernames_list, passwords_list = None, None
    if action:
        if not usernames or not passwords:
            raise Exception("USERNAMES or PASSWORDS not configured correctly in env")
        usernames_list = usernames.split(",")
        passwords_list = passwords.split(",")
        if len(usernames_list) != len(passwords_list):
            raise Exception("USERNAMES and PASSWORDS count mismatch")

    current_dayofweek = get_current_dayofweek(action)

    for index, user in enumerate(users):
        # 已经成功的配置不再参与策略尝试
        if success_list[index]:
            continue

        username = user["username"]
        password = user["password"]
        times = user["times"]
        roomid = user["roomid"]
        seatid = user["seatid"]
        seat_page_id = user.get("seatPageId")
        fid_enc = user.get("fidEnc")
        daysofweek = user["daysofweek"]

        # 今天不预约该配置，跳过
        if current_dayofweek not in daysofweek:
            logging.info("[strategic] Today not set to reserve, skip this config")
            continue

        # Actions 模式：根据索引或单账号覆盖用户名和密码
        if action:
            if len(usernames_list) == 1:
                username = usernames_list[0]
                password = passwords_list[0]
            elif index < len(usernames_list):
                username = usernames_list[index]
                password = passwords_list[index]
            else:
                logging.error(
                    "[strategic] Index out of range for USERNAMES/PASSWORDS, skipping this config."
                )
                continue

        # seatid 可能是字符串或列表，只在策略阶段针对第一个座位做一次精准尝试
        seat_list = [seatid] if isinstance(seatid, str) else seatid
        if not seat_list:
            logging.error("[strategic] Empty seat list, skip this config")
            continue

        logging.info(
            f"[strategic] Start first attempt for {username} -- {times} -- {seat_list} -- seatPageId={seat_page_id} -- fidEnc={fid_enc}"
        )

        # 1. 在 [T-30s, T] 区间内完成登录和基础 session（不提前获取页面 token）
        s = reserve(
            sleep_time=SLEEPTIME,
            max_attempt=MAX_ATTEMPT,
            enable_slider=ENABLE_SLIDER,
            enable_textclick=ENABLE_TEXTCLICK,
            reserve_next_day=RESERVE_NEXT_DAY,
        )
        s.get_login_status()
        s.login(username, password)
        s.requests.headers.update({"Host": "office.chaoxing.com"})
        # 将已登录的 session 存入 sessions[]，fallback 直接复用，无需重新登录
        if sessions is not None and sessions[index] is None:
            sessions[index] = s

        first_seat = seat_list[0]

        # 2. 等到“目标时间前若干秒”，预热滑块验证码，提前拿到多份 validate（如果启用了滑块）
        ten_before = target_dt - datetime.timedelta(seconds=STRATEGY_SLIDER_LEAD_SECONDS)
        while _beijing_now() < ten_before:
            time.sleep(0.1)

        captcha1 = captcha2 = captcha3 = ""
        # 根据开关决定是否预热验证码
        if ENABLE_SLIDER:
            # 滑块验证：预先获取三份 validate
            captcha1 = s.resolve_captcha("slide")
            if not captcha1:
                logging.warning(
                    "[strategic] First slider captcha failed or empty, retrying once more"
                )
                captcha1 = s.resolve_captcha("slide")
            logging.info(f"[strategic] Pre-resolved slider captcha1: {captcha1}")

            captcha2 = s.resolve_captcha("slide")
            if not captcha2:
                logging.warning(
                    "[strategic] Second slider captcha failed or empty, retrying once more"
                )
                captcha2 = s.resolve_captcha("slide")
            logging.info(f"[strategic] Pre-resolved slider captcha2: {captcha2}")

            captcha3 = s.resolve_captcha("slide")
            if not captcha3:
                logging.warning(
                    "[strategic] Third slider captcha failed or empty, retrying once more"
                )
                captcha3 = s.resolve_captcha("slide")
            logging.info(f"[strategic] Pre-resolved slider captcha3: {captcha3}")
        elif ENABLE_TEXTCLICK:
            # 选字验证：预先获取三份 validate（循环重试直到成功）
            def get_textclick_with_retry(name: str, max_retries: int = 10) -> str:
                for i in range(max_retries):
                    captcha = s.resolve_captcha("textclick")
                    if captcha:
                        logging.info(f"[strategic] {name} textclick captcha resolved: {captcha}")
                        return captcha
                    logging.warning(f"[strategic] {name} textclick captcha failed, retrying ({i + 1}/{max_retries})")
                    time.sleep(0.5)
                logging.error(f"[strategic] {name} textclick captcha failed after {max_retries} retries")
                return ""

            captcha1 = get_textclick_with_retry("First")
            captcha2 = get_textclick_with_retry("Second")

        # token URL 供所有 3 次提交复用
        # 使用当天日期获取页面 token，避免预约日尚未开放导致页面报错拿不到 submit_enc
        _token_day = _beijing_now().date()
        _token_url = s.url.format(
            roomId=roomid,
            day=str(_token_day),
            seatPageId=seat_page_id or "",
            fidEnc=fid_enc or "",
        )

        # 连接预热：在 T-4s 发一次真实的 token GET 请求（结果丢弃），建好 TCP+TLS 连接，后续复用省 ~100-200ms
        warm_dt = target_dt - datetime.timedelta(seconds=4)
        while _beijing_now() < warm_dt:
            time.sleep(0.05)
        s.warm_connection(_token_url)

        if SUBMIT_MODE == "burst":
            # ── 定时连发（极限型）──
            n_shots = len(BURST_OFFSETS_MS)
            captchas_list = [captcha1, captcha2, captcha3]

            if STRATEGIC_MODE == "C":
                # ── 策略 C + burst：等到 T + TOKEN_FETCH_DELAY_MS 取一次 token，复用给所有线程 ──
                fetch_dt = target_dt + datetime.timedelta(milliseconds=TOKEN_FETCH_DELAY_MS)
                while _beijing_now() < fetch_dt:
                    time.sleep(0.001)
                logging.info(
                    f"[strategic] [burst-C] Fetching single reusable token at {_beijing_now()} "
                    f"(target_dt + {TOKEN_FETCH_DELAY_MS}ms)"
                )
                pt, pv = s._get_page_token(_token_url, require_value=True)
                if pt:
                    logging.info(f"[strategic] [burst-C] Got token: {pt}")
                else:
                    logging.warning("[strategic] [burst-C] Token fetch failed, threads will fetch on-the-fly")
                pre_tokens = [(pt, pv)] * n_shots

            elif STRATEGIC_MODE == "A":
                # 策略 A + burst：主线程在 T - PRE_FETCH_TOKEN_MS 提前取 1 份 token，
                # 所有线程共用同一份，到点直接 POST，零 GET 延迟
                burst_prefetch_dt = target_dt - datetime.timedelta(milliseconds=PRE_FETCH_TOKEN_MS)
                if _beijing_now() < burst_prefetch_dt:
                    logging.info(
                        f"[strategic] [burst-A] Waiting until target_dt - {PRE_FETCH_TOKEN_MS}ms "
                        f"({burst_prefetch_dt}) to pre-fetch token"
                    )
                    while _beijing_now() < burst_prefetch_dt:
                        time.sleep(0.05)

                logging.info(
                    f"[strategic] [burst-A] Pre-fetching 1 shared token at {_beijing_now()}"
                )
                pt, pv = s._get_page_token(_token_url, require_value=True)
                if pt:
                    logging.info(f"[strategic] [burst-A] Pre-fetched shared token: {pt}")
                else:
                    logging.warning(
                        "[strategic] [burst-A] Token pre-fetch failed, "
                        "threads will fetch on-the-fly as fallback"
                    )
                pre_tokens = [(pt, pv)] * n_shots
            else:
                # 策略 B + burst：不预取，各线程在各自的发射时刻（T + offset）自己取 token 并立即提交
                logging.info(
                    f"[strategic] [burst-B] No pre-fetch; each thread will fetch token "
                    "on-the-fly at its own fire time"
                )
                pre_tokens = [("", "")] * n_shots

            burst_results = [None] * n_shots
            threads = []
            for burst_i, burst_offset_ms in enumerate(BURST_OFFSETS_MS):
                burst_cap = captchas_list[burst_i] if burst_i < len(captchas_list) else ""
                pt, pv = pre_tokens[burst_i] if burst_i < len(pre_tokens) else ("", "")
                t = threading.Thread(
                    target=_burst_shot_worker,
                    args=(
                        burst_i, burst_offset_ms, target_dt, s, _token_url,
                        times, roomid, first_seat, burst_cap, action, burst_results,
                        pt, pv,
                    ),
                    daemon=True,
                    name=f"burst-shot-{burst_i + 1}",
                )
                threads.append(t)

            logging.info(
                f"[strategic] [burst] Launching {len(threads)} shots at offsets "
                f"{BURST_OFFSETS_MS} ms from target_dt"
            )
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

            suc = any(r for r in burst_results if r)
            logging.info(
                f"[strategic] [burst] All shots done, results: {burst_results}, overall success: {suc}"
            )

        else:
            # ── 串行重试（稳健型）──
            # 每枪等到 HTTP 响应后，失败才发下一枪
            if STRATEGIC_MODE == "C":
                # 策略 C：等到 T + TOKEN_FETCH_DELAY_MS 取一次 token 并立即提交
                fetch_dt = target_dt + datetime.timedelta(milliseconds=TOKEN_FETCH_DELAY_MS)
                while _beijing_now() < fetch_dt:
                    time.sleep(0.001)
                logging.info(
                    f"[strategic] [C] Fetching token at {_beijing_now()} "
                    f"(target_dt + {TOKEN_FETCH_DELAY_MS}ms)"
                )
                token1, value1 = s._get_page_token(_token_url, require_value=True)
                if not token1:
                    logging.error("[strategic] [C] Token fetch failed, skip this config")
                    continue
                logging.info(f"[strategic] [C] Got token: {token1}, immediately submit")
                suc = s.get_submit(
                    url=s.submit_url,
                    times=times,
                    token=token1,
                    roomid=roomid,
                    seatid=first_seat,
                    captcha=captcha1,
                    action=action,
                    value=value1,
                )

            elif STRATEGIC_MODE == "A":
                # 策略 A：目标时间前 PRE_FETCH_TOKEN_MS 毫秒预取 token，
                #         目标时间后 FIRST_SUBMIT_OFFSET_MS 毫秒提交
                pre_fetch_dt = target_dt - datetime.timedelta(milliseconds=PRE_FETCH_TOKEN_MS)
                while _beijing_now() < pre_fetch_dt:
                    time.sleep(0.1)
                logging.info(
                    f"[strategic] [A] Pre-fetch page token at {_beijing_now()} (target_dt - {PRE_FETCH_TOKEN_MS}ms)"
                )
                token1, value1 = s._get_page_token(_token_url, require_value=True)
                if not token1:
                    logging.error("[strategic] Failed to get page token for first submit, skip this config")
                    continue
                logging.info(f"[strategic] Got page token for first submit: {token1}, value: {value1}")

                submit_dt1 = target_dt + datetime.timedelta(milliseconds=FIRST_SUBMIT_OFFSET_MS)
                while _beijing_now() < submit_dt1:
                    time.sleep(0.001)
                logging.info(
                    f"[strategic] [A] First submit at {_beijing_now()} (target_dt + {FIRST_SUBMIT_OFFSET_MS}ms)"
                )
                suc = s.get_submit(
                    url=s.submit_url,
                    times=times,
                    token=token1,
                    roomid=roomid,
                    seatid=first_seat,
                    captcha=captcha1,
                    action=action,
                    value=value1,
                )

            else:
                # 策略 B：目标时间后 FIRST_SUBMIT_OFFSET_MS 毫秒获取 token 并立即提交
                token_fetch_dt1 = target_dt + datetime.timedelta(milliseconds=FIRST_SUBMIT_OFFSET_MS)
                while _beijing_now() < token_fetch_dt1:
                    time.sleep(0.001)
                logging.info(
                    f"[strategic] [B] Fetch page token at {_beijing_now()} (target_dt + {FIRST_SUBMIT_OFFSET_MS}ms)"
                )
                token1, value1 = s._get_page_token(_token_url, require_value=True)
                if not token1:
                    logging.error("[strategic] Failed to get page token for first submit, skip this config")
                    continue
                logging.info(f"[strategic] Got page token for first submit: {token1}, value: {value1}")
                logging.info(f"[strategic] [B] Immediately submit after fetching page token")
                suc = s.get_submit(
                    url=s.submit_url,
                    times=times,
                    token=token1,
                    roomid=roomid,
                    seatid=first_seat,
                    captcha=captcha1,
                    action=action,
                    value=value1,
                )

            # 如果第一次没有成功：为第二次提交重新获取页面 token，再延迟 TARGET_OFFSET2_MS 毫秒提交
            if not suc:
                logging.info("[strategic] First submit failed, prepare second submit with NEW page token")

                token2, value2 = s._get_page_token(_token_url, require_value=True)
                if not token2:
                    logging.error("[strategic] Failed to get page token for second submit, skip to third/normal flow")
                else:
                    send_dt2 = _beijing_now() + datetime.timedelta(milliseconds=TARGET_OFFSET2_MS)
                    while _beijing_now() < send_dt2:
                        time.sleep(0.02)
                    logging.info(
                        f"[strategic] Second submit at {send_dt2} (now + {TARGET_OFFSET2_MS}ms) with NEW page token"
                    )
                    suc = s.get_submit(
                        url=s.submit_url,
                        times=times,
                        token=token2,
                        roomid=roomid,
                        seatid=first_seat,
                        captcha=captcha2,
                        action=action,
                        value=value2,
                    )

            # 如果第二次仍未成功：为第三次提交再次获取新的 token，再延迟 TARGET_OFFSET3_MS 毫秒提交
            if not suc:
                logging.info("[strategic] Second submit failed, prepare third submit with NEW page token")

                token3, value3 = s._get_page_token(_token_url, require_value=True)
                if not token3:
                    logging.error("[strategic] Failed to get page token for third submit, give up strategic submits for this config")
                else:
                    send_dt3 = _beijing_now() + datetime.timedelta(milliseconds=TARGET_OFFSET3_MS)
                    while _beijing_now() < send_dt3:
                        time.sleep(0.02)
                    logging.info(
                        f"[strategic] Third submit at {send_dt3} (now + {TARGET_OFFSET3_MS}ms) with NEW page token"
                    )
                    suc = s.get_submit(
                        url=s.submit_url,
                        times=times,
                        token=token3,
                        roomid=roomid,
                        seatid=first_seat,
                        captcha=captcha3,
                        action=action,
                        value=value3,
                    )

        success_list[index] = suc

    return success_list


def login_and_reserve(
    users, usernames, passwords, action, success_list=None, sessions=None
):
    logging.info(
        f"Global settings: \nSLEEPTIME: {SLEEPTIME}\nENDTIME: {ENDTIME}\nENABLE_SLIDER: {ENABLE_SLIDER}\nENABLE_TEXTCLICK: {ENABLE_TEXTCLICK}\nRESERVE_NEXT_DAY: {RESERVE_NEXT_DAY}"
    )

    usernames_list, passwords_list = None, None
    if action:
        if not usernames or not passwords:
            raise Exception("USERNAMES or PASSWORDS not configured correctly in env")
        usernames_list = usernames.split(",")
        passwords_list = passwords.split(",")
        if len(usernames_list) != len(passwords_list):
            raise Exception("USERNAMES and PASSWORDS count mismatch")

    if success_list is None:
        success_list = [False] * len(users)

    # 如果传入了 sessions，但长度和 users 不匹配，则忽略 sessions，退回每轮重登
    if sessions is not None and len(sessions) != len(users):
        logging.error("sessions length mismatch with users, ignore sessions and relogin each loop.")
        sessions = None

    current_dayofweek = get_current_dayofweek(action)
    for index, user in enumerate(users):
        username = user["username"]
        password = user["password"]
        times = user["times"]
        roomid = user["roomid"]
        seatid = user["seatid"]
        seat_page_id = user.get("seatPageId")
        fid_enc = user.get("fidEnc")
        daysofweek = user["daysofweek"]

        # 如果今天不在该配置的 daysofweek 中，直接跳过
        if current_dayofweek not in daysofweek:
            logging.info("Today not set to reserve")
            continue

        if action:
            if len(usernames_list) == 1:
                # 只有一个账号，所有配置都用这个账号
                username = usernames_list[0]
                password = passwords_list[0]
            elif index < len(usernames_list):
                username = usernames_list[index]
                password = passwords_list[index]
            else:
                logging.error(
                    "Index out of range for USERNAMES/PASSWORDS, skipping this config."
                )
                continue

        if not success_list[index]:
            logging.info(
                f"----------- {username} -- {times} -- {seatid} try -----------"
            )

            # 根据 RELOGIN_EVERY_LOOP 决定是否复用会话
            s = None
            if sessions is not None:
                s = sessions[index]
                if s is None:
                    # 该账号第一次使用：创建会话并登录
                    s = reserve(
                        sleep_time=SLEEPTIME,
                        max_attempt=MAX_ATTEMPT,
                        enable_slider=ENABLE_SLIDER,
                        enable_textclick=ENABLE_TEXTCLICK,
                        reserve_next_day=RESERVE_NEXT_DAY,
                    )
                    s.get_login_status()
                    s.login(username, password)
                    s.requests.headers.update({"Host": "office.chaoxing.com"})
                    sessions[index] = s
                else:
                    # 复用已有会话，确保 Host 头正确
                    s.requests.headers.update({"Host": "office.chaoxing.com"})
            else:
                # 维持原有行为：每一轮循环都重新创建会话并登录
                s = reserve(
                    sleep_time=SLEEPTIME,
                    max_attempt=MAX_ATTEMPT,
                    enable_slider=ENABLE_SLIDER,
                    enable_textclick=ENABLE_TEXTCLICK,
                    reserve_next_day=RESERVE_NEXT_DAY,
                )
                s.get_login_status()
                s.login(username, password)
                s.requests.headers.update({"Host": "office.chaoxing.com"})

            # 在 GitHub Actions 中传入 ENDTIME，确保内部循环在超过结束时间后及时停止
            suc = s.submit(
                times,
                roomid,
                seatid,
                action,
                ENDTIME if action else None,
                fidEnc=fid_enc,
                seat_page_id=seat_page_id,
            )
            success_list[index] = suc
    return success_list


def main(users, action=False):
    global MAX_ATTEMPT
    target_dt = _get_beijing_target_from_endtime()
    logging.info(
        f"start time {get_log_time(action)}, action {'on' if action else 'off'}, target_dt {target_dt}"
    )
    attempt_times = 0
    usernames, passwords = None, None
    if action:
        usernames, passwords = get_user_credentials(action)
    success_list = None

    # 根据 RELOGIN_EVERY_LOOP 决定是否为每个用户维护持久会话
    sessions = None
    if not RELOGIN_EVERY_LOOP:
        sessions = [None] * len(users)

    current_dayofweek = get_current_dayofweek(action)
    today_reservation_num = sum(
        1 for d in users if current_dayofweek in d.get("daysofweek")
    )

    # 只在 GitHub Actions 模式下执行一次“有策略”的第一次尝试
    strategic_done = False

    # 保存每个配置的初始座位号（优先取 seatid 第一个），用于预热失败后按 +1 递增
    original_seatids = []
    for user in users:
        sid = user.get("seatid")
        raw_sid = (
            sid
            if isinstance(sid, str)
            else (sid[0] if isinstance(sid, list) and sid else None)
        )
        try:
            original_seatids.append(int(raw_sid) if raw_sid is not None else None)
        except (TypeError, ValueError):
            logging.warning(
                f"[seat-increment] Invalid seatid {raw_sid}, skip auto-increment for this config"
            )
            original_seatids.append(None)
    seat_offset = 0

    while True:
        # 使用逻辑时间 _now(action)，在 GitHub Actions 下就是北京时间
        current_time = get_hms(action)
        if current_time >= ENDTIME:
            logging.info(
                f"Current time {current_time} >= ENDTIME {ENDTIME}, stop main loop"
            )
            return

        attempt_times += 1

        if not strategic_done and action:
            success_list = strategic_first_attempt(
                users, usernames, passwords, action, target_dt, success_list, sessions
            )
            strategic_done = True

            # 预热三次结束后，如果仍有配置未成功，自动递增座位号并立即继续尝试
            if success_list is not None and sum(success_list) < today_reservation_num:
                seat_offset = 1
                for i, user in enumerate(users):
                    if not success_list[i] and original_seatids[i] is not None \
                            and current_dayofweek in user.get("daysofweek", []):
                        new_seat = _format_seat_number(original_seatids[i] + seat_offset)
                        user["seatid"] = [new_seat]
                        logging.info(
                            f"[seat-increment-after-strategic] Config {i}: try seat {new_seat} "
                            f"(base {original_seatids[i]} + offset {seat_offset})"
                        )
                # 递增座位后立即调用 login_and_reserve（每个座位只试一次）
                MAX_ATTEMPT = 1
                if sessions is not None:
                    for s_obj in sessions:
                        if s_obj is not None:
                            s_obj.max_attempt = 1
                success_list = login_and_reserve(
                    users, usernames, passwords, action, success_list, sessions
                )
        else:
            # 预热结束后仍未成功：未成功配置按座位号 +1 继续尝试（每轮 +1）
            if success_list is not None and sum(success_list) < today_reservation_num:
                seat_offset += 1
                for i, user in enumerate(users):
                    if not success_list[i] and original_seatids[i] is not None \
                            and current_dayofweek in user.get("daysofweek", []):
                        new_seat = _format_seat_number(original_seatids[i] + seat_offset)
                        user["seatid"] = [new_seat]
                        logging.info(
                            f"[seat-increment] Config {i}: try seat {new_seat} "
                            f"(base {original_seatids[i]} + offset {seat_offset})"
                        )

                # 递增模式下每个座位只提交一次，失败就下一轮换座位
                MAX_ATTEMPT = 1
                if sessions is not None:
                    for s_obj in sessions:
                        if s_obj is not None:
                            s_obj.max_attempt = 1
            success_list = login_and_reserve(
                users, usernames, passwords, action, success_list, sessions
            )

        print(
            f"attempt time {attempt_times}, time now {current_time}, success list {success_list}"
        )
        if sum(success_list) == today_reservation_num:
            print(f"reserved successfully!")
            return


def debug(users, action=False):
    logging.info(
        f"Global settings: \nSLEEPTIME: {SLEEPTIME}\nENDTIME: {ENDTIME}\nENABLE_SLIDER: {ENABLE_SLIDER}\nENABLE_TEXTCLICK: {ENABLE_TEXTCLICK}\nRESERVE_NEXT_DAY: {RESERVE_NEXT_DAY}"
    )
    suc = False
    logging.info(f" Debug Mode start! , action {'on' if action else 'off'}")

    usernames_list, passwords_list = None, None
    if action:
        usernames, passwords = get_user_credentials(action)
        if not usernames or not passwords:
            logging.error("USERNAMES or PASSWORDS not configured correctly in env.")
            return
        usernames_list = usernames.split(",")
        passwords_list = passwords.split(",")
        if len(usernames_list) != len(passwords_list):
            logging.error("USERNAMES and PASSWORDS count mismatch.")
            return

    current_dayofweek = get_current_dayofweek(action)
    for index, user in enumerate(users):
        username = user["username"]
        password = user["password"]
        times = user["times"]
        roomid = user["roomid"]
        seatid = user["seatid"]
        seat_page_id = user.get("seatPageId")
        fid_enc = user.get("fidEnc")
        daysofweek = user["daysofweek"]
        if type(seatid) == str:
            seatid = [seatid]

        # 如果今天不在该配置的 daysofweek 中，直接跳过，不处理账号
        if current_dayofweek not in daysofweek:
            logging.info("Today not set to reserve")
            continue

        # 在 GitHub Actions 中，从环境变量获取账号密码
        if action:
            if len(usernames_list) == 1:
                # 只有一个账号时，所有配置都用这个账号
                username = usernames_list[0]
                password = passwords_list[0]
            elif index < len(usernames_list):
                username = usernames_list[index]
                password = passwords_list[index]
            else:
                logging.error(
                    "Index out of range for USERNAMES/PASSWORDS, skipping this config."
                )
                continue

        logging.info(f"----------- {username} -- {times} -- {seatid} try -----------")
        s = reserve(
            sleep_time=SLEEPTIME,
            max_attempt=MAX_ATTEMPT,
            enable_slider=ENABLE_SLIDER,
            enable_textclick=ENABLE_TEXTCLICK,
            reserve_next_day=RESERVE_NEXT_DAY,
        )
        s.get_login_status()
        s.login(username, password)
        s.requests.headers.update({"Host": "office.chaoxing.com"})
        suc = s.submit(times, roomid, seatid, action, None, fidEnc=fid_enc, seat_page_id=seat_page_id)
        if suc:
            return


def get_roomid(args1, args2):
    username = input("请输入用户名：")
    password = input("请输入密码：")
    s = reserve(
        sleep_time=SLEEPTIME,
        max_attempt=MAX_ATTEMPT,
        enable_slider=ENABLE_SLIDER,
        enable_textclick=ENABLE_TEXTCLICK,
        reserve_next_day=RESERVE_NEXT_DAY,
    )
    s.get_login_status()
    s.login(username=username, password=password)
    s.requests.headers.update({"Host": "office.chaoxing.com"})
    encode = input("请输入deptldEnc：")
    s.roomid(encode)


if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    parser = argparse.ArgumentParser(prog="Chao Xing seat auto reserve")
    parser.add_argument("-u", "--user", default=config_path, help="user config file")
    parser.add_argument(
        "-m",
        "--method",
        default="reserve",
        choices=["reserve", "debug", "room"],
        help="for debug",
    )
    parser.add_argument(
        "-a",
        "--action",
        action="store_true",
        help="use --action to enable in github action",
    )
    args = parser.parse_args()
    func_dict = {"reserve": main, "debug": debug, "room": get_roomid}
    with open(args.user, "r+") as data:
        config = json.load(data)
        usersdata = config["reserve"]

        # 从 config.json 读取所有策略参数（唯一配置来源）
        # ┌─────────────────────────────────────────────────────────────────────┐
        # │  mode (STRATEGIC_MODE) × submit_mode (SUBMIT_MODE) 四种组合         │
        # ├──────────┬────────────┬──────────────────────────────────────────────┤
        # │ mode=A   │ serial     │ T-pre_fetch_token_ms 预取token1              │
        # │          │            │ → T+first_submit_offset_ms POST，等结果       │
        # │          │            │ → 失败则现取token2，+offset2 POST，等结果      │
        # │          │            │ → 失败则现取token3，+offset3 POST             │
        # ├──────────┼────────────┼──────────────────────────────────────────────┤
        # │ mode=A   │ burst ★   │ T-pre_fetch_token_ms 预取token1/2/3           │
        # │          │            │ → T+burst[0] thread-1 直接POST（零GET延迟）   │
        # │          │            │ → T+burst[1] thread-2 直接POST（零GET延迟）   │
        # │          │            │ → T+burst[2] thread-3 直接POST（零GET延迟）   │
        # ├──────────┼────────────┼──────────────────────────────────────────────┤
        # │ mode=B   │ serial     │ T+first_submit_offset_ms 取token1并POST，等结果│
        # │ (默认)   │ (默认)     │ → 失败则现取token2，+offset2 POST，等结果      │
        # │          │            │ → 失败则现取token3，+offset3 POST             │
        # ├──────────┼────────────┼──────────────────────────────────────────────┤
        # │ mode=B   │ burst      │ T+burst[0] thread-1 自取token并POST           │
        # │          │            │ T+burst[1] thread-2 自取token并POST           │
        # │          │            │ T+burst[2] thread-3 自取token并POST           │
        # │          │            │ 注意：实际POST = burst[i] + GET网络延迟        │
        # └──────────┴────────────┴──────────────────────────────────────────────┘
        strategy_cfg = config.get("strategy", {})
        STRATEGY_LOGIN_LEAD_SECONDS = int(strategy_cfg.get("login_lead_seconds", 18))
        STRATEGY_SLIDER_LEAD_SECONDS = int(strategy_cfg.get("slider_lead_seconds", 14))
        STRATEGIC_MODE               = strategy_cfg.get("mode", "B")             # "A" | "B" | "C"
        PRE_FETCH_TOKEN_MS           = int(strategy_cfg.get("pre_fetch_token_ms", 3000))   # 仅 mode=A
        FIRST_SUBMIT_OFFSET_MS       = int(strategy_cfg.get("first_submit_offset_ms", 89)) # mode=A: 提交延迟; mode=B: 取token延迟
        TARGET_OFFSET2_MS            = int(strategy_cfg.get("target_offset2_ms", 150))     # 仅 serial
        TARGET_OFFSET3_MS            = int(strategy_cfg.get("target_offset3_ms", 160))     # 仅 serial
        SUBMIT_MODE                  = strategy_cfg.get("submit_mode", "serial")  # "serial" | "burst"
        BURST_OFFSETS_MS             = strategy_cfg.get("burst_offsets_ms", [120, 420, 820])  # 仅 burst
        # mode=C 预热取 token 参数
        TOKEN_FETCH_DELAY_MS         = int(strategy_cfg.get("token_fetch_delay_ms", 50))   # 目标时间后多少 ms 取 token

        # 控制是否在每一轮主循环中都重新登录
        RELOGIN_EVERY_LOOP = bool(config.get("relogin_every_loop", RELOGIN_EVERY_LOOP))

    func_dict[args.method](usersdata, args.action)
