import argparse
import concurrent.futures
import datetime
import json
import os
import pathlib
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request


DEFAULT_SERVER_PROJECT_ROOT = "/opt/Main_ChaoXingReserveSeat"
ROOT_DIR = pathlib.Path(
    os.getenv("SERVER_PROJECT_ROOT", DEFAULT_SERVER_PROJECT_ROOT)
).resolve()
RUNS_DIR = ROOT_DIR / "server_runs"
RESOURCE_MONITOR_FILE_NAME = "00_resource_monitor.json"
RESOURCE_MONITOR_INTERVAL_SECONDS = float(os.getenv("SERVER_RESOURCE_MONITOR_INTERVAL", "1.0") or "1.0")
RESOURCE_MONITOR_MAX_SAMPLES = int(os.getenv("SERVER_RESOURCE_MONITOR_MAX_SAMPLES", "900") or "900")
RESOURCE_THRESHOLDS = {
    "main_cpu_percent_warning": 60,
    "main_cpu_percent_danger": 85,
    "load_ratio_warning": 0.75,
    "load_ratio_danger": 1.0,
    "mem_available_percent_warning": 20,
    "mem_available_percent_danger": 10,
}
KEY_LOG_PATTERN = re.compile(
    r"Start first attempt|login successfully|\[strategic\]|\[burst\]|\[warm\]|"
    r"submit parameter|submit enc|Get token|Got token|token fetch failed|No submit_enc|"
    r"captcha|Slider captcha token|Textclick captcha token|保存失败|seat-increment|"
    r"dispatch|HTTP [0-9]{3}|exception|error|Current time|reserved successfully|success list",
    re.IGNORECASE,
)


class ResourceMonitor:
    def __init__(self, run_dir: pathlib.Path, run_id: str, payload: dict, user_count: int, max_concurrency: int):
        self.run_dir = run_dir
        self.path = run_dir / RESOURCE_MONITOR_FILE_NAME
        self.run_id = run_id
        self.payload = payload
        self.user_count = user_count
        self.max_concurrency = max_concurrency
        self.interval = max(0.5, RESOURCE_MONITOR_INTERVAL_SECONDS)
        self.max_samples = max(60, RESOURCE_MONITOR_MAX_SAMPLES)
        self.cpu_count = max(1, os.cpu_count() or 1)
        self.page_size = int(os.sysconf("SC_PAGE_SIZE"))
        self.started_at = _beijing_now().isoformat()
        self.finished_at = ""
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="resource-monitor", daemon=True)
        self.active: dict[int, dict] = {}
        self.samples: list[dict] = []
        self.prev_total_ticks: int | None = None
        self.prev_proc_ticks: dict[int, int] = {}
        self.summary = {
            "sampleCount": 0,
            "peakActiveMain": 0,
            "peakMainCpuPercent": 0.0,
            "peakMainRssMb": 0.0,
            "peakLoad1": 0.0,
            "peakLoadRatio": 0.0,
            "memTotalMb": 0.0,
            "minMemAvailableMb": None,
            "minMemAvailablePercent": None,
            "maxRiskLevel": "ok",
            "maxRiskLabel": "正常",
            "riskReasons": [],
            "capacity": {
                "status": "pending",
                "conclusion": "批次运行中，只记录资源采样；并发容量会在这一批全部结束后估算。",
            },
        }

    def start(self):
        self._sample()
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=self.interval + 1.0)
        self.finished_at = _beijing_now().isoformat()
        self._sample(status="finished")

    def register_process(self, pid: int, user: dict):
        with self.lock:
            self.active[pid] = dict(user)

    def unregister_process(self, pid: int):
        with self.lock:
            self.active.pop(pid, None)

    def _run(self):
        while not self.stop_event.wait(self.interval):
            self._sample()

    def _snapshot_active(self) -> dict[int, dict]:
        with self.lock:
            return {pid: dict(info) for pid, info in self.active.items()}

    def _sample(self, status: str = "running"):
        active = self._snapshot_active()
        total_ticks = self._read_total_cpu_ticks()
        mem = self._read_mem_info()
        load = self._read_loadavg()
        proc_ticks: dict[int, int] = {}
        proc_details: list[dict] = []
        rss_bytes = 0

        for pid, info in active.items():
            proc = self._read_proc_usage(pid)
            if not proc:
                continue
            proc_ticks[pid] = proc["ticks"]
            rss_bytes += proc["rssBytes"]
            proc_details.append(
                {
                    "pid": pid,
                    "index": info.get("index"),
                    "displayName": info.get("displayName", ""),
                    "startedAt": info.get("startedAt", ""),
                    "rssMb": round(proc["rssBytes"] / 1024 / 1024, 2),
                    "cpuTicks": proc["ticks"],
                }
            )

        main_cpu_percent = self._calculate_cpu_percent(total_ticks, proc_ticks)
        load_ratio = load["load1"] / self.cpu_count if self.cpu_count else 0.0
        mem_available_percent = (
            mem["availableBytes"] / mem["totalBytes"] * 100
            if mem["totalBytes"] > 0
            else 0.0
        )
        risk_level, risk_reasons = self._assess_risk(
            main_cpu_percent=main_cpu_percent,
            load_ratio=load_ratio,
            mem_available_percent=mem_available_percent,
        )

        sample = {
            "ts": _beijing_now().isoformat(),
            "status": status,
            "activeMainCount": len(proc_details),
            "activeMainPids": [item["pid"] for item in proc_details],
            "mainCpuPercent": round(main_cpu_percent, 2),
            "mainRssMb": round(rss_bytes / 1024 / 1024, 2),
            "load1": round(load["load1"], 2),
            "load5": round(load["load5"], 2),
            "load15": round(load["load15"], 2),
            "loadRatio": round(load_ratio, 2),
            "cpuCount": self.cpu_count,
            "memTotalMb": round(mem["totalBytes"] / 1024 / 1024, 2),
            "memAvailableMb": round(mem["availableBytes"] / 1024 / 1024, 2),
            "memAvailablePercent": round(mem_available_percent, 2),
            "riskLevel": risk_level,
            "riskLabel": self._risk_label(risk_level),
            "riskReasons": risk_reasons,
            "processes": proc_details,
        }

        self.samples.append(sample)
        if len(self.samples) > self.max_samples:
            self.samples = self.samples[-self.max_samples :]
        self._update_summary(sample)
        self._write_snapshot(status=status)

    def _calculate_cpu_percent(self, total_ticks: int, proc_ticks: dict[int, int]) -> float:
        if self.prev_total_ticks is None or total_ticks <= self.prev_total_ticks:
            self.prev_total_ticks = total_ticks
            self.prev_proc_ticks = proc_ticks
            return 0.0

        total_delta = total_ticks - self.prev_total_ticks
        proc_delta = 0
        for pid, ticks in proc_ticks.items():
            previous = self.prev_proc_ticks.get(pid, ticks)
            proc_delta += max(0, ticks - previous)

        self.prev_total_ticks = total_ticks
        self.prev_proc_ticks = proc_ticks
        if total_delta <= 0:
            return 0.0
        return min(100.0, proc_delta / total_delta * 100)

    def _update_summary(self, sample: dict):
        self.summary["sampleCount"] = len(self.samples)
        self.summary["peakActiveMain"] = max(self.summary["peakActiveMain"], sample["activeMainCount"])
        self.summary["peakMainCpuPercent"] = round(
            max(float(self.summary["peakMainCpuPercent"]), sample["mainCpuPercent"]),
            2,
        )
        self.summary["peakMainRssMb"] = round(
            max(float(self.summary["peakMainRssMb"]), sample["mainRssMb"]),
            2,
        )
        self.summary["peakLoad1"] = round(max(float(self.summary["peakLoad1"]), sample["load1"]), 2)
        self.summary["peakLoadRatio"] = round(max(float(self.summary["peakLoadRatio"]), sample["loadRatio"]), 2)
        self.summary["memTotalMb"] = max(float(self.summary.get("memTotalMb") or 0), sample["memTotalMb"])
        current_min_mb = self.summary["minMemAvailableMb"]
        if current_min_mb is None:
            self.summary["minMemAvailableMb"] = sample["memAvailableMb"]
        else:
            self.summary["minMemAvailableMb"] = round(min(float(current_min_mb), sample["memAvailableMb"]), 2)
        current_min = self.summary["minMemAvailablePercent"]
        if current_min is None:
            self.summary["minMemAvailablePercent"] = sample["memAvailablePercent"]
        else:
            self.summary["minMemAvailablePercent"] = round(min(float(current_min), sample["memAvailablePercent"]), 2)

        if self._risk_rank(sample["riskLevel"]) > self._risk_rank(self.summary["maxRiskLevel"]):
            self.summary["maxRiskLevel"] = sample["riskLevel"]
            self.summary["maxRiskLabel"] = sample["riskLabel"]
        for reason in sample["riskReasons"]:
            if reason == "资源占用在安全范围内" and self.summary["riskReasons"]:
                continue
            if reason != "资源占用在安全范围内" and "资源占用在安全范围内" in self.summary["riskReasons"]:
                self.summary["riskReasons"].remove("资源占用在安全范围内")
            if reason not in self.summary["riskReasons"]:
                self.summary["riskReasons"].append(reason)
        if sample.get("status") == "finished":
            self.summary["capacity"] = self._build_capacity_estimate()
        else:
            self.summary["capacity"] = {
                "status": "pending",
                "conclusion": "批次运行中，只记录资源采样；并发容量会在这一批全部结束后估算。",
                "observedPeakConcurrency": self.summary["peakActiveMain"],
                "safeAdditionalMainPy": None,
                "hardAdditionalMainPy": None,
                "recommendedMaxConcurrency": None,
                "hardMaxConcurrency": None,
                "limitingResource": "pending",
                "limitingResourceLabel": "等待批次结束",
                "details": [],
            }

    def _write_snapshot(self, status: str):
        data = {
            "ok": True,
            "schemaVersion": 1,
            "runId": self.run_id,
            "schoolId": self.payload.get("school_id", ""),
            "schoolName": self.payload.get("school_name", ""),
            "batchIndex": self.payload.get("batch_index"),
            "batchTotal": self.payload.get("batch_total"),
            "userCount": self.user_count,
            "maxConcurrency": self.max_concurrency,
            "intervalSeconds": self.interval,
            "status": status,
            "startedAt": self.started_at,
            "finishedAt": self.finished_at,
            "updatedAt": _beijing_now().isoformat(),
            "thresholds": RESOURCE_THRESHOLDS,
            "summary": self.summary,
            "samples": self.samples,
        }
        tmp_path = self.path.with_suffix(".json.tmp")
        try:
            tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(self.path)
        except Exception as exc:
            print(f"[resource-monitor] write failed: {exc}", file=sys.stderr)

    def _build_capacity_estimate(self) -> dict:
        active_samples = [sample for sample in self.samples if int(sample.get("activeMainCount") or 0) > 0]
        if not active_samples:
            return {
                "status": "unknown",
                "conclusion": "本批次还没有捕捉到运行中的 main.py，暂时无法估算还能增加多少并发。",
                "observedPeakConcurrency": 0,
                "safeAdditionalMainPy": None,
                "hardAdditionalMainPy": None,
                "recommendedMaxConcurrency": None,
                "hardMaxConcurrency": None,
                "limitingResource": "unknown",
                "limitingResourceLabel": "等待采样",
                "details": [],
            }

        peak_active = max(int(sample.get("activeMainCount") or 0) for sample in active_samples)
        peak_cpu = max(float(sample.get("mainCpuPercent") or 0) for sample in active_samples)
        peak_rss = max(float(sample.get("mainRssMb") or 0) for sample in active_samples)
        peak_load1 = max(float(sample.get("load1") or 0) for sample in active_samples)
        cpu_count = max(1, int(active_samples[-1].get("cpuCount") or self.cpu_count or 1))
        mem_total_mb = max(float(sample.get("memTotalMb") or 0) for sample in active_samples)
        min_mem_available_mb = min(float(sample.get("memAvailableMb") or 0) for sample in active_samples)

        avg_cpu_per_main = self._max_per_main(active_samples, "mainCpuPercent")
        avg_rss_per_main = self._max_per_main(active_samples, "mainRssMb")
        avg_load_per_main = peak_load1 / peak_active if peak_active > 0 and peak_load1 > 0 else 0.0

        details: list[dict] = []
        cpu_safe = self._headroom_count(
            RESOURCE_THRESHOLDS["main_cpu_percent_warning"],
            peak_cpu,
            avg_cpu_per_main,
        )
        cpu_hard = self._headroom_count(
            RESOURCE_THRESHOLDS["main_cpu_percent_danger"],
            peak_cpu,
            avg_cpu_per_main,
        )
        details.append(
            {
                "resource": "cpu",
                "label": "main.py CPU",
                "safeAdditional": cpu_safe,
                "hardAdditional": cpu_hard,
                "observed": round(peak_cpu, 2),
                "unit": "%",
                "perMain": round(avg_cpu_per_main, 2) if avg_cpu_per_main > 0 else None,
            }
        )

        load_safe = self._headroom_count(
            RESOURCE_THRESHOLDS["load_ratio_warning"] * cpu_count,
            peak_load1,
            avg_load_per_main,
        )
        load_hard = self._headroom_count(
            RESOURCE_THRESHOLDS["load_ratio_danger"] * cpu_count,
            peak_load1,
            avg_load_per_main,
        )
        details.append(
            {
                "resource": "load",
                "label": "系统 load",
                "safeAdditional": load_safe,
                "hardAdditional": load_hard,
                "observed": round(peak_load1, 2),
                "unit": "",
                "perMain": round(avg_load_per_main, 2) if avg_load_per_main > 0 else None,
            }
        )

        mem_safe_target = mem_total_mb * (RESOURCE_THRESHOLDS["mem_available_percent_warning"] / 100)
        mem_hard_target = mem_total_mb * (RESOURCE_THRESHOLDS["mem_available_percent_danger"] / 100)
        mem_safe = self._headroom_count(min_mem_available_mb, mem_safe_target, avg_rss_per_main)
        mem_hard = self._headroom_count(min_mem_available_mb, mem_hard_target, avg_rss_per_main)
        details.append(
            {
                "resource": "memory",
                "label": "可用内存",
                "safeAdditional": mem_safe,
                "hardAdditional": mem_hard,
                "observed": round(min_mem_available_mb, 2),
                "unit": "MB",
                "perMain": round(avg_rss_per_main, 2) if avg_rss_per_main > 0 else None,
            }
        )

        known_safe = [item["safeAdditional"] for item in details if item["safeAdditional"] is not None]
        known_hard = [item["hardAdditional"] for item in details if item["hardAdditional"] is not None]
        safe_additional = max(0, min(known_safe)) if known_safe else None
        hard_additional = max(0, min(known_hard)) if known_hard else None
        limiting = self._limiting_resource(details, "safeAdditional")

        recommended_max = peak_active + safe_additional if safe_additional is not None else None
        hard_max = peak_active + hard_additional if hard_additional is not None else None
        status = self._capacity_status(safe_additional=safe_additional, hard_additional=hard_additional)
        conclusion = self._capacity_conclusion(
            status=status,
            peak_active=peak_active,
            safe_additional=safe_additional,
            hard_additional=hard_additional,
            limiting=limiting,
        )
        return {
            "status": status,
            "conclusion": conclusion,
            "observedPeakConcurrency": peak_active,
            "safeAdditionalMainPy": safe_additional,
            "hardAdditionalMainPy": hard_additional,
            "recommendedMaxConcurrency": recommended_max,
            "hardMaxConcurrency": hard_max,
            "limitingResource": limiting.get("resource", "unknown"),
            "limitingResourceLabel": limiting.get("label", "未知"),
            "details": details,
        }

    def _max_per_main(self, samples: list[dict], key: str) -> float:
        values: list[float] = []
        for sample in samples:
            active = int(sample.get("activeMainCount") or 0)
            value = float(sample.get(key) or 0)
            if active > 0 and value > 0:
                values.append(value / active)
        return max(values) if values else 0.0

    def _headroom_count(self, target: float, observed: float, per_main: float) -> int | None:
        if per_main <= 0:
            return None
        return max(0, int((target - observed) // per_main))

    def _limiting_resource(self, details: list[dict], field: str) -> dict:
        known = [item for item in details if item.get(field) is not None]
        if not known:
            return {"resource": "unknown", "label": "未知"}
        return min(known, key=lambda item: item[field])

    def _capacity_status(self, safe_additional: int | None, hard_additional: int | None) -> str:
        if hard_additional == 0:
            return "full"
        if safe_additional == 0:
            return "tight"
        if safe_additional is None:
            return "unknown"
        if safe_additional <= 2:
            return "limited"
        return "roomy"

    def _capacity_conclusion(
        self,
        status: str,
        peak_active: int,
        safe_additional: int | None,
        hard_additional: int | None,
        limiting: dict,
    ) -> str:
        limit_label = limiting.get("label", "资源")
        if status == "unknown":
            return "采样不足，暂时无法估算还能增加多少 main.py。"
        if status == "full":
            return f"本批次峰值已接近危险线，不建议再增加并发；主要瓶颈是 {limit_label}。"
        if status == "tight":
            return f"本批次还有极少余量，建议不要再增加并发；如果强行增加，极限估计还能加 {hard_additional or 0} 个 main.py。"
        return (
            f"本批次峰值并发 {peak_active} 个 main.py；按安全阈值建议还能加 {safe_additional} 个，"
            f"建议最大并发约 {peak_active + safe_additional}；极限估计还能加 {hard_additional if hard_additional is not None else '未知'} 个。"
            f"当前最先触顶的资源是 {limit_label}。"
        )

    def _read_total_cpu_ticks(self) -> int:
        try:
            with open("/proc/stat", "r", encoding="utf-8", errors="ignore") as f:
                first = f.readline()
            parts = first.split()[1:]
            return sum(int(part) for part in parts)
        except Exception:
            return 0

    def _read_mem_info(self) -> dict[str, int]:
        total = 0
        available = 0
        try:
            with open("/proc/meminfo", "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        total = int(line.split()[1]) * 1024
                    elif line.startswith("MemAvailable:"):
                        available = int(line.split()[1]) * 1024
        except Exception:
            pass
        return {"totalBytes": total, "availableBytes": available}

    def _read_loadavg(self) -> dict[str, float]:
        try:
            load1, load5, load15 = os.getloadavg()
            return {"load1": float(load1), "load5": float(load5), "load15": float(load15)}
        except Exception:
            return {"load1": 0.0, "load5": 0.0, "load15": 0.0}

    def _read_proc_usage(self, pid: int) -> dict | None:
        try:
            stat_text = pathlib.Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="ignore")
            fields = stat_text.rsplit(") ", 1)[1].split()
            ticks = int(fields[11]) + int(fields[12])
            rss_pages = int(fields[21])
            return {"ticks": ticks, "rssBytes": max(0, rss_pages) * self.page_size}
        except Exception:
            return None

    def _assess_risk(self, main_cpu_percent: float, load_ratio: float, mem_available_percent: float) -> tuple[str, list[str]]:
        risk_level = "ok"
        reasons: list[str] = []

        if main_cpu_percent >= RESOURCE_THRESHOLDS["main_cpu_percent_danger"]:
            risk_level = "danger"
            reasons.append(f"main.py 总 CPU 已到 {main_cpu_percent:.1f}%")
        elif main_cpu_percent >= RESOURCE_THRESHOLDS["main_cpu_percent_warning"]:
            risk_level = self._max_risk(risk_level, "watch")
            reasons.append(f"main.py 总 CPU 偏高 {main_cpu_percent:.1f}%")

        if load_ratio >= RESOURCE_THRESHOLDS["load_ratio_danger"]:
            risk_level = "danger"
            reasons.append(f"系统负载/CPU 核数已到 {load_ratio:.2f}")
        elif load_ratio >= RESOURCE_THRESHOLDS["load_ratio_warning"]:
            risk_level = self._max_risk(risk_level, "watch")
            reasons.append(f"系统负载/CPU 核数偏高 {load_ratio:.2f}")

        if 0 < mem_available_percent <= RESOURCE_THRESHOLDS["mem_available_percent_danger"]:
            risk_level = "danger"
            reasons.append(f"可用内存只剩 {mem_available_percent:.1f}%")
        elif 0 < mem_available_percent <= RESOURCE_THRESHOLDS["mem_available_percent_warning"]:
            risk_level = self._max_risk(risk_level, "watch")
            reasons.append(f"可用内存偏低 {mem_available_percent:.1f}%")

        if not reasons:
            reasons.append("资源占用在安全范围内")
        return risk_level, reasons

    def _max_risk(self, left: str, right: str) -> str:
        return left if self._risk_rank(left) >= self._risk_rank(right) else right

    def _risk_rank(self, level: str) -> int:
        return {"ok": 0, "watch": 1, "danger": 2}.get(level, 0)

    def _risk_label(self, level: str) -> str:
        return {"ok": "正常", "watch": "需要关注", "danger": "接近危险"}.get(level, "正常")


def _beijing_now() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)


def _safe_name(raw: str) -> str:
    text = re.sub(r"[^0-9A-Za-z._-]+", "_", str(raw or "").strip())
    return text[:60] or "user"


def _load_payload(payload_file: str) -> dict:
    with open(payload_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _iter_users(payload: dict) -> list[dict]:
    users = payload.get("users")
    if isinstance(users, list) and users:
        return users
    return [payload]


def _build_user_dispatch_payload(payload: dict, user: dict) -> dict:
    merged = dict(user)
    inherited_keys = [
        "strategy",
        "endtime",
        "seat_api_mode",
        "reserve_next_day",
        "reserve_day_offset",
        "enable_slider",
        "enable_textclick",
    ]
    for key in inherited_keys:
        if key not in merged and key in payload:
            merged[key] = payload.get(key)
    return merged


def _get_feishu_webhook() -> str:
    for env_name in [
        "SERVER_FEISHU_WEBHOOK",
        "FEISHU_WEBHOOK",
        "FEISHU_BOT_WEBHOOK",
    ]:
        value = str(os.getenv(env_name, "")).strip()
        if value:
            return value
    return ""


def _get_feishu_keyword() -> str:
    return str(os.getenv("SERVER_FEISHU_KEYWORD", "腾讯云")).strip() or "腾讯云"


def _extract_key_log_lines(log_path: pathlib.Path, limit: int = 80) -> list[str]:
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [line.rstrip() for line in f if KEY_LOG_PATTERN.search(line)]
    except OSError:
        return []
    return lines[-limit:]


def _send_feishu_text(text: str) -> dict:
    webhook = _get_feishu_webhook()
    if not webhook:
        return {"ok": False, "skipped": True, "reason": "webhook_missing"}
    keyword = _get_feishu_keyword()
    normalized_text = str(text or "")
    if keyword not in normalized_text:
        normalized_text = f"{keyword}\n{normalized_text}"

    body = json.dumps(
        {"msg_type": "text", "content": {"text": normalized_text}},
        ensure_ascii=False,
    ).encode("utf-8")
    req = urllib.request.Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            detail = resp.read().decode("utf-8", errors="ignore")
            return {
                "ok": 200 <= getattr(resp, "status", 0) < 300,
                "status": getattr(resp, "status", 0),
                "detail": detail[:300],
            }
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        return {"ok": False, "status": e.code, "detail": detail[:300]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _notify_feishu_for_user(result: dict, run_id: str):
    log_path = pathlib.Path(result["log_path"])
    key_lines = _extract_key_log_lines(log_path)
    log_excerpt = "\n".join(key_lines) if key_lines else "[no key log lines matched]"
    text = (
        "【服务器抢座日志】\n"
        f"run_id: {run_id}\n"
        f"user: {result.get('display_name') or result.get('username') or 'user'}\n"
        f"returncode: {result.get('returncode')}\n"
        f"log_path: {result.get('log_path')}\n\n"
        f"{log_excerpt}"
    )
    notify_result = _send_feishu_text(text[:3500])
    print(
        json.dumps(
            {
                "event": "feishu_notify_user",
                "run_id": run_id,
                "username": result.get("username"),
                "display_name": result.get("display_name"),
                "notify_result": notify_result,
            },
            ensure_ascii=False,
        )
    )


def _run_one(user: dict, index: int, run_dir: pathlib.Path, payload: dict, monitor: ResourceMonitor) -> dict:
    username = str(user.get("username", "")).strip()
    remark = (
        user.get("remark")
        or user.get("comments")
        or ""
    )
    nickname = (
        user.get("nickname")
        or user.get("nickName")
        or user.get("name")
        or username
        or f"user_{index + 1}"
    )
    display_name = nickname or remark or username
    log_name = display_name
    log_path = run_dir / f"{index + 1:02d}_{_safe_name(log_name)}.log"
    env = os.environ.copy()
    dispatch_payload = _build_user_dispatch_payload(payload, user)
    env["DISPATCH_PAYLOAD"] = json.dumps(dispatch_payload, ensure_ascii=False)

    cmd = [sys.executable, "main.py", "--action", "--dispatch"]
    started_at = _beijing_now().isoformat()
    with open(log_path, "w", encoding="utf-8") as log_file:
        log_file.write(f"[batch] started_at={started_at}\n")
        log_file.write(f"[batch] user={nickname}\n")
        log_file.write(f"[batch] cmd={' '.join(cmd)}\n\n")
        log_file.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT_DIR,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        monitor.register_process(
            proc.pid,
            {
                "index": index + 1,
                "username": username,
                "displayName": display_name,
                "startedAt": started_at,
            },
        )
        log_file.write(f"[batch] pid={proc.pid}\n\n")
        log_file.flush()
        try:
            returncode = proc.wait()
        finally:
            monitor.unregister_process(proc.pid)

    return {
        "index": index + 1,
        "username": username,
        "display_name": display_name,
        "remark": remark,
        "nickname": nickname,
        "returncode": returncode,
        "log_path": str(log_path),
        "started_at": started_at,
        "finished_at": _beijing_now().isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description="Run dispatch payload users in local batch mode")
    parser.add_argument("--payload-file", required=True, help="Path to dispatch payload JSON")
    parser.add_argument("--concurrency", type=int, default=0, help="Override max concurrency")
    args = parser.parse_args()

    payload = _load_payload(args.payload_file)
    users = _iter_users(payload)
    max_concurrency = args.concurrency or int(payload.get("server_max_concurrency") or 13)
    max_concurrency = max(1, min(max_concurrency, len(users)))

    run_id = payload.get("run_id") or _beijing_now().strftime("%Y%m%d_%H%M%S_%f")
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "run_id": run_id,
        "school_id": payload.get("school_id", ""),
        "school_name": payload.get("school_name", ""),
        "batch_index": payload.get("batch_index"),
        "batch_total": payload.get("batch_total"),
        "user_count": len(users),
        "max_concurrency": max_concurrency,
        "started_at": _beijing_now().isoformat(),
        "resource_monitor_path": str(run_dir / RESOURCE_MONITOR_FILE_NAME),
        "results": [],
    }

    with open(run_dir / "payload.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    monitor = ResourceMonitor(run_dir, run_id, payload, len(users), max_concurrency)
    monitor.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            futures = [
                executor.submit(_run_one, user, idx, run_dir, payload, monitor)
                for idx, user in enumerate(users)
            ]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                summary["results"].append(result)
                _notify_feishu_for_user(result, run_id)
    finally:
        monitor.stop()

    summary["results"].sort(key=lambda item: item["index"])
    summary["finished_at"] = _beijing_now().isoformat()
    summary["failed"] = sum(1 for item in summary["results"] if item["returncode"] != 0)

    with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    raise SystemExit(1 if summary["failed"] else 0)


if __name__ == "__main__":
    main()
