import datetime
import re


BEIJING_TZ = datetime.timezone(datetime.timedelta(hours=8))
DATE_TEXT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_PAIR_TEXT_RE = re.compile(
    r"^\s*(\d{4}-\d{2}-\d{2})\s*[,，]\s*(\d{4}-\d{2}-\d{2})\s*$"
)


def beijing_today() -> datetime.date:
    return datetime.datetime.now(BEIJING_TZ).date()


def get_beijing_date(day_offset: int = 0) -> str:
    return (beijing_today() + datetime.timedelta(days=day_offset)).strftime("%Y-%m-%d")


def is_date_text(value) -> bool:
    return bool(DATE_TEXT_RE.fullmatch(str(value or "").strip()))


def parse_times_range(times) -> list[str]:
    """把 times 统一解析成 [start, end]。"""
    if isinstance(times, (list, tuple)):
        values = [str(item or "").strip() for item in list(times)[:2]]
        while len(values) < 2:
            values.append("")
        return values

    text = str(times or "").strip()
    if not text:
        return ["", ""]

    date_pair_match = DATE_PAIR_TEXT_RE.fullmatch(text)
    if date_pair_match:
        return [date_pair_match.group(1), date_pair_match.group(2)]

    for sep in ("~", "至", "-"):
        if sep not in text:
            continue
        parts = [part.strip() for part in text.split(sep, 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            return parts

    return [text, ""]


def is_custom_day_times(times) -> bool:
    start, end = parse_times_range(times)
    return is_date_text(start) and is_date_text(end)


def infer_use_custom_day(times, use_custom_day=False) -> bool:
    return bool(use_custom_day) or is_custom_day_times(times)


def normalize_day_offset(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        offset = int(text)
    except (TypeError, ValueError):
        return None
    return max(0, offset)


def resolve_request_day(
    times,
    reserve_next_day,
    use_custom_day=False,
    reserve_day_offset=None,
) -> str:
    start, _ = parse_times_range(times)
    if bool(use_custom_day) and is_date_text(start):
        return start
    day_offset = normalize_day_offset(reserve_day_offset)
    if day_offset is None:
        day_offset = 1 if reserve_next_day else 0
    return get_beijing_date(day_offset)


def _augment_user_like_custom_day(payload: dict) -> dict:
    next_payload = dict(payload or {})
    slots = next_payload.get("slots")
    if isinstance(slots, list):
        next_slots = []
        for slot in slots:
            if not isinstance(slot, dict):
                next_slots.append(slot)
                continue
            next_slot = dict(slot)
            if infer_use_custom_day(
                next_slot.get("times"),
                next_slot.get("use_custom_day"),
            ):
                next_slot["use_custom_day"] = True
            next_slots.append(next_slot)
        next_payload["slots"] = next_slots
    elif infer_use_custom_day(
        next_payload.get("times"),
        next_payload.get("use_custom_day"),
    ):
        next_payload["use_custom_day"] = True
    return next_payload


def apply_custom_day_to_dispatch_payload(payload: dict) -> dict:
    """为 dispatch payload 中的日期格式 times 自动补 use_custom_day。"""
    next_payload = _augment_user_like_custom_day(payload)
    users = next_payload.get("users")
    if not isinstance(users, list):
        return next_payload
    next_payload["users"] = [
        _augment_user_like_custom_day(user) if isinstance(user, dict) else user
        for user in users
    ]
    return next_payload
