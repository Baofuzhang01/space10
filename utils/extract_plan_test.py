import re

MAX_HOURS_PER_OBJECT = 16

TEST_TEXT = """
自习室id:5559
座位号:076
时间段:
周一:
8:00-13.00,13.00-18.00,18.00-22.00
周二:
8:00-13.00,13.00-18.00,18.00-22.00
周三:
10.00-15.00,15.00-18.00,18.00-22.00
周四:
8:00-13.00,13.00-18.00,18.00-22.00
周五:
13.00-18.00,18.00-22.00
周六:
13.00-18.00,18.00-22.00
周日:
8:00-13.00,13.00-18.00,18.00-22.00
""".strip()


"""
周一:8:00-22:00
周二:8:00-22:00
周三:8:00-22:00
周四:8:00-22:00
周五:8:00-22:00
周六:8:00-22:00
周日:8:00-22:00
"""


def extract_plan(text):
    # 提取roomid和seatid
    # 全局提取 roomid 和 seatid，避免受空行影响
    roomid = ""
    seatid = []
    for line in text.splitlines():
        line = line.strip()
        roomid_match = re.match(r"自习室id[：:](\d+)", line)
        if roomid_match:
            roomid = roomid_match.group(1)
        seatid_match = re.match(r"座位号[：:](\d+)", line)
        if seatid_match:
            # 补零为3位数
            seat_num = seatid_match.group(1).zfill(3)
            seatid = [seat_num]

    # 中文星期映射
    week_map = {
        "周一": "Monday",
        "周二": "Tuesday",
        "周三": "Wednesday",
        "周四": "Thursday",
        "周五": "Friday",
        "周六": "Saturday",
        "周日": "Sunday",
        "周天": "Sunday"
    }
    all_days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    # 支持多行时间段输入，兼容：
    # 周一:8:00-12:00
    # 周一:8:00-12:00，14:00-16:00
    # 每天:8:00-12:00,14:00-16:00
    day_prefix_pattern = r"^(周[一二三四五六日天])\s*[:：∶]?\s*(.*)$"
    everyday_prefix_pattern = r"^每天\s*[:：=∶]?\s*(.*)$"
    time_range_pattern = r"(\d{1,2}[:：∶.．。]\d{2})\s*[-~—–至]\s*(\d{1,2}[:：∶.．。]\d{2})"

    plans = []
    # 支持时间段在任意位置，整段文本查找
    def pad_time(t):
        # 补零，兼容所有冒号
        t = t.replace("：", ":").replace("∶", ":").replace(".", ":").replace("．", ":").replace("。", ":")
        parts = t.split(":")
        if len(parts) == 2:
            hour = parts[0].zfill(2)
            minute = parts[1].zfill(2)
            return f"{hour}:{minute}"
        return t

    def time_to_minutes(t):
        hour, minute = pad_time(t).split(":")
        return int(hour) * 60 + int(minute)

    def minutes_to_time(total_minutes):
        hour = total_minutes // 60
        minute = total_minutes % 60
        return f"{hour:02d}:{minute:02d}"

    def split_time_range(start, end):
        start_minutes = time_to_minutes(start)
        end_minutes = time_to_minutes(end)
        if end_minutes <= start_minutes:
            return [(pad_time(start), pad_time(end))]

        max_hours = MAX_HOURS_PER_OBJECT
        if max_hours is None:
            return [(pad_time(start), pad_time(end))]

        try:
            max_minutes = int(float(max_hours) * 60)
        except (TypeError, ValueError):
            max_minutes = 0

        if max_minutes <= 0:
            return [(pad_time(start), pad_time(end))]

        segments = []
        current = start_minutes
        while current < end_minutes:
            next_end = min(current + max_minutes, end_minutes)
            segments.append((minutes_to_time(current), minutes_to_time(next_end)))
            current = next_end
        return segments

    def append_plan(daysofweek, start, end):
        seatid_padded = [s.zfill(3) for s in seatid]
        for segment_start, segment_end in split_time_range(start, end):
            plans.append({
                "times": [segment_start, segment_end],
                "roomid": roomid,
                "seatid": seatid_padded,
                "seatPageId": roomid,
                "daysofweek": daysofweek
            })

    active_days = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        day_match = re.match(day_prefix_pattern, line)
        if day_match:
            day_cn, ranges_text = day_match.groups()
            day_en = week_map.get(day_cn, day_cn)
            active_days = [day_en]
            for start, end in re.findall(time_range_pattern, ranges_text):
                append_plan([day_en], pad_time(start), pad_time(end))
            continue

        everyday_match = re.match(everyday_prefix_pattern, line)
        if everyday_match:
            ranges_text = everyday_match.group(1)
            active_days = all_days[:]
            for start, end in re.findall(time_range_pattern, ranges_text):
                append_plan(all_days, pad_time(start), pad_time(end))
            continue

        if active_days:
            for start, end in re.findall(time_range_pattern, line):
                append_plan(active_days[:], pad_time(start), pad_time(end))
    return plans

if __name__ == "__main__":
    import json

    result = extract_plan(TEST_TEXT)
    print(json.dumps(result, ensure_ascii=False, indent=2))
