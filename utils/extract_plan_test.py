import re

TEST_TEXT = """
自习室id：9808
座位号:166
时间段:
周一:9:00-22:00
周二:9:00-22:00
周三:9:00-22:00
周四:9:00-22:00
周五:9:00-22:00
周六:9:00-22:00
周日:9:00-22:00
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

    # 支持多行时间段输入，跳过空行，兼容中英文冒号
    # 兼容全角冒号“∶”
    pattern = r"(周[一二三四五六日天])\s*[:：∶]\s*(\d{1,2}[:：∶]\d{2})\s*-\s*(\d{1,2}[:：∶]\d{2})"
    everyday_pattern = r"每天\s*[：=∶]\s*(\d{1,2}[:：∶]\d{2})\s*-\s*(\d{1,2}[:：∶]\d{2})"

    plans = []
    # 支持时间段在任意位置，整段文本查找
    def pad_time(t):
        # 补零，兼容所有冒号
        t = t.replace("：", ":").replace("∶", ":")
        parts = t.split(":")
        if len(parts) == 2:
            hour = parts[0].zfill(2)
            minute = parts[1].zfill(2)
            return f"{hour}:{minute}"
        return t

    for m in re.findall(pattern, text):
        day_cn, start, end = m
        day_en = week_map.get(day_cn, day_cn)
        start = pad_time(start)
        end = pad_time(end)
        # 确保seatid为3位数
        seatid_padded = [s.zfill(3) for s in seatid]
        plans.append({
            "times": [start, end],
            "roomid": roomid,
            "seatid": seatid_padded,
            "seatPageId": roomid,
            "daysofweek": [day_en]
        })

    for m2 in re.findall(everyday_pattern, text):
        start, end = m2
        start = pad_time(start)
        end = pad_time(end)
        # 确保seatid为3位数
        seatid_padded = [s.zfill(3) for s in seatid]
        plans.append({
            "times": [start, end],
            "roomid": roomid,
            "seatid": seatid_padded,
            "seatPageId": roomid,
            "daysofweek": all_days
        })
    return plans

if __name__ == "__main__":
    import json

    result = extract_plan(TEST_TEXT)
    print(json.dumps(result, ensure_ascii=False, indent=2))
