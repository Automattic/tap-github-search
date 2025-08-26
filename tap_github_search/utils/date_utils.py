from __future__ import annotations

from datetime import date, datetime
from typing import List


def get_last_complete_month() -> str:
    today = date.today()
    if today.month == 1:
        return f"{today.year - 1}-12"
    return f"{today.year}-{today.month - 1:02d}"


def get_last_complete_month_date() -> date:
    today = date.today()
    if today.month == 1:
        return date(today.year - 1, 12, 1)
    return date(today.year, today.month - 1, 1)


def month_to_date(month_str: str) -> date:
    year, month = map(int, month_str.split("-"))
    return date(year, month, 1)


def month_range(start_month: str, end_month: str) -> List[str]:
    months: List[str] = []
    start = datetime.strptime(f"{start_month}-01", "%Y-%m-%d")
    end = datetime.strptime(f"{end_month}-01", "%Y-%m-%d")
    current = start
    while current <= end:
        months.append(f"{current.year}-{current.month:02d}")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months
