from __future__ import annotations

from datetime import date, timedelta


def _floor_to_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def last_n_business_days(n: int, ref: date | None = None) -> list[date]:
    """
    返回最近 n 个工作日对应的日历日期（含 ref 当天若 ref 为工作日；若 ref 为周末则先回退到上周五）。
    顺序：从新到旧，例如周一调用可能为 [周一, 上周五, 上周四]。
    """
    if n <= 0:
        return []
    d = ref or date.today()
    d = _floor_to_weekday(d)
    out: list[date] = []
    cur = d
    while len(out) < n:
        if cur.weekday() < 5:
            out.append(cur)
        cur -= timedelta(days=1)
    return out


def retention_allowed_dates(n: int, ref: date | None = None) -> set[date]:
    """
    最近 n 个工作日对应的日历日期集合（周一至周五，不含法定节假日）。
    留存时应只保留 recorded_at 日期落在此集合内的行（避免误留中间的周末日期）。
    """
    return set(last_n_business_days(n, ref))
