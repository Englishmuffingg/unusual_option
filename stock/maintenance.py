from __future__ import annotations

from datetime import date

from stock import config
from stock.business_days import retention_allowed_dates
from stock.database import connect
from stock.repository import (
    clear_new_flag_when_not_today,
    delete_rows_outside_allowed_dates,
)


def run_maintenance(
    ref: date | None = None, *, table: str | None = None
) -> None:
    """
    仅保留最近 RETAIN_BUSINESS_DAYS 个工作日内的数据；
    并将非「今天」录入行的 is_new 置 0（界面红色 NEW 标记）。
    """
    table = table or config.TABLE_NAME
    d = ref or date.today()
    allowed = retention_allowed_dates(config.RETAIN_BUSINESS_DAYS, d)
    conn = connect()
    try:
        deleted = delete_rows_outside_allowed_dates(conn, table, allowed)
        cleared = clear_new_flag_when_not_today(conn, table, d)
        dates_str = ", ".join(sorted(x.isoformat() for x in allowed))
        print(
            f"[{table}] 维护：仅保留最近 {config.RETAIN_BUSINESS_DAYS} 个工作日（日期：{dates_str}）；"
            f"删除不在上述日期内的行 {deleted}；清除非今日 NEW 标记 {cleared} 行"
        )
    finally:
        conn.close()
