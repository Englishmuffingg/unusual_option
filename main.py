from __future__ import annotations

import threading
import time
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

from etf.pipeline import run_full as run_etf_full
from stock.dashboard_api import run_dashboard_api_server
from stock.pipeline import run_full as run_stock_full

CHICAGO_TZ = ZoneInfo("America/Chicago")
MARKET_OPEN = dt_time(8, 30)
MARKET_CLOSE = dt_time(15, 0)
RUN_INTERVAL_MINUTES = 5


def _now_chicago() -> datetime:
    return datetime.now(CHICAGO_TZ)


def _is_trading_day(ts: datetime) -> bool:
    return ts.weekday() < 5


def _in_run_window(ts: datetime) -> bool:
    current = ts.timetz().replace(tzinfo=None)
    return _is_trading_day(ts) and MARKET_OPEN <= current <= MARKET_CLOSE


def _next_weekday_open(ts: datetime) -> datetime:
    candidate = ts.astimezone(CHICAGO_TZ).replace(hour=MARKET_OPEN.hour, minute=MARKET_OPEN.minute, second=0, microsecond=0)
    if candidate <= ts or not _is_trading_day(candidate):
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def _next_run_time(ts: datetime) -> datetime:
    ts = ts.astimezone(CHICAGO_TZ)
    if not _is_trading_day(ts):
        return _next_weekday_open(ts)

    window_open = ts.replace(hour=MARKET_OPEN.hour, minute=MARKET_OPEN.minute, second=0, microsecond=0)
    window_close = ts.replace(hour=MARKET_CLOSE.hour, minute=MARKET_CLOSE.minute, second=0, microsecond=0)

    if ts < window_open:
        return window_open
    if ts > window_close:
        return _next_weekday_open(ts)

    minute = ((ts.minute + RUN_INTERVAL_MINUTES - 1) // RUN_INTERVAL_MINUTES) * RUN_INTERVAL_MINUTES
    candidate = ts.replace(second=0, microsecond=0)
    if minute >= 60:
        candidate = candidate.replace(minute=0) + timedelta(hours=1)
    else:
        candidate = candidate.replace(minute=minute)

    if candidate < ts:
        candidate += timedelta(minutes=RUN_INTERVAL_MINUTES)

    if candidate > window_close:
        return _next_weekday_open(ts)
    return candidate


def run_ingestion_cycle() -> None:
    start_ts = _now_chicago().isoformat()
    print(f"[scheduler] cycle started at {start_ts}")
    try:
        print("[scheduler] running stock pipeline...")
        run_stock_full()
        print("[scheduler] stock pipeline complete")
    except Exception as exc:
        print(f"[scheduler] stock pipeline failed: {exc}")

    try:
        print("[scheduler] running etf pipeline...")
        run_etf_full()
        print("[scheduler] etf pipeline complete")
    except Exception as exc:
        print(f"[scheduler] etf pipeline failed: {exc}")


def scheduler_loop() -> None:
    print("[scheduler] timezone=America/Chicago, weekdays only, every 5 minutes from 08:30 to 15:00")
    last_slot: str | None = None
    while True:
        now = _now_chicago()
        next_run = _next_run_time(now)
        sleep_seconds = max((next_run - now).total_seconds(), 0)
        if sleep_seconds > 0:
            print(f"[scheduler] next run at {next_run.isoformat()}")
            time.sleep(min(sleep_seconds, 60))
            continue

        slot_key = next_run.isoformat()
        if slot_key != last_slot and _in_run_window(next_run):
            last_slot = slot_key
            run_ingestion_cycle()
        time.sleep(1)


def main() -> None:
    api_thread = threading.Thread(
        target=run_dashboard_api_server,
        kwargs={"host": "127.0.0.1", "port": 8000, "reload": False},
        daemon=True,
        name="dashboard-api",
    )
    api_thread.start()
    scheduler_loop()


if __name__ == "__main__":
    main()
