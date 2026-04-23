from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from etf.pipeline import run_full as run_etf_full
from stock.dashboard import DB_PATH
from stock.dashboard_api import run_dashboard_api_server
from stock.pipeline import run_full as run_stock_full

CHICAGO_TZ = ZoneInfo("America/Chicago")
MARKET_OPEN = dt_time(8, 30)
MARKET_CLOSE = dt_time(15, 0)
RUN_INTERVAL_MINUTES = 5
WAIT_LOG_INTERVAL_SECONDS = 300


def _now_chicago() -> datetime:
    return datetime.now(CHICAGO_TZ)


def _is_trading_day(ts: datetime) -> bool:
    return ts.weekday() < 5


def _window_open(ts: datetime) -> datetime:
    return ts.astimezone(CHICAGO_TZ).replace(
        hour=MARKET_OPEN.hour,
        minute=MARKET_OPEN.minute,
        second=0,
        microsecond=0,
    )


def _window_close(ts: datetime) -> datetime:
    return ts.astimezone(CHICAGO_TZ).replace(
        hour=MARKET_CLOSE.hour,
        minute=MARKET_CLOSE.minute,
        second=0,
        microsecond=0,
    )


def _next_weekday_open(ts: datetime) -> datetime:
    candidate = _window_open(ts)
    if candidate <= ts or not _is_trading_day(candidate):
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def _floor_to_slot(ts: datetime) -> datetime:
    ts = ts.astimezone(CHICAGO_TZ).replace(second=0, microsecond=0)
    floored_minute = (ts.minute // RUN_INTERVAL_MINUTES) * RUN_INTERVAL_MINUTES
    return ts.replace(minute=floored_minute)


def _first_slot_at_or_after(ts: datetime) -> datetime:
    ts = ts.astimezone(CHICAGO_TZ)
    if not _is_trading_day(ts):
        return _next_weekday_open(ts)

    window_open = _window_open(ts)
    window_close = _window_close(ts)
    if ts < window_open:
        return window_open
    if ts > window_close:
        return _next_weekday_open(ts)

    slot = _floor_to_slot(ts)
    if slot < window_open:
        return window_open
    if slot > window_close:
        return _next_weekday_open(ts)
    return slot


def _next_slot_after(slot: datetime) -> datetime:
    slot = slot.astimezone(CHICAGO_TZ).replace(second=0, microsecond=0)
    candidate = slot + timedelta(minutes=RUN_INTERVAL_MINUTES)
    if _is_trading_day(candidate) and candidate <= _window_close(candidate):
        return candidate
    return _next_weekday_open(slot + timedelta(days=1))


def _load_latest_snapshot_times(db_path: Path) -> dict[str, str | None]:
    if not db_path.exists():
        return {"stock": None, "etf": None}

    conn = sqlite3.connect(db_path)
    try:
        out: dict[str, str | None] = {}
        for table in ["stock", "etf"]:
            row = conn.execute(
                f'SELECT MAX(COALESCE(snapshot_at, recorded_at)) FROM "{table}"'
            ).fetchone()
            out[table] = row[0] if row else None
        return out
    finally:
        conn.close()


def run_ingestion_cycle(scheduled_slot: datetime) -> None:
    actual_start = _now_chicago()
    print(
        f"[scheduler] cycle started | scheduled={scheduled_slot.isoformat()} | actual={actual_start.isoformat()}"
    )

    stock_ok = False
    etf_ok = False

    try:
        print("[scheduler] stock pipeline started")
        run_stock_full()
        stock_ok = True
        print("[scheduler] stock pipeline finished")
    except Exception as exc:
        print(f"[scheduler] stock pipeline failed: {exc}")

    try:
        print("[scheduler] etf pipeline started")
        run_etf_full()
        etf_ok = True
        print("[scheduler] etf pipeline finished")
    except Exception as exc:
        print(f"[scheduler] etf pipeline failed: {exc}")

    actual_end = _now_chicago()
    print(
        "[scheduler] cycle completed | "
        f"scheduled={scheduled_slot.isoformat()} | "
        f"finished={actual_end.isoformat()} | "
        f"stock_ok={stock_ok} | etf_ok={etf_ok}"
    )


def scheduler_loop() -> None:
    print("[scheduler] timezone=America/Chicago, weekdays only, every 5 minutes from 08:30 to 15:00")
    latest = _load_latest_snapshot_times(DB_PATH)
    print(
        "[scheduler] latest snapshots | "
        f"stock={latest['stock'] or '-'} | etf={latest['etf'] or '-'}"
    )

    next_run_at = _first_slot_at_or_after(_now_chicago())
    print(f"[scheduler] first scheduled slot {next_run_at.isoformat()}")
    last_wait_log_at: datetime | None = None

    while True:
        now = _now_chicago()
        if now >= next_run_at:
            run_ingestion_cycle(next_run_at)
            next_run_at = _next_slot_after(next_run_at)
            print(f"[scheduler] next scheduled slot {next_run_at.isoformat()}")
            last_wait_log_at = None
            continue

        should_log = (
            last_wait_log_at is None
            or (now - last_wait_log_at).total_seconds() >= WAIT_LOG_INTERVAL_SECONDS
        )
        if should_log:
            wait_seconds = max(int((next_run_at - now).total_seconds()), 0)
            print(
                f"[scheduler] waiting for slot {next_run_at.isoformat()} "
                f"({wait_seconds}s remaining)"
            )
            last_wait_log_at = now

        sleep_seconds = min(max((next_run_at - now).total_seconds(), 0), 30)
        time.sleep(sleep_seconds if sleep_seconds > 0 else 1)


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
