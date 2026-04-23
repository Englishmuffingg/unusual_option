from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from stock.fetcher import fetch_stock_dataframe
from stock.maintenance import run_maintenance
from stock.repository import ingest_dataframe
from stock.schema import normalize_dataframe_columns


def run_ingestion() -> None:
    df = fetch_stock_dataframe()
    df = normalize_dataframe_columns(df)
    ts = datetime.now(ZoneInfo("America/Chicago")).replace(microsecond=0).isoformat()
    df = df.copy()
    df["snapshot_at"] = ts
    print("[stock] 快照时间 snapshot_at（写入 recorded_at）:", ts)
    inserted, dropped, dup_skip = ingest_dataframe(df)
    print(f"[stock] 入库：新增 {inserted} 行；丢弃空值 {dropped} 行；去重跳过 {dup_skip} 行")


def run_full() -> None:
    """主流程：拉取 → 校验/去重/入库 → 留存与 NEW 标记维护。"""
    run_ingestion()
    run_maintenance()


if __name__ == "__main__":
    run_full()
