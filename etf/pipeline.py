from __future__ import annotations

from datetime import datetime

from etf import config as etf_config
from etf.fetcher import fetch_etf_dataframe
from stock.maintenance import run_maintenance
from stock.repository import ingest_dataframe
from stock.schema import normalize_dataframe_columns


def run_ingestion() -> None:
    df = fetch_etf_dataframe()
    df = normalize_dataframe_columns(df)
    ts = datetime.now().astimezone().replace(microsecond=0).isoformat()
    df = df.copy()
    df["snapshot_at"] = ts
    print("[etf] 快照时间 snapshot_at（写入 recorded_at）:", ts)
    inserted, dropped, dup_skip = ingest_dataframe(df, table=etf_config.TABLE_NAME)
    print(f"[etf] 入库：新增 {inserted} 行；丢弃空值 {dropped} 行；去重跳过 {dup_skip} 行")


def run_full() -> None:
    """ETF：拉取 → 校验/去重/入库 → 留存与 NEW 维护。"""
    run_ingestion()
    run_maintenance(table=etf_config.TABLE_NAME)


if __name__ == "__main__":
    run_full()
