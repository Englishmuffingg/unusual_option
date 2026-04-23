from __future__ import annotations

import re

import pandas as pd


_RESERVED = {
    "id",
    "recorded_at",
    "snapshot_at",
    "refresh_id",
    "contract_signature",
    "is_new",
    "is_refreshed",
}


def sql_safe_column(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]", "_", str(name).strip())
    if not s or s[0].isdigit():
        s = f"c_{s}"
    if s.lower() in _RESERVED:
        s = f"src_{s}"
    return s


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [sql_safe_column(c) for c in out.columns]
    return out
