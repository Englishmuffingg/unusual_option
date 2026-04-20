from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd


def _canonical_value_for_signing(v: object) -> str:
    """
    把「语义相同」的值规范成同一字符串再参与签名。

    典型问题：SQLite 读出来是整数字符串 '2598'，pandas CSV 是 float 2598.0，
    直接 str() 会得到 '2598' vs '2598.0'，指纹不同 → 误插入「重复」行。
    """
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except TypeError:
        pass
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return ""
    s = s.replace(",", "")
    try:
        x = float(s)
        if x != x:  # NaN
            return ""
        if abs(x - round(x)) < 1e-9:
            return str(int(round(x)))
        return format(x, ".15g")
    except (ValueError, OverflowError):
        return s


def row_signature(row: pd.Series, content_cols: list[str]) -> str:
    parts: list[str] = []
    for c in sorted(content_cols):
        v = row[c]
        parts.append(_canonical_value_for_signing(v))
    raw = "\x1f".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def signatures_for_dataframe(df: pd.DataFrame, content_cols: list[str]) -> set[str]:
    return {row_signature(df.loc[i], content_cols) for i in range(len(df))}


def filter_new_by_signature(
    df: pd.DataFrame, content_cols: list[str], existing: Iterable[str]
) -> tuple[pd.DataFrame, int]:
    """去掉与 existing 签名重复的行（不包含时间列）。"""
    seen = set(existing)
    keep_idx: list[int] = []
    dup = 0
    for i in range(len(df)):
        sig = row_signature(df.loc[i], content_cols)
        if sig in seen:
            dup += 1
            continue
        seen.add(sig)
        keep_idx.append(i)
    return df.iloc[keep_idx].reset_index(drop=True), dup
