from __future__ import annotations

import pandas as pd


def drop_rows_with_any_null(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    空值检查：任意列为空（NaN/None/空字符串）则整行丢弃。
    返回 (清洗后 DataFrame, 丢弃行数)。
    """
    if df.empty:
        return df, 0
    before = len(df)
    sub = df.replace(r"^\s*$", pd.NA, regex=True)
    sub = sub.dropna(how="any")
    after = len(sub)
    return sub.reset_index(drop=True), before - after
