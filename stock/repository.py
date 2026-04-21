from __future__ import annotations

import sqlite3
from datetime import date, datetime

import pandas as pd

from stock import config
from stock.deduplicate import row_signature
from stock.database import connect, ensure_table


def _local_now_iso() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def load_existing_signatures(
    conn: sqlite3.Connection, table: str, content_cols: list[str]
) -> set[str]:
    if not content_cols:
        return set()
    cols = ", ".join(f'"{c}"' for c in sorted(content_cols))
    cur = conn.execute(f'SELECT {cols} FROM "{table}"')
    sigs: set[str] = set()
    for row in cur.fetchall():
        s = pd.Series(dict(zip(sorted(content_cols), row)))
        sigs.add(row_signature(s, content_cols))
    return sigs


def insert_rows(
    conn: sqlite3.Connection,
    table: str,
    df: pd.DataFrame,
    content_cols: list[str],
    recorded_at: str | None = None,
) -> int:
    if df.empty:
        return 0
    ts = recorded_at or _local_now_iso()
    col_names = ", ".join(["recorded_at", "is_new"] + [f'"{c}"' for c in content_cols])
    placeholders = ", ".join(["?"] * (2 + len(content_cols)))
    sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'
    def _to_py(v: object) -> object:
        if pd.isna(v):
            return None
        if hasattr(v, "item"):
            try:
                return v.item()
            except Exception:
                return v
        return v

    n = 0
    for i in range(len(df)):
        vals: list[object] = [ts, 1]
        for c in content_cols:
            vals.append(_to_py(df.loc[i, c]))
        conn.execute(sql, vals)
        n += 1
    conn.commit()
    return n


def clear_new_flag_when_not_today(
    conn: sqlite3.Connection, table: str, today: date | None = None
) -> int:
    """非「今天」录入的行取消 NEW 标记（用于 UI 红色 new）。"""
    d = (today or date.today()).isoformat()
    cur = conn.execute(
        f'UPDATE "{table}" SET is_new = 0 WHERE substr(recorded_at, 1, 10) != ?',
        (d,),
    )
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def delete_rows_outside_allowed_dates(
    conn: sqlite3.Connection, table: str, allowed_dates: set[date]
) -> int:
    """删除 recorded_at 的日历日期不在 allowed_dates（最近若干个工作日）内的行。"""
    if not allowed_dates:
        return 0
    iso = sorted(d.isoformat() for d in allowed_dates)
    placeholders = ",".join("?" * len(iso))
    cur = conn.execute(
        f'DELETE FROM "{table}" WHERE substr(recorded_at, 1, 10) NOT IN ({placeholders})',
        iso,
    )
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def delete_expired_rows_by_expiration_date(
    conn: sqlite3.Connection, table: str, today: date | None = None
) -> int:
    """
    删除 expiration_date 早于今天的过期行。
    仅处理可被 SQLite date() 识别的日期字符串。
    """
    d = (today or date.today()).isoformat()
    cur = conn.execute(
        f"""
        DELETE FROM "{table}"
        WHERE expiration_date IS NOT NULL
          AND date(expiration_date) IS NOT NULL
          AND date(expiration_date) < ?
        """,
        (d,),
    )
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def ingest_dataframe(
    df: pd.DataFrame, *, table: str | None = None
) -> tuple[int, int, int]:
    """
    校验 + 去重 + 入库。返回 (插入行数, 丢弃空值行数, 去重跳过行数)。
    同一批内先清掉非今天的 NEW，再插入新行（is_new=1）。
    """
    from stock import deduplicate, validate

    table = table or config.TABLE_NAME
    snapshot_ts: str | None = None
    work = df
    if "snapshot_at" in df.columns:
        snapshot_ts = str(df["snapshot_at"].iloc[0])
        work = df.drop(columns=["snapshot_at"])

    cleaned, dropped = validate.drop_rows_with_any_null(work)
    if cleaned.empty:
        return 0, dropped, 0

    content_cols = list(cleaned.columns)
    conn = connect()
    try:
        ensure_table(conn, table, content_cols)
        existing = load_existing_signatures(conn, table, content_cols)
        new_df, dup_skip = deduplicate.filter_new_by_signature(
            cleaned, content_cols, existing
        )
        clear_new_flag_when_not_today(conn, table)
        inserted = insert_rows(
            conn, table, new_df, content_cols, recorded_at=snapshot_ts
        )
        return inserted, dropped, dup_skip
    finally:
        conn.close()
