from __future__ import annotations

import sqlite3
from datetime import date, datetime
from uuid import uuid4

import pandas as pd

from stock import config
from stock.deduplicate import row_signature
from stock.database import connect, ensure_current_state_table, ensure_table


def _local_now_iso() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def load_existing_signature_ids(
    conn: sqlite3.Connection, table: str, content_cols: list[str]
) -> dict[str, list[int]]:
    if not content_cols:
        return {}
    ordered_cols = sorted(content_cols)
    cols = ", ".join(f'"{c}"' for c in ordered_cols)
    cur = conn.execute(f'SELECT id, {cols} FROM "{table}"')
    sig_to_ids: dict[str, list[int]] = {}
    for row in cur.fetchall():
        row_id = int(row[0])
        values = row[1:]
        s = pd.Series(dict(zip(ordered_cols, values)))
        sig = row_signature(s, content_cols)
        if sig not in sig_to_ids:
            sig_to_ids[sig] = []
        sig_to_ids[sig].append(row_id)
    return sig_to_ids


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


def _to_py(v: object) -> object:
    if pd.isna(v):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            return v
    return v


def insert_snapshot_rows(
    conn: sqlite3.Connection,
    table: str,
    df: pd.DataFrame,
    content_cols: list[str],
    *,
    snapshot_at: str,
    refresh_id: str,
    signature_flags: dict[str, tuple[int, int]],
) -> int:
    """
    仅追加写入最近快照历史层（不覆盖旧快照）。
    """
    if df.empty:
        return 0
    col_names = ", ".join(
        ["recorded_at", "snapshot_at", "refresh_id", "contract_signature", "is_new", "is_refreshed"]
        + [f'"{c}"' for c in content_cols]
    )
    placeholders = ", ".join(["?"] * (6 + len(content_cols)))
    sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'
    n = 0
    for i in range(len(df)):
        sig = row_signature(df.loc[i], content_cols)
        is_new, is_refreshed = signature_flags.get(sig, (1, 0))
        vals: list[object] = [snapshot_at, snapshot_at, refresh_id, sig, is_new, is_refreshed]
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
        f'DELETE FROM "{table}" WHERE substr(COALESCE(snapshot_at, recorded_at), 1, 10) NOT IN ({placeholders})',
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


def refresh_existing_rows_for_seen_signatures(
    conn: sqlite3.Connection,
    table: str,
    signature_to_ids: dict[str, list[int]],
    seen_signatures: set[str],
    recorded_at: str | None = None,
) -> int:
    """
    当前批次中再次出现（重复）的数据：不新增行，仅刷新 recorded_at 并标记 is_refreshed=1。
    """
    if not seen_signatures:
        return 0
    ts = recorded_at or _local_now_iso()
    row_ids: list[int] = []
    for sig in seen_signatures:
        row_ids.extend(signature_to_ids.get(sig, []))
    if not row_ids:
        return 0
    placeholders = ",".join("?" * len(row_ids))
    cur = conn.execute(
        f"""
        UPDATE "{table}"
        SET recorded_at = ?, is_refreshed = 1
        WHERE id IN ({placeholders})
        """,
        [ts] + row_ids,
    )
    conn.commit()
    return cur.rowcount if cur.rowcount is not None else 0


def ingest_dataframe(
    df: pd.DataFrame, *, table: str | None = None
) -> tuple[int, int, int]:
    """
    校验 + 去重 + 入库。返回 (插入行数, 丢弃空值行数, 去重跳过行数)。
    新逻辑：
    - 快照历史层 append（每轮独立 refresh_id）
    - current_state 层 upsert（每签名保留最新状态）
    """
    from stock import validate

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
    snapshot_ts = snapshot_ts or _local_now_iso()
    refresh_id = f"{table}-{snapshot_ts}-{uuid4().hex[:8]}"
    conn = connect()
    try:
        ensure_table(conn, table, content_cols)
        state_table = ensure_current_state_table(conn, table)

        # 只做“同一批内”去重，保证每轮快照 append 稳定。
        keep_idx: list[int] = []
        seen_batch: set[str] = set()
        dup_skip = 0
        for i in range(len(cleaned)):
            sig = row_signature(cleaned.loc[i], content_cols)
            if sig in seen_batch:
                dup_skip += 1
                continue
            seen_batch.add(sig)
            keep_idx.append(i)
        batch_df = cleaned.iloc[keep_idx].reset_index(drop=True)

        existing_state = {
            row[0]
            for row in conn.execute(f'SELECT contract_signature FROM "{state_table}"').fetchall()
            if row and row[0]
        }
        signature_flags = {
            sig: (0, 1) if sig in existing_state else (1, 0) for sig in seen_batch
        }

        inserted = insert_snapshot_rows(
            conn,
            table,
            batch_df,
            content_cols,
            snapshot_at=snapshot_ts,
            refresh_id=refresh_id,
            signature_flags=signature_flags,
        )

        upsert_current_state(
            conn,
            state_table=state_table,
            df=batch_df,
            content_cols=content_cols,
            snapshot_at=snapshot_ts,
            refresh_id=refresh_id,
        )
        clear_new_flag_when_not_today(conn, table)
        return inserted, dropped, dup_skip
    finally:
        conn.close()


def upsert_current_state(
    conn: sqlite3.Connection,
    *,
    state_table: str,
    df: pd.DataFrame,
    content_cols: list[str],
    snapshot_at: str,
    refresh_id: str,
) -> int:
    if df.empty:
        return 0

    field_map = {
        "ticker": "ticker",
        "ticker_type": "ticker_type",
        "contract_symbol": "contract_symbol",
        "contract_display_name": "contract_display_name",
        "option_type": "option_type",
        "expiration_date": "expiration_date",
        "strike": "strike",
        "dte": "dte",
        "options_volume": "options_volume",
        "open_interest": "open_interest",
        "volume_to_open_interest_ratio": "volume_to_open_interest_ratio",
        "bid_price": "bid_price",
        "ask_price": "ask_price",
        "mid_price": "mid_price",
    }
    sql = f'''
        INSERT INTO "{state_table}" (
            contract_signature, ticker, ticker_type, contract_symbol, contract_display_name,
            option_type, expiration_date, strike, dte,
            options_volume, open_interest, volume_to_open_interest_ratio,
            bid_price, ask_price, mid_price,
            first_seen_at, last_seen_at, last_refresh_id
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(contract_signature) DO UPDATE SET
            ticker=excluded.ticker,
            ticker_type=excluded.ticker_type,
            contract_symbol=excluded.contract_symbol,
            contract_display_name=excluded.contract_display_name,
            option_type=excluded.option_type,
            expiration_date=excluded.expiration_date,
            strike=excluded.strike,
            dte=excluded.dte,
            options_volume=excluded.options_volume,
            open_interest=excluded.open_interest,
            volume_to_open_interest_ratio=excluded.volume_to_open_interest_ratio,
            bid_price=excluded.bid_price,
            ask_price=excluded.ask_price,
            mid_price=excluded.mid_price,
            last_seen_at=excluded.last_seen_at,
            last_refresh_id=excluded.last_refresh_id
    '''
    n = 0
    for i in range(len(df)):
        sig = row_signature(df.loc[i], content_cols)
        vals: list[object] = [sig]
        for c in [
            "ticker",
            "ticker_type",
            "contract_symbol",
            "contract_display_name",
            "option_type",
            "expiration_date",
            "strike",
            "dte",
            "options_volume",
            "open_interest",
            "volume_to_open_interest_ratio",
            "bid_price",
            "ask_price",
            "mid_price",
        ]:
            src = field_map[c]
            vals.append(_to_py(df.loc[i, src]) if src in df.columns else None)
        vals.extend([snapshot_at, snapshot_at, refresh_id])
        conn.execute(sql, vals)
        n += 1
    conn.commit()
    return n
