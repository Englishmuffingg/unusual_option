from __future__ import annotations

import sqlite3
from pathlib import Path

from stock import config


def connect() -> sqlite3.Connection:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_table(conn: sqlite3.Connection, table: str, content_columns: list[str]) -> None:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    if cur.fetchone() is None:
        cols_sql = ", ".join(f'"{c}" TEXT' for c in content_columns)
        conn.execute(
            f'''
            CREATE TABLE "{table}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                snapshot_at TEXT,
                refresh_id TEXT,
                contract_signature TEXT,
                is_new INTEGER NOT NULL DEFAULT 1,
                is_refreshed INTEGER NOT NULL DEFAULT 0,
                {cols_sql}
            )
            '''
        )
        conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table}_snapshot_at" ON "{table}" ("snapshot_at")')
        conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table}_refresh_id" ON "{table}" ("refresh_id")')
        conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table}_contract_signature" ON "{table}" ("contract_signature")')
        conn.commit()
        return

    info = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    existing = {row[1] for row in info}
    for sys_col, ddl in [
        ("snapshot_at", 'TEXT'),
        ("refresh_id", 'TEXT'),
        ("contract_signature", 'TEXT'),
    ]:
        if sys_col not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{sys_col}" {ddl}')
    if "is_refreshed" not in existing:
        conn.execute(
            f'ALTER TABLE "{table}" ADD COLUMN "is_refreshed" INTEGER NOT NULL DEFAULT 0'
        )
    for c in content_columns:
        if c not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT')
    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table}_snapshot_at" ON "{table}" ("snapshot_at")')
    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table}_refresh_id" ON "{table}" ("refresh_id")')
    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table}_contract_signature" ON "{table}" ("contract_signature")')
    conn.commit()


def ensure_is_refreshed_column(conn: sqlite3.Connection, table: str) -> bool:
    """
    仅保证系统列 is_refreshed 存在。
    返回是否执行了新增列（True=本次新增）。
    """
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    if cur.fetchone() is None:
        return False
    info = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    existing = {row[1] for row in info}
    if "is_refreshed" in existing:
        return False
    conn.execute(
        f'ALTER TABLE "{table}" ADD COLUMN "is_refreshed" INTEGER NOT NULL DEFAULT 0'
    )
    conn.commit()
    return True


def ensure_current_state_table(conn: sqlite3.Connection, table: str) -> str:
    """
    创建/维护 current_state 表，命名为 <table>_current_state。
    """
    state_table = f"{table}_current_state"
    conn.execute(
        f'''
        CREATE TABLE IF NOT EXISTS "{state_table}" (
            contract_signature TEXT PRIMARY KEY,
            ticker TEXT,
            ticker_type TEXT,
            contract_symbol TEXT,
            contract_display_name TEXT,
            option_type TEXT,
            expiration_date TEXT,
            strike TEXT,
            dte TEXT,
            options_volume TEXT,
            open_interest TEXT,
            volume_to_open_interest_ratio TEXT,
            bid_price TEXT,
            ask_price TEXT,
            mid_price TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT,
            last_refresh_id TEXT
        )
        '''
    )
    conn.execute(
        f'CREATE INDEX IF NOT EXISTS "idx_{state_table}_ticker" ON "{state_table}" ("ticker")'
    )
    conn.execute(
        f'CREATE INDEX IF NOT EXISTS "idx_{state_table}_last_seen_at" ON "{state_table}" ("last_seen_at")'
    )
    conn.commit()
    return state_table
