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
                is_new INTEGER NOT NULL DEFAULT 1,
                {cols_sql}
            )
            '''
        )
        conn.commit()
        return

    info = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    existing = {row[1] for row in info}
    for c in content_columns:
        if c not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT')
    conn.commit()
