from __future__ import annotations

from io import BytesIO, StringIO

import pandas as pd
import requests

from etf import config
from stock.http_client import build_session


def _fetch(session: requests.Session, url: str) -> requests.Response:
    r = session.get(url, timeout=60, allow_redirects=True)
    print("\nGET:", url)
    print("status:", r.status_code, "bytes:", len(r.content), "content-type:", r.headers.get("content-type"))
    return r


def _try_fetch_csv(session: requests.Session, url: str) -> pd.DataFrame | None:
    resp = _fetch(session, url)
    if resp.status_code != 200:
        return None

    content_type = (resp.headers.get("content-type") or "").lower()
    disposition = (resp.headers.get("content-disposition") or "").lower()
    head = resp.content.lstrip()[:40].lower()
    looks_like_csv_header = head.startswith(b"symbol,") or head.startswith(b"ticker,")

    is_csv = (
        content_type.startswith("text/csv")
        or "text/csv" in content_type
        or "octet-stream" in content_type
        or ".csv" in disposition
        or looks_like_csv_header
    )
    if not is_csv:
        return None

    return pd.read_csv(BytesIO(resp.content), encoding="utf-8-sig")


def _pick_best_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    if not tables:
        raise RuntimeError("HTML 里没有找到表格")
    required = {"Symbol", "Contract", "Volume/Open Interest", "Option Volume", "Open Interest"}

    def score(df: pd.DataFrame) -> tuple[int, int]:
        cols = set(str(c).strip() for c in df.columns)
        return (len(required & cols), len(df))

    return max(tables, key=score)


def _parse_html_table(html_text: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html_text), flavor="lxml")
    return _pick_best_table(tables)


def _try_fetch_html_table(session: requests.Session, url: str) -> pd.DataFrame | None:
    resp = _fetch(session, url)
    if resp.status_code != 200:
        return None
    try:
        return _parse_html_table(resp.text)
    except Exception as e:
        print("解析 HTML 表格失败：", type(e).__name__, str(e)[:200])
        return None


def fetch_etf_dataframe() -> pd.DataFrame:
    """优先全量 CSV；失败则 Show All HTML、async HTML。"""
    session = build_session()

    df_csv = _try_fetch_csv(session, config.CSV_URL)
    if df_csv is not None:
        print("ETF CSV 行数:", len(df_csv), "列数:", len(df_csv.columns))
        return df_csv

    df_html = _try_fetch_html_table(session, config.SHOW_ALL_URL)
    if df_html is not None:
        print("ETF Show All 表格行数:", len(df_html), "列数:", len(df_html.columns))
        return df_html

    print("提示：ETF 回退到 async HTML（数据可能不全）。")
    resp = _fetch(session, config.HTML_URL)
    resp.raise_for_status()
    df = _parse_html_table(resp.text)
    print("ETF Async HTML 表格行数:", len(df), "列数:", len(df.columns))
    return df
