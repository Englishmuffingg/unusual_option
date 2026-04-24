from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response

try:
    from stock.database import connect, ensure_dashboard_projection_table
    from stock.dashboard import (
        DB_PATH,
        dte_profile,
        focused_contracts,
        get_focus_tickers,
        load_table_read_only,
        normalize_df,
        strike_profile,
        summarize_by_ticker,
    )
except ModuleNotFoundError:
    # 鍏煎浠庨潪椤圭洰鏍圭洰褰曠洿鎺ヨ繍琛岃剼鏈紙渚嬪锛歝d stock 鍚?python dashboard_api.py锛?
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from stock.database import connect, ensure_dashboard_projection_table  # type: ignore
    from stock.dashboard import (  # type: ignore
        DB_PATH,
        dte_profile,
        focused_contracts,
        get_focus_tickers,
        load_table_read_only,
        normalize_df,
        strike_profile,
        summarize_by_ticker,
    )

FOCUS_TICKERS = ["SPY", "GLD", "QQQ", "MSFT"]
FINGERPRINT_BUCKETS = [0, 1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000]
DASHBOARD_CACHE: dict[str, dict[str, Any]] = {}
PROJECTION_ARTIFACT_KEY = "dashboard_payload_v4"

app = FastAPI(title="Unusual Options Dashboard API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _to_float(v: Any, nd: int = 2) -> float:
    if pd.isna(v):
        return 0.0
    return round(float(v), nd)


def _dashboard_cache_signature(table: str) -> tuple[str | None, str | None, str | None, str | None]:
    if not DB_PATH.exists():
        return None, None, None, None

    conn = sqlite3.connect(DB_PATH)
    try:
        raw_row = conn.execute(
            f'''
            SELECT
                COALESCE(snapshot_at, recorded_at),
                refresh_id,
                COUNT(*) OVER ()
            FROM "{table}"
            WHERE COALESCE(snapshot_at, recorded_at) IS NOT NULL
            ORDER BY COALESCE(snapshot_at, recorded_at) DESC, refresh_id DESC, id DESC
            LIMIT 1
            '''
        ).fetchone()
        current_row = conn.execute(
            f'''
            SELECT
                COALESCE(last_seen_at, first_seen_at),
                last_refresh_id,
                COUNT(*) OVER ()
            FROM "{table}_current_state"
            WHERE COALESCE(last_seen_at, first_seen_at) IS NOT NULL
            ORDER BY COALESCE(last_seen_at, first_seen_at) DESC, last_refresh_id DESC
            LIMIT 1
            '''
        ).fetchone()
        raw_ts = raw_row[0] if raw_row else None
        raw_refresh_id = raw_row[1] if raw_row and len(raw_row) > 1 else None
        raw_count = raw_row[2] if raw_row and len(raw_row) > 2 else None
        current_ts = current_row[0] if current_row else None
        current_refresh_id = current_row[1] if current_row and len(current_row) > 1 else None
        current_count = current_row[2] if current_row and len(current_row) > 2 else None
        raw_sig = f"{raw_ts}|{raw_refresh_id or '-'}|{raw_count or 0}" if raw_ts else None
        current_sig = f"{current_ts}|{current_refresh_id or '-'}|{current_count or 0}" if current_ts else None
        return raw_sig, current_sig, raw_refresh_id, current_refresh_id
    except sqlite3.Error:
        return None, None, None, None
    finally:
        conn.close()


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def _attach_cache_status(payload: dict[str, Any], *, artifact_key: str, generated_at: str | None, refresh_status: str | None, last_attempt_at: str | None, last_success_at: str | None, last_error: str | None) -> dict[str, Any]:
    metadata = payload.setdefault("metadata", {})
    snapshot_meta = payload.setdefault("snapshot_meta", {})
    cache_meta = {
        "artifact_key": artifact_key,
        "generated_at": generated_at,
        "refresh_status": refresh_status or "unknown",
        "last_attempt_at": last_attempt_at,
        "last_success_at": last_success_at,
        "last_error": last_error,
    }
    metadata.update(
        {
            "artifact_key": artifact_key,
            "cache_generated_at": generated_at,
            "cache_refresh_status": refresh_status or "unknown",
            "cache_last_attempt_at": last_attempt_at,
            "cache_last_success_at": last_success_at,
            "cache_last_error": last_error,
        }
    )
    snapshot_meta.update(
        {
            "artifact_key": artifact_key,
            "cache_generated_at": generated_at,
            "cache_refresh_status": refresh_status or "unknown",
            "cache_last_attempt_at": last_attempt_at,
            "cache_last_success_at": last_success_at,
            "cache_last_error": last_error,
        }
    )
    payload["cache_meta"] = cache_meta
    return payload


def _load_projection_payload(table: str, raw_signature: str | None, current_signature: str | None) -> dict[str, Any] | None:
    conn = connect()
    try:
        cache_table = ensure_dashboard_projection_table(conn)
        row = conn.execute(
            f'''
            SELECT payload_json, generated_at, refresh_status, last_attempt_at, last_success_at, last_error
            FROM "{cache_table}"
            WHERE dataset = ? AND artifact_key = ? AND raw_signature IS ? AND current_signature IS ?
            ''',
            (table, PROJECTION_ARTIFACT_KEY, raw_signature, current_signature),
        ).fetchone()
        if not row or not row[0]:
            return None
        payload = json.loads(str(row[0]))
        return _attach_cache_status(
            payload,
            artifact_key=PROJECTION_ARTIFACT_KEY,
            generated_at=str(row[1]) if row[1] is not None else None,
            refresh_status=str(row[2]) if row[2] is not None else None,
            last_attempt_at=str(row[3]) if row[3] is not None else None,
            last_success_at=str(row[4]) if row[4] is not None else None,
            last_error=str(row[5]) if row[5] is not None else None,
        )
    finally:
        conn.close()


def _store_projection_payload(
    table: str,
    *,
    payload: dict[str, Any],
    raw_signature: str | None,
    current_signature: str | None,
    raw_refresh_id: str | None,
    current_refresh_id: str | None,
    snapshot_time: str | None,
    window_start_time: str | None,
    window_end_time: str | None,
    generated_at: str,
) -> None:
    conn = connect()
    try:
        cache_table = ensure_dashboard_projection_table(conn)
        conn.execute(
            f'DELETE FROM "{cache_table}" WHERE dataset = ? AND artifact_key <> ?',
            (table, PROJECTION_ARTIFACT_KEY),
        )
        conn.execute(
            f'''
            INSERT INTO "{cache_table}" (
                dataset, artifact_key, raw_signature, current_signature,
                raw_refresh_id, current_refresh_id,
                snapshot_time, window_start_time, window_end_time,
                refresh_status, last_attempt_at, last_success_at, last_error,
                generated_at, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset, artifact_key) DO UPDATE SET
                raw_signature = excluded.raw_signature,
                current_signature = excluded.current_signature,
                raw_refresh_id = excluded.raw_refresh_id,
                current_refresh_id = excluded.current_refresh_id,
                snapshot_time = excluded.snapshot_time,
                window_start_time = excluded.window_start_time,
                window_end_time = excluded.window_end_time,
                refresh_status = excluded.refresh_status,
                last_attempt_at = excluded.last_attempt_at,
                last_success_at = excluded.last_success_at,
                last_error = excluded.last_error,
                generated_at = excluded.generated_at,
                payload_json = excluded.payload_json
            ''',
            (
                table,
                PROJECTION_ARTIFACT_KEY,
                raw_signature,
                current_signature,
                raw_refresh_id,
                current_refresh_id,
                snapshot_time,
                window_start_time,
                window_end_time,
                "success",
                generated_at,
                generated_at,
                None,
                generated_at,
                json.dumps(payload, ensure_ascii=False, default=_json_default),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def mark_dashboard_projection_refresh_failed(
    table: str,
    *,
    error: str,
    raw_signature: str | None = None,
    current_signature: str | None = None,
    raw_refresh_id: str | None = None,
    current_refresh_id: str | None = None,
) -> None:
    attempted_at = _local_generated_at()
    conn = connect()
    try:
        cache_table = ensure_dashboard_projection_table(conn)
        conn.execute(
            f'DELETE FROM "{cache_table}" WHERE dataset = ? AND artifact_key <> ?',
            (table, PROJECTION_ARTIFACT_KEY),
        )
        existing = conn.execute(
            f'''
            SELECT snapshot_time, window_start_time, window_end_time, generated_at, payload_json, last_success_at
            FROM "{cache_table}"
            WHERE dataset = ? AND artifact_key = ?
            ''',
            (table, PROJECTION_ARTIFACT_KEY),
        ).fetchone()
        snapshot_time = existing[0] if existing else None
        window_start_time = existing[1] if existing else None
        window_end_time = existing[2] if existing else None
        generated_at = existing[3] if existing else attempted_at
        payload_json = existing[4] if existing and existing[4] else "{}"
        last_success_at = existing[5] if existing else None
        conn.execute(
            f'''
            INSERT INTO "{cache_table}" (
                dataset, artifact_key, raw_signature, current_signature,
                raw_refresh_id, current_refresh_id,
                snapshot_time, window_start_time, window_end_time,
                refresh_status, last_attempt_at, last_success_at, last_error,
                generated_at, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset, artifact_key) DO UPDATE SET
                raw_signature = excluded.raw_signature,
                current_signature = excluded.current_signature,
                raw_refresh_id = excluded.raw_refresh_id,
                current_refresh_id = excluded.current_refresh_id,
                refresh_status = excluded.refresh_status,
                last_attempt_at = excluded.last_attempt_at,
                last_success_at = excluded.last_success_at,
                last_error = excluded.last_error
            ''',
            (
                table,
                PROJECTION_ARTIFACT_KEY,
                raw_signature,
                current_signature,
                raw_refresh_id,
                current_refresh_id,
                snapshot_time,
                window_start_time,
                window_end_time,
                "failed",
                attempted_at,
                last_success_at,
                error[:1000],
                generated_at,
                payload_json,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _metric_bucket(value: Any) -> int:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0]
    num = float(num)
    for bucket in FINGERPRINT_BUCKETS:
        if num <= bucket:
            return int(bucket)
    return int(FINGERPRINT_BUCKETS[-1])


def _flow_desc(call_pct: float, put_pct: float) -> str:
    if call_pct >= 0.65:
        return "call-heavy"
    if put_pct >= 0.65:
        return "put-heavy"
    return "mixed"


def _build_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = summarize_by_ticker(df)
    if summary.empty:
        return summary

    summary = summary.copy()
    for col in ["call_premium_pct", "put_premium_pct", "avg_ratio", "max_ratio", "median_dte"]:
        if col in summary.columns:
            summary[col] = pd.to_numeric(summary[col], errors="coerce")
    summary["bullish_score"] = (
        summary["call_premium_pct"].fillna(0) - summary["put_premium_pct"].fillna(0)
    )
    summary["flow_desc"] = summary.apply(
        lambda r: _flow_desc(
            float(r["call_premium_pct"]) if pd.notna(r["call_premium_pct"]) else 0.0,
            float(r["put_premium_pct"]) if pd.notna(r["put_premium_pct"]) else 0.0,
        ),
        axis=1,
    )

    if "is_refreshed" in df.columns:
        ref = (
            df.groupby("ticker", dropna=False)["is_refreshed"]
            .sum()
            .rename("refreshed_rows")
            .reset_index()
        )
        summary = summary.merge(ref, on="ticker", how="left")
    else:
        summary["refreshed_rows"] = 0

    summary["refreshed_rows"] = summary["refreshed_rows"].fillna(0)
    summary["refreshed_row_pct"] = summary["refreshed_rows"] / summary["rows"].replace(0, pd.NA)
    return summary


def _focus_blocks(df: pd.DataFrame, summary: pd.DataFrame, dataset_name: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    focus = get_focus_tickers(df, dataset_name)
    for ticker in FOCUS_TICKERS:
        if ticker not in set(focus):
            continue
        row = summary[summary["ticker"] == ticker]
        if row.empty:
            continue
        r = row.iloc[0]
        contracts = focused_contracts(df, ticker, new_only=False, top_n=8)
        strike = strike_profile(df, [ticker]).sort_values("total_est_premium", ascending=False).head(6)
        out.append(
            {
                "ticker": ticker,
                "flow_desc": r.get("flow_desc", "鍙屽悜鍗氬紙"),
                "call_premium_pct": _to_float(r.get("call_premium_pct", 0), 4),
                "put_premium_pct": _to_float(r.get("put_premium_pct", 0), 4),
                "median_dte": _to_float(r.get("median_dte", 0)),
                "avg_ratio": _to_float(r.get("avg_ratio", 0)),
                "max_ratio": _to_float(r.get("max_ratio", 0)),
                "refreshed_rows": int(r.get("refreshed_rows", 0) or 0),
                "new_rows": int(r.get("new_rows", 0) or 0),
                "top_strikes": strike[["option_type", "strike", "total_est_premium"]].fillna(0).to_dict("records"),
                "top_contracts": contracts.fillna(0).to_dict("records"),
            }
        )
    return out


def _section_payload(df: pd.DataFrame, name: str, dataset_name: str) -> dict[str, Any]:
    summary = _build_summary(df)
    top_tickers = summary["ticker"].head(12).tolist() if not summary.empty else []
    dte = dte_profile(df, top_tickers).fillna(0)
    strikes = strike_profile(df, top_tickers).fillna(0)

    best_ticker = "-"
    if not summary.empty:
        s2 = summary.copy()
        s2["priority_score"] = s2["total_est_premium"].fillna(0) * (1 + s2["avg_ratio"].fillna(0) / 10)
        best_ticker = str(s2.sort_values("priority_score", ascending=False).iloc[0]["ticker"])

    bubble = summary[["ticker", "median_dte", "bullish_score", "total_est_premium", "avg_ratio", "flow_desc"]].fillna(0).to_dict("records") if not summary.empty else []

    contracts_cols = [
        "snapshot_time",
        "ticker",
        "ticker_type",
        "contract_signature",
        "contract_symbol",
        "contract_display_name",
        "option_type",
        "strike",
        "expiration_date",
        "dte",
        "options_volume",
        "open_interest",
        "volume_to_open_interest_ratio",
        "estimated_premium",
        "is_refreshed",
        "is_new",
        "recorded_at",
        "previous_options_volume",
        "current_options_volume",
        "previous_open_interest",
        "current_open_interest",
        "delta_volume",
        "delta_open_interest",
        "status",
    ]
    contracts_cols = [c for c in contracts_cols if c in df.columns]

    contracts = []
    if contracts_cols:
        contracts = (
            df[contracts_cols]
            .copy()
            .sort_values(["estimated_premium", "options_volume"], ascending=[False, False])
            .head(600)
            .where(pd.notna, None)
            .to_dict("records")
        )

    return {
        "key": name,
        "summary": summary.fillna(0).to_dict("records"),
        "bubble": bubble,
        "dte_profile": dte.to_dict("records"),
        "strike_profile": strikes[["ticker", "option_type", "strike", "total_est_premium", "rows"]].to_dict("records") if not strikes.empty else [],
        "focus_blocks": _focus_blocks(df, summary, dataset_name),
        "contracts": contracts,
        "best_ticker": best_ticker,
    }


def _light_section_payload(df: pd.DataFrame, name: str) -> dict[str, Any]:
    contracts_cols = [
        "snapshot_time",
        "ticker",
        "ticker_type",
        "contract_signature",
        "contract_symbol",
        "contract_display_name",
        "option_type",
        "strike",
        "expiration_date",
        "dte",
        "options_volume",
        "current_options_volume",
        "open_interest",
        "current_open_interest",
        "volume_to_open_interest_ratio",
        "estimated_premium",
        "bid_price",
        "ask_price",
        "mid_price",
        "is_refreshed",
        "is_new",
        "status",
        "recorded_at",
        "previous_options_volume",
        "previous_open_interest",
        "delta_volume",
        "delta_open_interest",
    ]
    contracts_cols = [c for c in contracts_cols if c in df.columns]
    contracts = []
    if contracts_cols:
        contracts = (
            df[contracts_cols]
            .copy()
            .sort_values(["estimated_premium", "options_volume"], ascending=[False, False])
            .head(600)
            .where(pd.notna, None)
            .to_dict("records")
        )

    best_ticker = "-"
    if not df.empty and "ticker" in df.columns:
        ticker_score = (
            normalize_df(df)
            .groupby("ticker", dropna=False)["estimated_premium"]
            .sum()
            .sort_values(ascending=False)
        )
        if not ticker_score.empty:
            best_ticker = str(ticker_score.index[0])

    return {
        "key": name,
        "summary": [],
        "bubble": [],
        "dte_profile": [],
        "strike_profile": [],
        "focus_blocks": [],
        "contracts": contracts,
        "best_ticker": best_ticker,
    }


def _ensure_contract_signature(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "contract_signature" not in out.columns:
        out["contract_signature"] = ""
    out["contract_signature"] = out["contract_signature"].fillna("").astype(str).str.strip()
    missing = out["contract_signature"] == ""
    for col in ["ticker", "contract_symbol", "expiration_date", "strike", "option_type"]:
        if col not in out.columns:
            out[col] = ""
    if missing.any():
        parts = (
            out["ticker"].fillna("").astype(str).str.upper()
            + "|"
            + out["contract_symbol"].fillna("").astype(str)
            + "|"
            + out["expiration_date"].fillna("").astype(str)
            + "|"
            + out["strike"].fillna("").astype(str)
            + "|"
            + out["option_type"].fillna("").astype(str).str.upper()
        )
        out.loc[missing, "contract_signature"] = parts.loc[missing]
    return out


def _aggregate_snapshot_contracts(df: pd.DataFrame) -> pd.DataFrame:
    work = _ensure_contract_signature(normalize_df(df))
    if work.empty:
        return pd.DataFrame(
            columns=[
                "contract_signature",
                "ticker",
                "ticker_type",
                "contract_symbol",
                "contract_display_name",
                "option_type",
                "strike",
                "expiration_date",
                "dte",
                "options_volume",
                "open_interest",
                "estimated_premium",
                "volume_to_open_interest_ratio",
                "bid_price",
                "ask_price",
                "mid_price",
            ]
        )

    agg_map = {
        "ticker": "first",
        "ticker_type": "first",
        "contract_symbol": "first",
        "contract_display_name": "first",
        "option_type": "first",
        "strike": "first",
        "expiration_date": "first",
        "dte": "first",
        "options_volume": "sum",
        "open_interest": "sum",
        "estimated_premium": "sum",
        "volume_to_open_interest_ratio": "first",
        "bid_price": "first",
        "ask_price": "first",
        "mid_price": "first",
    }
    return work.groupby(["contract_signature"], dropna=False).agg(agg_map).reset_index()


def _build_snapshot_fingerprint(snapshot_df: pd.DataFrame) -> str:
    agg = _aggregate_snapshot_contracts(snapshot_df)
    return _build_snapshot_fingerprint_from_agg(agg)


def _build_snapshot_fingerprint_from_agg(agg: pd.DataFrame) -> str:
    if agg.empty:
        return "empty"

    stable = agg[["contract_signature", "options_volume", "open_interest"]].copy()
    stable["contract_signature"] = stable["contract_signature"].fillna("").astype(str)
    stable["volume_bucket"] = stable["options_volume"].apply(_metric_bucket)
    stable["open_interest_bucket"] = stable["open_interest"].apply(_metric_bucket)
    stable = stable[["contract_signature", "volume_bucket", "open_interest_bucket"]]
    stable = stable.sort_values("contract_signature").reset_index(drop=True)
    payload = stable.to_json(orient="records", date_format="iso", double_precision=6)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _snapshot_batches(raw_df: pd.DataFrame) -> list[tuple[pd.DataFrame, Any]]:
    if raw_df.empty:
        return []

    work = _ensure_contract_signature(raw_df)
    if "snapshot_at" not in work.columns and "recorded_at" not in work.columns:
        return []
    snapshot_series = pd.to_datetime(work["snapshot_at"], errors="coerce") if "snapshot_at" in work.columns else pd.Series(pd.NaT, index=work.index)
    recorded_series = pd.to_datetime(work["recorded_at"], errors="coerce") if "recorded_at" in work.columns else pd.Series(pd.NaT, index=work.index)
    work["_batch_time"] = snapshot_series.fillna(recorded_series)
    work = work[work["_batch_time"].notna()].copy()
    if work.empty:
        return []

    batches: list[tuple[pd.DataFrame, Any]] = []
    if "refresh_id" not in work.columns:
        work["refresh_id"] = ""
    work["refresh_id"] = work["refresh_id"].fillna("").astype(str).str.strip()

    with_refresh = work[work["refresh_id"] != ""].copy()
    without_refresh = work[work["refresh_id"] == ""].copy()

    if not with_refresh.empty:
        ref = (
            with_refresh[["refresh_id", "_batch_time"]]
            .dropna()
            .sort_values("_batch_time")
            .drop_duplicates(subset=["refresh_id"], keep="last")
        )
        for _, row in ref.iterrows():
            refresh_id = str(row["refresh_id"])
            snapshot_time = row["_batch_time"]
            frame = with_refresh[with_refresh["refresh_id"] == refresh_id].copy()
            batches.append((frame, snapshot_time))

    if not without_refresh.empty:
        for snapshot_time in sorted(without_refresh["_batch_time"].dropna().unique()):
            frame = without_refresh[without_refresh["_batch_time"] == snapshot_time].copy()
            batches.append((frame, snapshot_time))

    batches.sort(key=lambda item: pd.to_datetime(item[1], errors="coerce"))
    return batches


def _build_contract_comparison(latest_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    cur = _aggregate_snapshot_contracts(latest_df)
    prev = _aggregate_snapshot_contracts(previous_df)
    return _build_contract_comparison_from_agg(cur, prev)


def _build_contract_comparison_from_agg(cur: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    merged = cur.merge(prev, on=["contract_signature"], how="outer", suffixes=("_cur", "_prev"))
    if merged.empty:
        return merged

    cur_keys = set(cur["contract_signature"].astype(str).tolist())
    prev_keys = set(prev["contract_signature"].astype(str).tolist())
    for col in [
        "options_volume_cur",
        "options_volume_prev",
        "open_interest_cur",
        "open_interest_prev",
        "estimated_premium_cur",
        "estimated_premium_prev",
    ]:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)

    merged["ticker"] = merged["ticker_cur"] if "ticker_cur" in merged.columns else ""
    if "ticker_prev" in merged.columns:
        merged["ticker"] = merged["ticker"].fillna(merged["ticker_prev"])
    merged["contract_symbol"] = merged["contract_symbol_cur"] if "contract_symbol_cur" in merged.columns else ""
    if "contract_symbol_prev" in merged.columns:
        merged["contract_symbol"] = merged["contract_symbol"].fillna(merged["contract_symbol_prev"])
    merged["in_latest"] = merged["contract_signature"].astype(str).isin(cur_keys)
    merged["in_previous"] = merged["contract_signature"].astype(str).isin(prev_keys)
    merged["delta_volume"] = merged["options_volume_cur"] - merged["options_volume_prev"]
    merged["delta_premium"] = merged["estimated_premium_cur"] - merged["estimated_premium_prev"]
    merged["delta_open_interest"] = merged["open_interest_cur"] - merged["open_interest_prev"]
    merged["status"] = "continued"
    merged.loc[merged["in_latest"] & ~merged["in_previous"], "status"] = "new"
    merged.loc[~merged["in_latest"] & merged["in_previous"], "status"] = "inactive"
    return merged


def _latest_previous_frames_by_effective_change(
    raw_df: pd.DataFrame,
    effective_batches: list[dict[str, Any]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, Any, Any, str]:
    empty = raw_df.iloc[0:0].copy()
    effective_batches = effective_batches if effective_batches is not None else _effective_snapshot_batches(raw_df)
    if not effective_batches:
        return empty, empty, None, None, "effective_change_previous"

    latest_batch = effective_batches[-1]
    previous_batch = effective_batches[-2] if len(effective_batches) >= 2 else None
    previous_df = previous_batch["frame"].copy() if previous_batch is not None else empty
    previous_ts = previous_batch["snapshot_time"] if previous_batch is not None else None

    return (
        latest_batch["frame"].copy(),
        previous_df,
        latest_batch["snapshot_time"],
        previous_ts,
        "effective_change_previous",
    )


def _effective_snapshot_batches(raw_df: pd.DataFrame) -> list[dict[str, Any]]:
    ordered = _snapshot_batches(raw_df)
    effective: list[dict[str, Any]] = []
    last_fingerprint: str | None = None
    for frame, snapshot_time in ordered:
        agg = _aggregate_snapshot_contracts(frame)
        fingerprint = _build_snapshot_fingerprint_from_agg(agg)
        if fingerprint == last_fingerprint:
            continue
        effective.append(
            {
                "snapshot_time": snapshot_time,
                "fingerprint": fingerprint,
                "frame": frame.copy(),
                "agg": agg,
            }
        )
        last_fingerprint = fingerprint
    return effective


def _snapshot_summary(df: pd.DataFrame) -> dict[str, Any]:
    work = normalize_df(df)
    if work.empty:
        return {
            "current_total": 0,
            "new_count": 0,
            "continued_count": 0,
            "put_ratio": 0.0,
            "call_ratio": 0.0,
            "dominant_ticker": "-",
            "dominant_dte_bucket": "-",
            "dominant_strike_bucket": "-",
        }

    total = len(work)
    put_count = int((work["option_type"] == "PUT").sum())
    call_count = int((work["option_type"] == "CALL").sum())
    dominant_ticker = (
        work.groupby("ticker", dropna=False)["estimated_premium"].sum().sort_values(ascending=False).index[0]
        if not work.empty
        else "-"
    )
    dominant_dte = (
        work.groupby("dte_bucket", dropna=False)["estimated_premium"].sum().sort_values(ascending=False).index[0]
        if "dte_bucket" in work.columns and work["dte_bucket"].notna().any()
        else "-"
    )
    strike_bucket = strike_profile(work, work["ticker"].dropna().unique().tolist()[:20])
    dominant_strike = "-"
    if not strike_bucket.empty:
        top_strike = strike_bucket.sort_values("total_est_premium", ascending=False).iloc[0]
        dominant_strike = f"{top_strike['ticker']} {top_strike['strike']}"

    return {
        "current_total": total,
        "new_count": int(work["is_new"].sum()) if "is_new" in work.columns else 0,
        "continued_count": int(work["is_refreshed"].sum()) if "is_refreshed" in work.columns else 0,
        "put_ratio": _to_float(put_count / total if total else 0, 4),
        "call_ratio": _to_float(call_count / total if total else 0, 4),
        "dominant_ticker": str(dominant_ticker),
        "dominant_dte_bucket": str(dominant_dte),
        "dominant_strike_bucket": dominant_strike,
    }


def _top_metric_rows(rows: list[dict[str, Any]], key: str, top_n: int = 8) -> list[dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: float(row.get(key) or 0), reverse=True)
    return ranked[:top_n]


def _build_current_overview(df: pd.DataFrame) -> dict[str, Any]:
    work = normalize_df(df)
    if work.empty:
        return {
            "active_contract_count": 0,
            "active_ticker_count": 0,
            "top_ticker": "-",
            "put_ratio": 0.0,
            "call_ratio": 0.0,
            "dte_distribution": [],
            "ticker_volume_top": [],
            "top_active_contracts": [],
        }

    ticker_summary = (
        work.groupby("ticker", dropna=False)
        .agg(total_volume=("options_volume", "sum"), rows=("contract_symbol", "count"))
        .reset_index()
        .sort_values(["total_volume", "rows"], ascending=[False, False])
    )
    dte_summary = (
        work.groupby("dte_bucket", dropna=False)
        .agg(total_volume=("options_volume", "sum"))
        .reset_index()
        .sort_values("total_volume", ascending=False)
    )
    total_rows = len(work)
    put_ratio = float((work["option_type"] == "PUT").sum()) / total_rows if total_rows else 0.0
    call_ratio = float((work["option_type"] == "CALL").sum()) / total_rows if total_rows else 0.0

    top_contracts = (
        work.sort_values(["estimated_premium", "options_volume"], ascending=[False, False])
        .head(6)[
            [
                "ticker",
                "contract_symbol",
                "contract_display_name",
                "option_type",
                "expiration_date",
                "strike",
                "options_volume",
                "open_interest",
                "volume_to_open_interest_ratio",
            ]
        ]
        .where(pd.notna, None)
        .to_dict("records")
    )

    return {
        "active_contract_count": total_rows,
        "active_ticker_count": int(work["ticker"].nunique()),
        "top_ticker": str(ticker_summary.iloc[0]["ticker"]) if not ticker_summary.empty else "-",
        "put_ratio": _to_float(put_ratio, 4),
        "call_ratio": _to_float(call_ratio, 4),
        "dte_distribution": dte_summary.where(pd.notna, None).to_dict("records"),
        "ticker_volume_top": ticker_summary.head(8).where(pd.notna, None).to_dict("records"),
        "top_active_contracts": top_contracts,
    }


def _build_change_feed_from_batches(
    effective_batches: list[dict[str, Any]],
    *,
    window_start: pd.Timestamp | None = None,
    window_end: pd.Timestamp | None = None,
    include_inactive: bool = False,
    limit: int | None = 200,
) -> list[dict[str, Any]]:
    if len(effective_batches) < 2:
        return []

    events: list[dict[str, Any]] = []
    latest_ts = pd.to_datetime(effective_batches[-1]["snapshot_time"], errors="coerce")
    if window_end is None and pd.notna(latest_ts):
        window_end = latest_ts
    if window_start is None and pd.notna(latest_ts):
        window_start = latest_ts.normalize()

    for idx in range(1, len(effective_batches)):
        previous_batch = effective_batches[idx - 1]
        latest_batch = effective_batches[idx]
        latest_batch_ts = pd.to_datetime(latest_batch["snapshot_time"], errors="coerce")
        if pd.isna(latest_batch_ts):
            continue
        if window_start is not None and latest_batch_ts < window_start:
            continue
        if window_end is not None and latest_batch_ts > window_end:
            continue
        comparison = _build_contract_comparison_from_agg(latest_batch["agg"], previous_batch["agg"])
        if comparison.empty:
            continue

        comparison["event_time"] = latest_batch_ts
        comparison["previous_snapshot_time"] = str(previous_batch["snapshot_time"])
        comparison["current_snapshot_time"] = str(latest_batch["snapshot_time"])

        new_rows = comparison[comparison["status"] == "new"].copy()
        if not new_rows.empty:
            new_rows["event_type"] = "NEW"
            new_rows["rank_score"] = (
                pd.to_numeric(new_rows["estimated_premium_cur"], errors="coerce").fillna(0)
                + pd.to_numeric(new_rows["options_volume_cur"], errors="coerce").fillna(0)
            )
            events.extend(new_rows.sort_values("rank_score", ascending=False).head(20).to_dict("records"))

        if include_inactive:
            inactive_rows = comparison[comparison["status"] == "inactive"].copy()
            if not inactive_rows.empty:
                inactive_rows["event_type"] = "INACTIVE"
                inactive_rows["rank_score"] = (
                    pd.to_numeric(inactive_rows["estimated_premium_prev"], errors="coerce").fillna(0)
                    + pd.to_numeric(inactive_rows["options_volume_prev"], errors="coerce").fillna(0)
                )
                events.extend(inactive_rows.sort_values("rank_score", ascending=False).head(10).to_dict("records"))

        update_rows = comparison[
            (comparison["status"] == "continued")
            & (
                comparison["delta_volume"].ne(0)
                | comparison["delta_open_interest"].ne(0)
                | comparison["delta_premium"].ne(0)
            )
        ].copy()
        if not update_rows.empty:
            update_rows["event_type"] = "UPDATE"
            update_rows["rank_score"] = (
                pd.to_numeric(update_rows["delta_volume"], errors="coerce").fillna(0).abs()
                + pd.to_numeric(update_rows["delta_open_interest"], errors="coerce").fillna(0).abs()
                + pd.to_numeric(update_rows["delta_premium"], errors="coerce").fillna(0).abs()
            )
            events.extend(update_rows.sort_values("rank_score", ascending=False).head(40).to_dict("records"))

    if not events:
        return []

    feed = pd.DataFrame(events)
    if feed.empty:
        return []

    feed["event_time"] = pd.to_datetime(feed["event_time"], errors="coerce")
    feed["current_options_volume"] = pd.to_numeric(feed.get("options_volume_cur", 0), errors="coerce").fillna(0)
    feed["previous_options_volume"] = pd.to_numeric(feed.get("options_volume_prev", 0), errors="coerce").fillna(0)
    feed["current_open_interest"] = pd.to_numeric(feed.get("open_interest_cur", 0), errors="coerce").fillna(0)
    feed["previous_open_interest"] = pd.to_numeric(feed.get("open_interest_prev", 0), errors="coerce").fillna(0)
    premium_cur = pd.to_numeric(feed["estimated_premium_cur"], errors="coerce") if "estimated_premium_cur" in feed.columns else pd.Series(0, index=feed.index)
    premium_prev = pd.to_numeric(feed["estimated_premium_prev"], errors="coerce") if "estimated_premium_prev" in feed.columns else pd.Series(0, index=feed.index)
    feed["estimated_premium"] = premium_cur.fillna(premium_prev.fillna(0))
    display_cur = feed["contract_display_name_cur"] if "contract_display_name_cur" in feed.columns else pd.Series("", index=feed.index)
    display_prev = feed["contract_display_name_prev"] if "contract_display_name_prev" in feed.columns else pd.Series("", index=feed.index)
    type_cur = feed["option_type_cur"] if "option_type_cur" in feed.columns else pd.Series("", index=feed.index)
    type_prev = feed["option_type_prev"] if "option_type_prev" in feed.columns else pd.Series("", index=feed.index)
    exp_cur = feed["expiration_date_cur"] if "expiration_date_cur" in feed.columns else pd.Series("", index=feed.index)
    exp_prev = feed["expiration_date_prev"] if "expiration_date_prev" in feed.columns else pd.Series("", index=feed.index)
    feed["contract_display_name"] = display_cur.fillna(display_prev)
    feed["option_type"] = type_cur.fillna(type_prev)
    feed["expiration_date"] = exp_cur.fillna(exp_prev)
    feed["strike"] = pd.to_numeric(feed.get("strike_cur", 0), errors="coerce").fillna(
        pd.to_numeric(feed.get("strike_prev", 0), errors="coerce").fillna(0)
    )
    feed["dte"] = pd.to_numeric(feed.get("dte_cur", 0), errors="coerce").fillna(
        pd.to_numeric(feed.get("dte_prev", 0), errors="coerce").fillna(0)
    )
    for cur_col, prev_col, out_col in [
        ("bid_price_cur", "bid_price_prev", "bid_price"),
        ("ask_price_cur", "ask_price_prev", "ask_price"),
        ("mid_price_cur", "mid_price_prev", "mid_price"),
        ("volume_to_open_interest_ratio_cur", "volume_to_open_interest_ratio_prev", "volume_to_open_interest_ratio"),
    ]:
        cur_series = pd.to_numeric(feed.get(cur_col, 0), errors="coerce") if cur_col in feed.columns else pd.Series(0, index=feed.index)
        prev_series = pd.to_numeric(feed.get(prev_col, 0), errors="coerce") if prev_col in feed.columns else pd.Series(0, index=feed.index)
        feed[out_col] = cur_series.fillna(prev_series.fillna(0))
    priority_map = {"NEW": 0, "UPDATE": 1, "INACTIVE": 2}
    feed["event_priority"] = feed["event_type"].map(priority_map).fillna(9)
    if "rank_score" not in feed.columns:
        feed["rank_score"] = 0
    feed = feed.sort_values(
        ["event_priority", "event_time", "rank_score", "estimated_premium"],
        ascending=[True, False, False, False],
    )
    if limit is not None:
        feed = feed.head(limit)
    out_cols = [
        "event_time",
        "event_type",
        "ticker",
        "contract_signature",
        "contract_symbol",
        "contract_display_name",
        "option_type",
        "expiration_date",
        "strike",
        "dte",
        "previous_options_volume",
        "current_options_volume",
        "delta_volume",
        "previous_open_interest",
        "current_open_interest",
        "delta_open_interest",
        "estimated_premium",
        "bid_price",
        "ask_price",
        "mid_price",
        "volume_to_open_interest_ratio",
        "previous_snapshot_time",
        "current_snapshot_time",
    ]
    return feed[out_cols].where(pd.notna(feed[out_cols]), None).to_dict("records")


def _intraday_window_start(
    raw_df: pd.DataFrame,
    latest_ts: Any,
    effective_batches: list[dict[str, Any]] | None = None,
) -> pd.Timestamp | None:
    latest = pd.to_datetime(latest_ts, errors="coerce")
    if pd.isna(latest):
        return None

    effective_batches = effective_batches if effective_batches is not None else _effective_snapshot_batches(raw_df)
    same_day = [
        pd.to_datetime(batch["snapshot_time"], errors="coerce")
        for batch in effective_batches
        if pd.to_datetime(batch["snapshot_time"], errors="coerce").date() == latest.date()
    ]
    same_day = [ts for ts in same_day if pd.notna(ts)]
    if same_day:
        return min(same_day)
    return latest.normalize() + pd.Timedelta(hours=8, minutes=30)


def _build_period_summary(
    change_feed: list[dict[str, Any]],
    *,
    window_start: pd.Timestamp | None,
    window_end: pd.Timestamp | None,
) -> list[dict[str, Any]]:
    if not change_feed:
        return []
    feed = pd.DataFrame(change_feed)
    feed["event_time"] = pd.to_datetime(feed["event_time"], errors="coerce")
    feed = feed[feed["event_time"].notna()].copy()
    if feed.empty:
        return []

    if window_end is None:
        window_end = feed["event_time"].max()
    if window_start is None:
        window_start = feed["event_time"].min()

    window = feed[(feed["event_time"] >= window_start) & (feed["event_time"] <= window_end)].copy()
    if window.empty:
        return []

    summary = (
        window.groupby("ticker", dropna=False)
        .agg(
            total_new_count=("event_type", lambda s: int((s == "NEW").sum())),
            total_update_count=("event_type", lambda s: int((s == "UPDATE").sum())),
            cumulative_delta_volume=("delta_volume", "sum"),
            cumulative_delta_open_interest=("delta_open_interest", "sum"),
            cumulative_estimated_premium=("estimated_premium", "sum"),
            event_count=("event_type", "count"),
            last_event_time=("event_time", "max"),
            put_events=("option_type", lambda s: int((s.astype(str).str.upper() == "PUT").sum())),
            call_events=("option_type", lambda s: int((s.astype(str).str.upper() == "CALL").sum())),
        )
        .reset_index()
        .sort_values(
            ["event_count", "cumulative_delta_volume", "cumulative_delta_open_interest"],
            ascending=[False, False, False],
        )
    )
    total_events = (summary["put_events"] + summary["call_events"]).replace(0, pd.NA)
    summary["put_ratio"] = (summary["put_events"] / total_events).fillna(0)
    summary["call_ratio"] = (summary["call_events"] / total_events).fillna(0)
    summary["window_start_time"] = str(window_start)
    summary["window_end_time"] = str(window_end)
    summary = summary.drop(columns=["put_events", "call_events"])
    return summary.where(pd.notna(summary), None).to_dict("records")


def _build_period_summary_from_batches(
    effective_batches: list[dict[str, Any]],
    *,
    window_start: pd.Timestamp | None,
    window_end: pd.Timestamp | None,
) -> list[dict[str, Any]]:
    if len(effective_batches) < 2 or window_end is None:
        return []
    if window_start is None:
        window_start = window_end

    ticker_totals: dict[str, dict[str, Any]] = {}
    for idx in range(1, len(effective_batches)):
        previous_batch = effective_batches[idx - 1]
        latest_batch = effective_batches[idx]
        event_time = pd.to_datetime(latest_batch["snapshot_time"], errors="coerce")
        if pd.isna(event_time) or event_time < window_start or event_time > window_end:
            continue

        comparison = _build_contract_comparison_from_agg(latest_batch["agg"], previous_batch["agg"])
        if comparison.empty:
            continue
        comparison = comparison[
            (comparison["status"] == "new")
            | (
                (comparison["status"] == "continued")
                & (
                    comparison["delta_volume"].ne(0)
                    | comparison["delta_open_interest"].ne(0)
                    | comparison["delta_premium"].ne(0)
                )
            )
        ].copy()
        if comparison.empty:
            continue

        comparison["resolved_ticker"] = comparison.get("ticker", "").fillna("")
        option_type = comparison.get("option_type_cur")
        if option_type is None:
            option_type = pd.Series("", index=comparison.index)
        option_type_prev = comparison.get("option_type_prev")
        if option_type_prev is not None:
            option_type = option_type.fillna(option_type_prev)
        comparison["resolved_option_type"] = option_type.astype(str).str.upper()

        grouped = (
            comparison.groupby("resolved_ticker", dropna=False)
            .agg(
                total_new_count=("status", lambda s: int((s == "new").sum())),
                total_update_count=("status", lambda s: int((s == "continued").sum())),
                cumulative_delta_volume=("delta_volume", "sum"),
                cumulative_delta_open_interest=("delta_open_interest", "sum"),
                cumulative_estimated_premium=("delta_premium", "sum"),
                event_count=("status", "count"),
                put_events=("resolved_option_type", lambda s: int((s == "PUT").sum())),
                call_events=("resolved_option_type", lambda s: int((s == "CALL").sum())),
            )
            .reset_index()
        )

        for row in grouped.to_dict("records"):
            ticker = str(row.get("resolved_ticker") or "-").upper()
            acc = ticker_totals.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "window_start_time": str(window_start),
                    "window_end_time": str(window_end),
                    "last_event_time": str(event_time),
                    "total_new_count": 0,
                    "total_update_count": 0,
                    "cumulative_delta_volume": 0.0,
                    "cumulative_delta_open_interest": 0.0,
                    "cumulative_estimated_premium": 0.0,
                    "event_count": 0,
                    "put_events": 0,
                    "call_events": 0,
                },
            )
            acc["last_event_time"] = str(event_time)
            acc["total_new_count"] += int(row.get("total_new_count") or 0)
            acc["total_update_count"] += int(row.get("total_update_count") or 0)
            acc["cumulative_delta_volume"] += float(row.get("cumulative_delta_volume") or 0)
            acc["cumulative_delta_open_interest"] += float(row.get("cumulative_delta_open_interest") or 0)
            acc["cumulative_estimated_premium"] += float(row.get("cumulative_estimated_premium") or 0)
            acc["event_count"] += int(row.get("event_count") or 0)
            acc["put_events"] += int(row.get("put_events") or 0)
            acc["call_events"] += int(row.get("call_events") or 0)

    if not ticker_totals:
        return []

    out: list[dict[str, Any]] = []
    for row in ticker_totals.values():
        total_cp = row["put_events"] + row["call_events"]
        row["put_ratio"] = _to_float(row["put_events"] / total_cp if total_cp else 0, 4)
        row["call_ratio"] = _to_float(row["call_events"] / total_cp if total_cp else 0, 4)
        row.pop("put_events", None)
        row.pop("call_events", None)
        out.append(row)

    out.sort(
        key=lambda row: (
            int(row.get("event_count") or 0),
            float(row.get("cumulative_delta_volume") or 0),
            float(row.get("cumulative_delta_open_interest") or 0),
        ),
        reverse=True,
    )
    return out


def _build_refresh_delta(raw_df: pd.DataFrame) -> dict[str, Any]:
    latest_df, prev_df, latest_ts, previous_ts, comparison_mode = _latest_previous_frames_by_effective_change(raw_df)
    if latest_df.empty:
        return {
            "snapshot_time": None,
            "previous_snapshot_time": None,
            "comparison_mode": comparison_mode,
            "contract_count_delta": 0,
            "options_volume_delta": 0,
            "estimated_premium_delta": 0,
            "open_interest_delta": 0,
            "new_contract_count": 0,
            "persistent_contract_count": 0,
            "ticker_rank": [],
            "contract_changes": [],
        }

    cur = _aggregate_snapshot_contracts(latest_df)
    prev = _aggregate_snapshot_contracts(prev_df)
    merged = _build_contract_comparison_from_agg(cur, prev)

    ticker_rank = (
        merged.groupby("ticker", dropna=False)
        .agg(
            premium_delta=("delta_premium", "sum"),
            volume_delta=("delta_volume", "sum"),
            open_interest_delta=("delta_open_interest", "sum"),
            contract_count_delta=("contract_signature", "count"),
        )
        .reset_index()
        .sort_values("premium_delta", ascending=False)
        .head(15)
    )

    changes_cols = [
        "ticker",
        "contract_symbol",
        "contract_display_name_cur",
        "contract_display_name_prev",
        "option_type_cur",
        "option_type_prev",
        "strike_cur",
        "strike_prev",
        "expiration_date_cur",
        "expiration_date_prev",
        "dte_cur",
        "dte_prev",
        "options_volume_cur",
        "options_volume_prev",
        "open_interest_cur",
        "open_interest_prev",
        "estimated_premium_cur",
        "estimated_premium_prev",
        "delta_volume",
        "delta_premium",
        "delta_open_interest",
        "status",
    ]
    change_rows = merged[changes_cols].copy()
    change_rows = change_rows.sort_values("delta_premium", ascending=False).head(800)

    return {
        "snapshot_time": str(latest_ts) if latest_ts is not None else None,
        "previous_snapshot_time": str(previous_ts) if previous_ts is not None else None,
        "comparison_mode": comparison_mode,
        "contract_count_delta": int(len(cur) - len(prev)),
        "options_volume_delta": _to_float(cur["options_volume"].sum() - prev["options_volume"].sum(), 0),
        "estimated_premium_delta": _to_float(cur["estimated_premium"].sum() - prev["estimated_premium"].sum(), 2),
        "open_interest_delta": _to_float(cur["open_interest"].sum() - prev["open_interest"].sum(), 0),
        "new_contract_count": int((merged["status"] == "new").sum()),
        "persistent_contract_count": int((merged["status"] == "continued").sum()),
        "ticker_rank": ticker_rank.fillna(0).to_dict("records"),
        "contract_changes": change_rows.where(pd.notna(change_rows), None).to_dict("records"),
    }


def _build_refresh_delta_from_comparison(
    cur: pd.DataFrame,
    prev: pd.DataFrame,
    merged: pd.DataFrame,
    *,
    latest_ts: Any,
    previous_ts: Any,
    comparison_mode: str,
) -> dict[str, Any]:
    if cur.empty and merged.empty:
        return {
            "snapshot_time": None,
            "previous_snapshot_time": None,
            "comparison_mode": comparison_mode,
            "contract_count_delta": 0,
            "options_volume_delta": 0,
            "estimated_premium_delta": 0,
            "open_interest_delta": 0,
            "new_contract_count": 0,
            "persistent_contract_count": 0,
            "ticker_rank": [],
            "contract_changes": [],
        }

    ticker_rank = (
        merged.groupby("ticker", dropna=False)
        .agg(
            premium_delta=("delta_premium", "sum"),
            volume_delta=("delta_volume", "sum"),
            open_interest_delta=("delta_open_interest", "sum"),
            contract_count_delta=("contract_signature", "count"),
        )
        .reset_index()
        .sort_values("premium_delta", ascending=False)
        .head(15)
    ) if not merged.empty else pd.DataFrame()

    changes_cols = [
        "ticker",
        "contract_symbol",
        "contract_display_name_cur",
        "contract_display_name_prev",
        "option_type_cur",
        "option_type_prev",
        "strike_cur",
        "strike_prev",
        "expiration_date_cur",
        "expiration_date_prev",
        "dte_cur",
        "dte_prev",
        "options_volume_cur",
        "options_volume_prev",
        "open_interest_cur",
        "open_interest_prev",
        "estimated_premium_cur",
        "estimated_premium_prev",
        "delta_volume",
        "delta_premium",
        "delta_open_interest",
        "status",
    ]
    change_rows = merged[changes_cols].copy() if not merged.empty else pd.DataFrame(columns=changes_cols)
    if not change_rows.empty:
        change_rows = change_rows.sort_values("delta_premium", ascending=False).head(800)

    return {
        "snapshot_time": str(latest_ts) if latest_ts is not None else None,
        "previous_snapshot_time": str(previous_ts) if previous_ts is not None else None,
        "comparison_mode": comparison_mode,
        "contract_count_delta": int(len(cur) - len(prev)),
        "options_volume_delta": _to_float(cur["options_volume"].sum() - prev["options_volume"].sum(), 0) if not cur.empty or not prev.empty else 0,
        "estimated_premium_delta": _to_float(cur["estimated_premium"].sum() - prev["estimated_premium"].sum(), 2) if not cur.empty or not prev.empty else 0,
        "open_interest_delta": _to_float(cur["open_interest"].sum() - prev["open_interest"].sum(), 0) if not cur.empty or not prev.empty else 0,
        "new_contract_count": int((merged["status"] == "new").sum()) if not merged.empty else 0,
        "persistent_contract_count": int((merged["status"] == "continued").sum()) if not merged.empty else 0,
        "ticker_rank": ticker_rank.fillna(0).to_dict("records") if not ticker_rank.empty else [],
        "contract_changes": change_rows.where(pd.notna(change_rows), None).to_dict("records"),
    }


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
    <html>
      <head><meta charset="utf-8"><title>Unusual Options Dashboard API</title></head>
      <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2>Unusual Options Dashboard API 宸插惎鍔?/h2>
        <p>杩欐槸 JSON API 鏈嶅姟锛屾帴鍙ｉ粯璁よ繑鍥炵揣鍑?JSON锛堝崟琛屾樉绀烘槸姝ｅ父鐨勶級銆?/p>
        <ul>
          <li><a href="/api/health">/api/health</a></li>
          <li><a href="/api/dashboard?table=stock&pretty=1">/api/dashboard?table=stock&pretty=1</a></li>
          <li><a href="/api/dashboard?table=etf&pretty=1">/api/dashboard?table=etf&pretty=1</a></li>
          <li><a href="/docs">/docs</a> (Swagger)</li>
        </ul>
      </body>
    </html>
    """


def _local_generated_at() -> str:
    return pd.Timestamp.now(tz="America/Chicago").replace(microsecond=0).isoformat()


def _generate_dashboard_payload(table: str) -> dict[str, Any]:
    raw_signature, current_signature, raw_refresh_id, current_refresh_id = _dashboard_cache_signature(table)
    try:
        raw = load_table_read_only(DB_PATH, table)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"读取数据库失败: {exc}") from exc

    try:
        current_raw = load_table_read_only(DB_PATH, f"{table}_current_state")
    except Exception:
        current_raw = pd.DataFrame()

    effective_batches = _effective_snapshot_batches(raw)
    latest_raw, previous_raw, latest_ts, previous_ts, comparison_mode = _latest_previous_frames_by_effective_change(
        raw,
        effective_batches=effective_batches,
    )
    latest_df = _ensure_contract_signature(normalize_df(latest_raw))
    previous_df = _ensure_contract_signature(normalize_df(previous_raw))
    latest_agg = effective_batches[-1]["agg"] if effective_batches else _aggregate_snapshot_contracts(latest_raw)
    previous_agg = effective_batches[-2]["agg"] if len(effective_batches) >= 2 else _aggregate_snapshot_contracts(previous_raw)
    comparison_df = _build_contract_comparison_from_agg(latest_agg, previous_agg)

    if current_raw.empty:
        df = latest_df.copy()
    else:
        df = _ensure_contract_signature(normalize_df(current_raw))
        df["recorded_at"] = current_raw.get("last_seen_at")
        df["snapshot_at"] = current_raw.get("last_seen_at")
        df["refresh_id"] = current_raw.get("last_refresh_id")

    latest_keys = set(latest_df["contract_signature"].astype(str).tolist()) if not latest_df.empty else set()
    prev_keys = set(previous_df["contract_signature"].astype(str).tolist()) if not previous_df.empty else set()
    if latest_keys:
        df = df[df["contract_signature"].isin(latest_keys)].copy()

    df["is_new"] = df["contract_signature"].isin(latest_keys - prev_keys).astype(int)
    df["is_refreshed"] = df["contract_signature"].isin(latest_keys & prev_keys).astype(int)
    df["inactive"] = 0
    df["snapshot_time"] = str(latest_ts) if latest_ts is not None else None
    for col in [
        "previous_options_volume",
        "previous_open_interest",
        "delta_volume",
        "delta_open_interest",
        "current_options_volume",
        "current_open_interest",
    ]:
        if col in df.columns:
            df = df.drop(columns=[col])

    if not comparison_df.empty:
        previous_fields = comparison_df[
            [
                "contract_signature",
                "options_volume_prev",
                "open_interest_prev",
                "delta_volume",
                "delta_open_interest",
                "in_previous",
                "status",
            ]
        ].rename(
            columns={
                "options_volume_prev": "previous_options_volume",
                "open_interest_prev": "previous_open_interest",
            }
        )
        previous_fields["previous_options_volume"] = previous_fields["previous_options_volume"].where(
            previous_fields["in_previous"],
            None,
        )
        previous_fields["previous_open_interest"] = previous_fields["previous_open_interest"].where(
            previous_fields["in_previous"],
            None,
        )
        previous_fields = previous_fields.drop(columns=["in_previous"])
        df = df.merge(previous_fields, on="contract_signature", how="left")
        df["status"] = df["status"].fillna("new")
    else:
        df["previous_options_volume"] = None
        df["previous_open_interest"] = None
        df["delta_volume"] = pd.to_numeric(df["options_volume"], errors="coerce").fillna(0)
        df["delta_open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0)
        df["status"] = "new"
    df["current_options_volume"] = pd.to_numeric(df["options_volume"], errors="coerce").fillna(0)
    df["current_open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0)

    refreshed_df = df[df["is_refreshed"] == 1].copy()
    today_new_df = df[df["is_new"] == 1].copy()
    overall_section = _light_section_payload(df, "overall")
    refreshed_section = _light_section_payload(refreshed_df, "refreshed")
    today_section = _light_section_payload(today_new_df, "today_new")

    overall_summary = _build_summary(df)
    summary_payload = _snapshot_summary(df)
    latest_ts_dt = pd.to_datetime(latest_ts, errors="coerce")
    intraday_window_start = _intraday_window_start(raw, latest_ts, effective_batches=effective_batches)
    three_day_window_start = (
        latest_ts_dt.normalize() - pd.Timedelta(days=2)
        if pd.notna(latest_ts_dt)
        else None
    )
    window_end = latest_ts_dt if pd.notna(latest_ts_dt) else None
    intraday_event_rows = _build_change_feed_from_batches(
        effective_batches,
        window_start=intraday_window_start,
        window_end=window_end,
        include_inactive=False,
        limit=None,
    )
    change_feed = intraday_event_rows[:200]
    daily_summary = _build_period_summary_from_batches(
        effective_batches,
        window_start=intraday_window_start,
        window_end=window_end,
    )
    three_day_summary = _build_period_summary_from_batches(
        effective_batches,
        window_start=three_day_window_start,
        window_end=window_end,
    )
    current_overview = _build_current_overview(df)
    strongest_focus = current_overview.get("top_ticker", "-")
    generated_at = _local_generated_at()

    cards = {
        "refreshed_ticker_count": int(refreshed_df["ticker"].nunique()) if not refreshed_df.empty else 0,
        "refreshed_contract_count": int(len(refreshed_df)),
        "refreshed_total_premium": _to_float(refreshed_df["estimated_premium"].sum(), 2) if not refreshed_df.empty else 0,
        "today_new_ticker_count": int(today_new_df["ticker"].nunique()) if not today_new_df.empty else 0,
        "today_new_contract_count": int(len(today_new_df)),
        "overall_ticker_count": int(df["ticker"].nunique()) if not df.empty else 0,
        "overall_median_dte": _to_float(overall_summary["median_dte"].mean(), 2) if not overall_summary.empty else 0,
        "overall_avg_ratio": _to_float(overall_summary["avg_ratio"].mean(), 2) if not overall_summary.empty else 0,
        "best_refreshed_ticker": refreshed_section["best_ticker"],
        "best_today_new_ticker": today_section["best_ticker"],
        "strongest_focus_ticker": strongest_focus,
    }

    payload = {
        "table": table,
        "db_path": str(DB_PATH),
        "comparison_mode": comparison_mode,
        "dashboard_generated_at": generated_at,
        "artifact_key": PROJECTION_ARTIFACT_KEY,
        "metadata": {
            "dashboard_generated_at": generated_at,
            "comparison_mode": comparison_mode,
            "artifact_key": PROJECTION_ARTIFACT_KEY,
            "cache_generated_at": generated_at,
            "cache_refresh_status": "success",
            "cache_last_attempt_at": generated_at,
            "cache_last_success_at": generated_at,
            "cache_last_error": None,
            "snapshot_time": str(latest_ts) if latest_ts is not None else None,
            "latest_snapshot_time": str(latest_ts) if latest_ts is not None else None,
            "previous_snapshot_time": str(previous_ts) if previous_ts is not None else None,
            "window_start_time": str(intraday_window_start) if intraday_window_start is not None else None,
            "window_end_time": str(window_end) if window_end is not None else None,
            "raw_signature": raw_signature,
            "current_signature": current_signature,
            "raw_refresh_id": raw_refresh_id,
            "current_refresh_id": current_refresh_id,
            "data_source": table,
            "active_filter_summary": {
                "focus_tickers": FOCUS_TICKERS,
            },
        },
        "summary": summary_payload,
        "current_overview": current_overview,
        "cards": cards,
        "metric_explanations": [
            "Call premium share high = more call-heavy flow",
            "Put premium share high = more defensive or bearish flow",
            "新增 = 相对于最近一次有效变化快照新增",
            "延续 = 相对于最近一次有效变化快照继续出现",
            "comparison_mode = effective_change_previous",
            "avg_ratio / max_ratio = 异常强度参考",
        ],
        "sections": {
            "refreshed": refreshed_section,
            "today_new": today_section,
            "overall": overall_section,
        },
        "change_feed": change_feed,
        "change_feed_preview": change_feed,
        "intraday_event_rows": intraday_event_rows,
        "current_snapshot_rows": overall_section["contracts"],
        "continued_rows": refreshed_section["contracts"],
        "daily_summary": daily_summary,
        "three_day_summary": three_day_summary,
        "refresh_delta": _build_refresh_delta_from_comparison(
            latest_agg,
            previous_agg,
            comparison_df,
            latest_ts=latest_ts,
            previous_ts=previous_ts,
            comparison_mode=comparison_mode,
        ),
        "snapshot_meta": {
            "dashboard_generated_at": generated_at,
            "artifact_key": PROJECTION_ARTIFACT_KEY,
            "cache_generated_at": generated_at,
            "cache_refresh_status": "success",
            "cache_last_attempt_at": generated_at,
            "cache_last_success_at": generated_at,
            "cache_last_error": None,
            "snapshot_time": str(latest_ts) if latest_ts is not None else None,
            "latest_snapshot_time": str(latest_ts) if latest_ts is not None else None,
            "previous_snapshot_time": str(previous_ts) if previous_ts is not None else None,
            "comparison_mode": comparison_mode,
            "window_start_time": str(intraday_window_start) if intraday_window_start is not None else None,
            "window_end_time": str(window_end) if window_end is not None else None,
            "raw_signature": raw_signature,
            "current_signature": current_signature,
            "raw_refresh_id": raw_refresh_id,
            "current_refresh_id": current_refresh_id,
        },
        "cache_meta": {
            "artifact_key": PROJECTION_ARTIFACT_KEY,
            "generated_at": generated_at,
            "refresh_status": "success",
            "last_attempt_at": generated_at,
            "last_success_at": generated_at,
            "last_error": None,
        },
    }
    return jsonable_encoder(payload)


def refresh_dashboard_projection(table: str) -> dict[str, Any]:
    raw_signature, current_signature, raw_refresh_id, current_refresh_id = _dashboard_cache_signature(table)
    try:
        json_payload = _generate_dashboard_payload(table)
        metadata = json_payload.get("metadata", {})
        generated_at = str(json_payload.get("dashboard_generated_at") or _local_generated_at())
        _store_projection_payload(
            table,
            payload=json_payload,
            raw_signature=raw_signature,
            current_signature=current_signature,
            raw_refresh_id=raw_refresh_id,
            current_refresh_id=current_refresh_id,
            snapshot_time=metadata.get("snapshot_time"),
            window_start_time=metadata.get("window_start_time"),
            window_end_time=metadata.get("window_end_time"),
            generated_at=generated_at,
        )
        json_payload = _attach_cache_status(
            json_payload,
            artifact_key=PROJECTION_ARTIFACT_KEY,
            generated_at=generated_at,
            refresh_status="success",
            last_attempt_at=generated_at,
            last_success_at=generated_at,
            last_error=None,
        )
        cache_key = f"{table}:{raw_signature}:{current_signature}"
        DASHBOARD_CACHE.clear()
        DASHBOARD_CACHE[cache_key] = json_payload
        return json_payload
    except Exception as exc:
        mark_dashboard_projection_refresh_failed(
            table,
            error=str(exc),
            raw_signature=raw_signature,
            current_signature=current_signature,
            raw_refresh_id=raw_refresh_id,
            current_refresh_id=current_refresh_id,
        )
        raise


@app.get("/api/dashboard")
def dashboard_api(
    table: str = Query("stock", pattern="^(stock|etf)$"),
    pretty: int = Query(0, ge=0, le=1, description="1=formatted JSON output"),
) -> dict[str, Any]:
    raw_signature, current_signature, _raw_refresh_id, _current_refresh_id = _dashboard_cache_signature(table)
    cache_key = f"{table}:{raw_signature}:{current_signature}"
    json_payload = DASHBOARD_CACHE.get(cache_key)
    if json_payload is None:
        json_payload = _load_projection_payload(table, raw_signature, current_signature)
    if json_payload is None:
        json_payload = refresh_dashboard_projection(table)

    if pretty == 1:
        return Response(
            content=json.dumps(json_payload, ensure_ascii=False, indent=2),
            media_type="application/json; charset=utf-8",
        )
    return json_payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="鍚姩寮傚父鏈熸潈 Dashboard API 鏈嶅姟")
    parser.add_argument("--host", default="127.0.0.1", help="鐩戝惉鍦板潃锛岄粯璁?127.0.0.1")
    parser.add_argument("--port", type=int, default=8000, help="鐩戝惉绔彛锛岄粯璁?8000")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="寮€鍙戞ā寮忚嚜鍔ㄩ噸杞斤紙闇€瑕佷粠椤圭洰鏍圭洰褰曞惎鍔級",
    )
    return parser


def run_dashboard_api_server(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
) -> None:
    try:
        import uvicorn
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "?? uvicorn????? `pip install -r requirements.txt`?"
        ) from exc
    print(f"[dashboard_api] ???: http://{host}:{port}/api/dashboard?table=stock")
    if reload:
        print(
            "[dashboard_api] ????????????? --reload ??????????"
            "????????? `python run_dashboard_api.py --reload`?"
        )
    uvicorn.run(app, host=host, port=port, reload=False)


def dashboard() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    run_dashboard_api_server(host=args.host, port=args.port, reload=args.reload)





