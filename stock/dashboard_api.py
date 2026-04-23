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


def _dashboard_cache_signature(table: str) -> tuple[str | None, str | None]:
    if not DB_PATH.exists():
        return None, None

    conn = sqlite3.connect(DB_PATH)
    try:
        raw_row = conn.execute(
            f'SELECT MAX(COALESCE(snapshot_at, recorded_at)) FROM "{table}"'
        ).fetchone()
        current_row = conn.execute(
            f'SELECT MAX(COALESCE(last_seen_at, first_seen_at)) FROM "{table}_current_state"'
        ).fetchone()
        raw_sig = raw_row[0] if raw_row else None
        current_sig = current_row[0] if current_row else None
        return raw_sig, current_sig
    except sqlite3.Error:
        return None, None
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
    ts_col = "snapshot_at" if "snapshot_at" in work.columns else "recorded_at"
    if ts_col not in work.columns:
        return []

    work[ts_col] = pd.to_datetime(work[ts_col], errors="coerce")
    work = work[work[ts_col].notna()].copy()
    if work.empty:
        return []

    batches: list[tuple[pd.DataFrame, Any]] = []
    has_refresh = "refresh_id" in work.columns and work["refresh_id"].fillna("").astype(str).str.strip().ne("").any()

    if has_refresh:
        ref = (
            work[["refresh_id", ts_col]]
            .dropna()
            .assign(refresh_id=lambda df: df["refresh_id"].astype(str))
            .sort_values(ts_col)
            .drop_duplicates(subset=["refresh_id"], keep="last")
            .sort_values(ts_col, ascending=False)
        )
        for _, row in ref.iterrows():
            refresh_id = row["refresh_id"]
            snapshot_time = row[ts_col]
            frame = work[work["refresh_id"].astype(str) == refresh_id].copy()
            batches.append((frame, snapshot_time))
        return batches

    for snapshot_time in sorted(work[ts_col].dropna().unique(), reverse=True):
        frame = work[work[ts_col] == snapshot_time].copy()
        batches.append((frame, snapshot_time))
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
) -> tuple[pd.DataFrame, pd.DataFrame, Any, Any, str]:
    empty = raw_df.iloc[0:0].copy()
    effective_batches = _effective_snapshot_batches(raw_df)
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
    ordered = list(reversed(_snapshot_batches(raw_df)))
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


def _build_inactive_contract_rows(comparison_df: pd.DataFrame) -> list[dict[str, Any]]:
    if comparison_df.empty:
        return []

    inactive = comparison_df[comparison_df["status"] == "inactive"].copy()
    if inactive.empty:
        return []

    rows = pd.DataFrame(
        {
            "contract_signature": inactive["contract_signature"],
            "ticker": inactive["ticker"].fillna(""),
            "contract_symbol": inactive["contract_symbol"].fillna(""),
            "contract_display_name": inactive.get("contract_display_name_prev", ""),
            "option_type": inactive.get("option_type_prev", ""),
            "expiration_date": inactive.get("expiration_date_prev", None),
            "strike": pd.to_numeric(inactive.get("strike_prev", 0), errors="coerce").fillna(0),
            "previous_options_volume": pd.to_numeric(inactive["options_volume_prev"], errors="coerce").fillna(0),
            "previous_open_interest": pd.to_numeric(inactive["open_interest_prev"], errors="coerce").fillna(0),
        }
    )
    rows = rows.sort_values(["previous_options_volume", "previous_open_interest"], ascending=[False, False])
    return rows.where(pd.notna(rows), None).to_dict("records")


def _snapshot_summary(df: pd.DataFrame, inactive_count: int = 0) -> dict[str, Any]:
    work = normalize_df(df)
    if work.empty:
        return {
            "current_total": 0,
            "new_count": 0,
            "continued_count": 0,
            "inactive_count": inactive_count,
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
        "inactive_count": inactive_count,
        "put_ratio": _to_float(put_count / total if total else 0, 4),
        "call_ratio": _to_float(call_count / total if total else 0, 4),
        "dominant_ticker": str(dominant_ticker),
        "dominant_dte_bucket": str(dominant_dte),
        "dominant_strike_bucket": dominant_strike,
    }


def _build_change_feed(raw_df: pd.DataFrame, limit: int = 200) -> list[dict[str, Any]]:
    effective_batches = _effective_snapshot_batches(raw_df)
    if len(effective_batches) < 2:
        return []

    events: list[dict[str, Any]] = []
    recent_batches = effective_batches[-25:]
    for idx in range(1, len(recent_batches)):
        previous_batch = recent_batches[idx - 1]
        latest_batch = recent_batches[idx]
        comparison = _build_contract_comparison_from_agg(latest_batch["agg"], previous_batch["agg"])
        if comparison.empty:
            continue

        comparison["event_time"] = pd.to_datetime(latest_batch["snapshot_time"], errors="coerce")
        comparison["previous_snapshot_time"] = str(previous_batch["snapshot_time"])
        comparison["current_snapshot_time"] = str(latest_batch["snapshot_time"])

        new_rows = comparison[comparison["status"] == "new"].copy()
        if not new_rows.empty:
            new_rows["event_type"] = "NEW"
            events.extend(new_rows.to_dict("records"))

        inactive_rows = comparison[comparison["status"] == "inactive"].copy()
        if not inactive_rows.empty:
            inactive_rows["event_type"] = "INACTIVE"
            events.extend(inactive_rows.to_dict("records"))

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
            events.extend(update_rows.to_dict("records"))

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
    feed = feed.sort_values(["event_time", "event_type", "estimated_premium"], ascending=[False, True, False]).head(limit)
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
        "previous_snapshot_time",
        "current_snapshot_time",
    ]
    return feed[out_cols].where(pd.notna(feed[out_cols]), None).to_dict("records")


def _build_period_summary(change_feed: list[dict[str, Any]], days: int) -> list[dict[str, Any]]:
    if not change_feed:
        return []
    feed = pd.DataFrame(change_feed)
    feed["event_time"] = pd.to_datetime(feed["event_time"], errors="coerce")
    feed = feed[feed["event_time"].notna()].copy()
    if feed.empty:
        return []

    latest_time = feed["event_time"].max()
    window_start = latest_time.normalize() - pd.Timedelta(days=days - 1)
    window = feed[feed["event_time"] >= window_start].copy()
    if window.empty:
        return []

    summary = (
        window.groupby("ticker", dropna=False)
        .agg(
            total_new_count=("event_type", lambda s: int((s == "NEW").sum())),
            total_update_count=("event_type", lambda s: int((s == "UPDATE").sum())),
            total_inactive_count=("event_type", lambda s: int((s == "INACTIVE").sum())),
            cumulative_delta_volume=("delta_volume", "sum"),
            cumulative_delta_open_interest=("delta_open_interest", "sum"),
            cumulative_estimated_premium=("estimated_premium", "sum"),
            event_count=("event_type", "count"),
            last_event_time=("event_time", "max"),
        )
        .reset_index()
        .sort_values(
            ["event_count", "cumulative_delta_volume", "cumulative_delta_open_interest"],
            ascending=[False, False, False],
        )
    )
    return summary.where(pd.notna(summary), None).to_dict("records")


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
            "inactive_contract_count": 0,
            "disappeared_contract_count": 0,
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
        "inactive_contract_count": int((merged["status"] == "inactive").sum()),
        "disappeared_contract_count": int((merged["status"] == "inactive").sum()),
        "persistent_contract_count": int((merged["status"] == "continued").sum()),
        "ticker_rank": ticker_rank.fillna(0).to_dict("records"),
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


@app.get("/api/dashboard")
def dashboard_api(
    table: str = Query("stock", pattern="^(stock|etf)$"),
    pretty: int = Query(0, ge=0, le=1, description="1=formatted JSON output"),
) -> dict[str, Any]:
    raw_signature, current_signature = _dashboard_cache_signature(table)
    cache_key = f"{table}:{raw_signature}:{current_signature}"
    cached = DASHBOARD_CACHE.get(cache_key)
    if cached is not None:
        if pretty == 1:
            return Response(
                content=json.dumps(cached, ensure_ascii=False, indent=2),
                media_type="application/json; charset=utf-8",
            )
        return cached

    try:
        raw = load_table_read_only(DB_PATH, table)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"读取数据库失败: {exc}") from exc

    try:
        current_raw = load_table_read_only(DB_PATH, f"{table}_current_state")
    except Exception:
        current_raw = pd.DataFrame()

    latest_raw, previous_raw, latest_ts, previous_ts, comparison_mode = _latest_previous_frames_by_effective_change(raw)
    latest_df = _ensure_contract_signature(normalize_df(latest_raw))
    previous_df = _ensure_contract_signature(normalize_df(previous_raw))
    comparison_df = _build_contract_comparison(latest_raw, previous_raw)

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

    refreshed_df = df[df["is_refreshed"] == 1].copy()
    today_new_df = df[df["is_new"] == 1].copy()
    inactive_rows = _build_inactive_contract_rows(comparison_df)

    overall_section = _section_payload(df, "overall", table)
    refreshed_section = _section_payload(refreshed_df, "refreshed", table)
    today_section = _section_payload(today_new_df, "today_new", table)
    inactive_section = {
        "key": "inactive_helper",
        "summary": [],
        "bubble": [],
        "dte_profile": [],
        "strike_profile": [],
        "focus_blocks": [],
        "contracts": inactive_rows,
        "best_ticker": "-",
    }

    overall_summary = _build_summary(df)
    focus_overall = _focus_blocks(df, overall_summary, table)
    change_feed = _build_change_feed(raw)
    summary_payload = _snapshot_summary(df, inactive_count=len(inactive_rows))
    daily_summary = _build_period_summary(change_feed, days=1)
    three_day_summary = _build_period_summary(change_feed, days=3)
    strongest_focus = "-"
    if focus_overall:
        strongest_focus = max(
            focus_overall,
            key=lambda x: float(x.get("avg_ratio", 0)) * (1 + float(x.get("call_premium_pct", 0))),
        )["ticker"]

    cards = {
        "refreshed_ticker_count": int(refreshed_df["ticker"].nunique()) if not refreshed_df.empty else 0,
        "refreshed_contract_count": int(len(refreshed_df)),
        "refreshed_total_premium": _to_float(refreshed_df["estimated_premium"].sum(), 2) if not refreshed_df.empty else 0,
        "today_new_ticker_count": int(today_new_df["ticker"].nunique()) if not today_new_df.empty else 0,
        "today_new_contract_count": int(len(today_new_df)),
        "inactive_contract_count": int(len(inactive_rows)),
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
        "metadata": {
            "comparison_mode": comparison_mode,
            "latest_snapshot_time": str(latest_ts) if latest_ts is not None else None,
            "previous_snapshot_time": str(previous_ts) if previous_ts is not None else None,
            "data_source": table,
            "active_filter_summary": {
                "focus_tickers": FOCUS_TICKERS,
            },
        },
        "summary": summary_payload,
        "cards": cards,
        "metric_explanations": [
            "Call premium share high = more call-heavy flow",
            "Put premium share high = more defensive or bearish flow",
            "新增 = 相对于最近一次有效变化快照新增",
            "延续 = 相对于最近一次有效变化快照继续出现",
            "不再异常 = 相对于最近一次有效变化快照未继续被异常捕捉",
            "comparison_mode = effective_change_previous",
            "avg_ratio / max_ratio = 异常强度参考",
        ],
        "sections": {
            "refreshed": refreshed_section,
            "today_new": today_section,
            "overall": overall_section,
            "inactive_helper": inactive_section,
        },
        "change_feed": change_feed,
        "current_snapshot_rows": overall_section["contracts"],
        "continued_rows": refreshed_section["contracts"],
        "inactive_rows": inactive_rows,
        "daily_summary": daily_summary,
        "three_day_summary": three_day_summary,
        "refresh_delta": _build_refresh_delta(raw),
        "snapshot_meta": {
            "latest_snapshot_time": str(latest_ts) if latest_ts is not None else None,
            "previous_snapshot_time": str(previous_ts) if previous_ts is not None else None,
            "comparison_mode": comparison_mode,
        },
    }
    json_payload = jsonable_encoder(payload)
    DASHBOARD_CACHE.clear()
    DASHBOARD_CACHE[cache_key] = json_payload
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





