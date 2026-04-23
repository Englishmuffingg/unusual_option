from __future__ import annotations

import argparse
import json
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
    # 兼容从非项目根目录直接运行脚本（例如：cd stock 后 python dashboard_api.py）
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


def _flow_desc(call_pct: float, put_pct: float) -> str:
    if call_pct >= 0.65:
        return "偏多"
    if put_pct >= 0.65:
        return "偏防守"
    return "双向博弈"


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
                "flow_desc": r.get("flow_desc", "双向博弈"),
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


def _build_contract_comparison(latest_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    cur = _aggregate_snapshot_contracts(latest_df)
    prev = _aggregate_snapshot_contracts(previous_df)
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
    merged.loc[~merged["in_latest"] & merged["in_previous"], "status"] = "disappeared"
    return merged


def _latest_previous_frames(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, Any, Any]:
    if raw_df.empty:
        return raw_df.iloc[0:0].copy(), raw_df.iloc[0:0].copy(), None, None
    work = _ensure_contract_signature(raw_df)
    ts_col = "snapshot_at" if "snapshot_at" in work.columns else "recorded_at"
    if ts_col not in work.columns:
        return work.iloc[0:0].copy(), work.iloc[0:0].copy(), None, None
    work[ts_col] = pd.to_datetime(work[ts_col], errors="coerce")
    work = work[work[ts_col].notna()].copy()
    if work.empty:
        return work.iloc[0:0].copy(), work.iloc[0:0].copy(), None, None

    if "refresh_id" in work.columns and work["refresh_id"].fillna("").astype(str).str.strip().ne("").any():
        ref = (
            work[["refresh_id", ts_col]]
            .dropna()
            .sort_values(ts_col)
            .drop_duplicates(subset=["refresh_id"], keep="last")
        )
        ordered = ref.sort_values(ts_col, ascending=False).reset_index(drop=True)
        latest_id = ordered.iloc[0]["refresh_id"]
        latest_ts = ordered.iloc[0][ts_col]
        latest_df = work[work["refresh_id"] == latest_id].copy()
        prev_df = work.iloc[0:0].copy()
        prev_ts = None
        if len(ordered) >= 2:
            prev_id = ordered.iloc[1]["refresh_id"]
            prev_df = work[work["refresh_id"] == prev_id].copy()
            prev_ts = ordered.iloc[1][ts_col]
        return latest_df, prev_df, latest_ts, prev_ts

    snaps = sorted(work[ts_col].dropna().unique())
    latest = snaps[-1]
    previous = snaps[-2] if len(snaps) >= 2 else None
    latest_df = work[work[ts_col] == latest].copy()
    prev_df = work[work[ts_col] == previous].copy() if previous is not None else work.iloc[0:0].copy()
    return latest_df, prev_df, latest, previous


def _build_refresh_delta(raw_df: pd.DataFrame) -> dict[str, Any]:
    latest_df, prev_df, latest_ts, previous_ts = _latest_previous_frames(raw_df)
    if latest_df.empty:
        return {
            "snapshot_time": None,
            "previous_snapshot_time": None,
            "contract_count_delta": 0,
            "options_volume_delta": 0,
            "estimated_premium_delta": 0,
            "open_interest_delta": 0,
            "new_contract_count": 0,
            "disappeared_contract_count": 0,
            "persistent_contract_count": 0,
            "ticker_rank": [],
            "contract_changes": [],
        }

    cur = _aggregate_snapshot_contracts(latest_df)
    prev = _aggregate_snapshot_contracts(prev_df)
    merged = _build_contract_comparison(latest_df, prev_df)

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
        "contract_count_delta": int(len(cur) - len(prev)),
        "options_volume_delta": _to_float(cur["options_volume"].sum() - prev["options_volume"].sum(), 0),
        "estimated_premium_delta": _to_float(cur["estimated_premium"].sum() - prev["estimated_premium"].sum(), 2),
        "open_interest_delta": _to_float(cur["open_interest"].sum() - prev["open_interest"].sum(), 0),
        "new_contract_count": int((merged["status"] == "new").sum()),
        "disappeared_contract_count": int((merged["status"] == "disappeared").sum()),
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
        <h2>Unusual Options Dashboard API 已启动</h2>
        <p>这是 JSON API 服务，接口默认返回紧凑 JSON（单行显示是正常的）。</p>
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
    pretty: int = Query(0, ge=0, le=1, description="1=格式化输出 JSON，便于浏览器阅读"),
) -> dict[str, Any]:
    try:
        raw = load_table_read_only(DB_PATH, table)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"读取数据库失败: {exc}") from exc

    try:
        current_raw = load_table_read_only(DB_PATH, f"{table}_current_state")
    except Exception:
        current_raw = pd.DataFrame()

    latest_raw, previous_raw, latest_ts, previous_ts = _latest_previous_frames(raw)
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

    if not comparison_df.empty:
        previous_fields = comparison_df[
            [
                "contract_signature",
                "options_volume_prev",
                "open_interest_prev",
                "delta_volume",
                "delta_open_interest",
                "in_previous",
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

    refreshed_df = df[df["is_refreshed"] == 1].copy()
    today_new_df = df[df["is_new"] == 1].copy()

    overall_section = _section_payload(df, "overall", table)
    refreshed_section = _section_payload(refreshed_df, "refreshed", table)
    today_section = _section_payload(today_new_df, "today_new", table)

    overall_summary = _build_summary(df)
    focus_overall = _focus_blocks(df, overall_summary, table)
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
        "cards": cards,
        "metric_explanations": [
            "Call 权利金占比高 = 更偏多",
            "Put 权利金占比高 = 更偏防守",
            "is_new = latest 有、previous 没有（新增）",
            "is_refreshed = latest 与 previous 都有（延续）",
            "median_dte = 主流期限结构",
            "avg_ratio / max_ratio = 异常程度",
            "bullish_score = Call 权利金占比 - Put 权利金占比",
        ],
        "sections": {
            "refreshed": refreshed_section,
            "today_new": today_section,
            "overall": overall_section,
        },
        "refresh_delta": _build_refresh_delta(raw),
        "snapshot_meta": {
            "latest_snapshot_time": str(latest_ts) if latest_ts is not None else None,
            "previous_snapshot_time": str(previous_ts) if previous_ts is not None else None,
        },
    }
    json_payload = jsonable_encoder(payload)
    if pretty == 1:
        return Response(
            content=json.dumps(json_payload, ensure_ascii=False, indent=2),
            media_type="application/json; charset=utf-8",
        )
    return json_payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="启动异常期权 Dashboard API 服务")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=8000, help="监听端口，默认 8000")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="开发模式自动重载（需要从项目根目录启动）",
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



