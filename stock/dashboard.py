from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# =========================
# 路径配置
# =========================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = PROJECT_DIR / "data"

DB_PATH = DATA_DIR / "stock_unusual_options.sqlite"
OUTPUT_HTML = BASE_DIR / "unusual_options_dashboard.html"

TOP_TICKERS = 20
TOP_STRIKES_PER_TICKER = 12
TOP_FOCUS_CONTRACTS = 12

DTE_BUCKETS = [0, 2, 7, 14, 30, 60, 120, 365, 10_000]
DTE_LABELS = ["0到2天", "3到7天", "8到14天", "15到30天", "31到60天", "61到120天", "121到365天", "365天以上"]


# =========================
# 只读读取数据库
# =========================
def load_table_read_only(db_path: Path, table_name: str) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    uri = f"file:{db_path.as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)


# =========================
# 数据预处理
# =========================
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    numeric_defaults = {
        "options_volume": 0,
        "open_interest": 0,
        "bid_price": 0,
        "ask_price": 0,
        "mid_price": 0,
        "volume_to_open_interest_ratio": 0,
        "strike": 0,
        "dte": 0,
        "is_new": 0,
        "is_refreshed": 0,
        "inactive": 0,
        "previous_options_volume": 0,
        "previous_open_interest": 0,
        "delta_volume": 0,
        "delta_open_interest": 0,
    }
    for col, default in numeric_defaults.items():
        if col not in out.columns:
            out[col] = default
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(default)

    for col in ["recorded_at", "expiration_date"]:
        if col not in out.columns:
            out[col] = pd.NaT
        out[col] = pd.to_datetime(out[col], errors="coerce")

    if "ticker" not in out.columns:
        out["ticker"] = ""
    out["ticker"] = out["ticker"].fillna("").astype(str).str.upper().str.strip()

    if "option_type" not in out.columns:
        out["option_type"] = ""
    out["option_type"] = out["option_type"].fillna("").astype(str).str.upper().str.strip()

    out["is_new"] = out["is_new"].astype(int)
    out["is_refreshed"] = out["is_refreshed"].astype(int)
    out["inactive"] = out["inactive"].astype(int)
    out["is_call"] = (out["option_type"] == "CALL").astype(int)
    out["is_put"] = (out["option_type"] == "PUT").astype(int)

    out["estimated_premium"] = out["options_volume"].fillna(0) * out["mid_price"].fillna(0) * 100

    out["dte_bucket"] = pd.cut(
        out["dte"],
        bins=DTE_BUCKETS,
        labels=DTE_LABELS,
        include_lowest=True,
        right=True,
    )

    return out


# =========================
# 聚合分析
# =========================
def summarize_by_ticker(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "ticker", "rows", "contracts", "total_volume", "total_open_interest",
                "total_est_premium", "avg_dte", "median_dte", "avg_ratio", "max_ratio",
                "call_rows", "put_rows", "new_rows",
                "call_volume", "put_volume", "call_premium", "put_premium",
                "call_row_pct", "put_row_pct", "new_row_pct",
                "call_volume_pct", "put_volume_pct", "call_premium_pct", "put_premium_pct"
            ]
        )

    base = (
        df.groupby("ticker", dropna=False)
        .agg(
            rows=("contract_symbol", "count"),
            contracts=("contract_symbol", "nunique"),
            total_volume=("options_volume", "sum"),
            total_open_interest=("open_interest", "sum"),
            total_est_premium=("estimated_premium", "sum"),
            avg_dte=("dte", "mean"),
            median_dte=("dte", "median"),
            avg_ratio=("volume_to_open_interest_ratio", "mean"),
            max_ratio=("volume_to_open_interest_ratio", "max"),
            call_rows=("is_call", "sum"),
            put_rows=("is_put", "sum"),
            new_rows=("is_new", "sum"),
        )
        .reset_index()
    )

    vol_split = (
        df.pivot_table(
            index="ticker",
            columns="option_type",
            values="options_volume",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for col in ["CALL", "PUT"]:
        if col not in vol_split.columns:
            vol_split[col] = 0
    vol_split = vol_split.rename(columns={"CALL": "call_volume", "PUT": "put_volume"})

    prem_split = (
        df.pivot_table(
            index="ticker",
            columns="option_type",
            values="estimated_premium",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for col in ["CALL", "PUT"]:
        if col not in prem_split.columns:
            prem_split[col] = 0
    prem_split = prem_split.rename(columns={"CALL": "call_premium", "PUT": "put_premium"})

    summary = base.merge(vol_split, on="ticker", how="left").merge(prem_split, on="ticker", how="left")

    summary["call_row_pct"] = summary["call_rows"] / summary["rows"].replace(0, pd.NA)
    summary["put_row_pct"] = summary["put_rows"] / summary["rows"].replace(0, pd.NA)
    summary["new_row_pct"] = summary["new_rows"] / summary["rows"].replace(0, pd.NA)

    summary["cp_total_volume"] = summary["call_volume"] + summary["put_volume"]
    summary["call_volume_pct"] = summary["call_volume"] / summary["cp_total_volume"].replace(0, pd.NA)
    summary["put_volume_pct"] = summary["put_volume"] / summary["cp_total_volume"].replace(0, pd.NA)

    summary["cp_total_premium"] = summary["call_premium"] + summary["put_premium"]
    summary["call_premium_pct"] = summary["call_premium"] / summary["cp_total_premium"].replace(0, pd.NA)
    summary["put_premium_pct"] = summary["put_premium"] / summary["cp_total_premium"].replace(0, pd.NA)

    return summary.sort_values(
        ["total_est_premium", "total_volume"],
        ascending=[False, False]
    ).reset_index(drop=True)


def dte_profile(df: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    if df.empty or not tickers:
        return pd.DataFrame(columns=["ticker", "dte_bucket", "total_est_premium", "total_volume"])

    return (
        df[df["ticker"].isin(tickers)]
        .groupby(["ticker", "dte_bucket"], dropna=False)
        .agg(
            total_est_premium=("estimated_premium", "sum"),
            total_volume=("options_volume", "sum"),
        )
        .reset_index()
    )


def strike_profile(df: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    if df.empty or not tickers:
        return pd.DataFrame(columns=["ticker", "option_type", "strike", "total_est_premium", "total_volume", "rows", "avg_dte", "max_ratio"])

    prof = (
        df[df["ticker"].isin(tickers)]
        .groupby(["ticker", "option_type", "strike"], dropna=False)
        .agg(
            total_est_premium=("estimated_premium", "sum"),
            total_volume=("options_volume", "sum"),
            rows=("contract_symbol", "count"),
            avg_dte=("dte", "mean"),
            max_ratio=("volume_to_open_interest_ratio", "max"),
        )
        .reset_index()
    )

    prof["rank_within_ticker"] = prof.groupby("ticker")["total_est_premium"].rank(method="first", ascending=False)
    return prof[prof["rank_within_ticker"] <= TOP_STRIKES_PER_TICKER].copy()


def top_contracts(df: pd.DataFrame, tickers: List[str], top_n: int = 12) -> pd.DataFrame:
    if df.empty or not tickers:
        return pd.DataFrame()

    cols = [
        "ticker",
        "contract_display_name",
        "option_type",
        "strike",
        "expiration_date",
        "dte",
        "is_new",
        "options_volume",
        "open_interest",
        "volume_to_open_interest_ratio",
        "mid_price",
        "estimated_premium",
    ]
    cols = [c for c in cols if c in df.columns]

    return (
        df[df["ticker"].isin(tickers)]
        .sort_values(
            ["estimated_premium", "options_volume", "volume_to_open_interest_ratio"],
            ascending=[False, False, False],
        )[cols]
        .head(top_n)
    )


def focused_contracts(df: pd.DataFrame, ticker: str, new_only: bool = False, top_n: int = 12) -> pd.DataFrame:
    sub = df[df["ticker"] == ticker].copy()
    if new_only:
        sub = sub[sub["is_new"] == 1]

    if sub.empty:
        return pd.DataFrame()

    cols = [
        "contract_display_name",
        "option_type",
        "strike",
        "expiration_date",
        "dte",
        "is_new",
        "options_volume",
        "open_interest",
        "volume_to_open_interest_ratio",
        "mid_price",
        "estimated_premium",
    ]
    cols = [c for c in cols if c in sub.columns]

    return sub.sort_values(
        ["estimated_premium", "options_volume", "volume_to_open_interest_ratio"],
        ascending=[False, False, False],
    )[cols].head(top_n)


def get_focus_tickers(df: pd.DataFrame, dataset_name: str) -> List[str]:
    all_tickers = set(df["ticker"].dropna().unique())

    if dataset_name == "etf":
        wanted = ["SPY", "GLD", "QQQ"]
    elif dataset_name == "stock":
        wanted = ["MSFT"]
    else:
        wanted = ["SPY", "GLD", "QQQ", "MSFT"]

    return [t for t in wanted if t in all_tickers]


# =========================
# 中文分析文案
# =========================
def narrative_for_ticker(summary_row: pd.Series, ticker_df: pd.DataFrame, ticker: str) -> str:
    call_prem = summary_row.get("call_premium_pct", 0)
    put_prem = summary_row.get("put_premium_pct", 0)
    new_ratio = summary_row.get("new_row_pct", 0)
    avg_dte = summary_row.get("avg_dte", 0)
    max_ratio = summary_row.get("max_ratio", 0)

    if pd.notna(call_prem) and call_prem >= 0.65:
        cp_text = "Call 权利金占明显主导，因此整体资金倾向偏多。"
    elif pd.notna(put_prem) and put_prem >= 0.65:
        cp_text = "Put 权利金占明显主导，因此整体资金倾向偏防守或偏空。"
    else:
        cp_text = "Call 和 Put 权利金相对均衡，因此当前资金更像双向博弈，而不是单边押注。"

    if pd.notna(new_ratio) and new_ratio >= 0.5:
        new_text = "新增记录占比较高，说明当前更像是新资金在建仓，而不只是旧合约继续活跃。"
    elif pd.notna(new_ratio) and new_ratio >= 0.2:
        new_text = "新增资金有一定占比，但原有活跃合约仍然占据重要位置。"
    else:
        new_text = "当前大部分活跃度仍来自原有合约集合，而不只是新出现的异常合约。"

    if pd.notna(avg_dte) and avg_dte <= 7:
        dte_text = "平均 DTE 很短，说明这批资金更集中在近端交易、事件驱动或短线博弈。"
    elif pd.notna(avg_dte) and avg_dte <= 45:
        dte_text = "平均 DTE 落在短中期区间，更像战术性布局，而不是单纯的日内噪音。"
    else:
        dte_text = "平均 DTE 相对较长，因此这批活动更像中期布局、结构仓位或持续性观点表达。"

    if pd.notna(max_ratio) and max_ratio >= 50:
        ratio_text = "最大 Volume/Open Interest 比例非常高，说明至少有一部分合约出现了极不寻常的换手或明显的新活动。"
    elif pd.notna(max_ratio) and max_ratio >= 10:
        ratio_text = "这批合约中存在明显异常的成交，Volume 明显高于正常 Open Interest 水平。"
    else:
        ratio_text = "整体 Volume/Open Interest 结构较活跃，但还没有被极端异常值完全主导。"

    strike_tbl = (
        ticker_df.groupby(["option_type", "strike"], dropna=False)
        .agg(total_est_premium=("estimated_premium", "sum"))
        .reset_index()
        .sort_values("total_est_premium", ascending=False)
        .head(3)
    )

    if not strike_tbl.empty:
        strike_text = "主要 strike 聚集在：" + "，".join(
            f"{row.option_type} {row.strike:g}" for _, row in strike_tbl.iterrows()
        ) + "。"
    else:
        strike_text = "暂时没有特别突出的 strike 聚集区域。"

    return (
        f"<p><strong>{ticker}</strong>："
        f"{cp_text}{new_text}{dte_text}{ratio_text}{strike_text}</p>"
    )


def build_overall_narrative(summary: pd.DataFrame, df: pd.DataFrame, max_tickers: int = 8) -> str:
    if summary.empty:
        return "<p>暂无整体分析内容。</p>"

    blocks = []
    for ticker in summary["ticker"].head(max_tickers):
        row = summary[summary["ticker"] == ticker]
        if row.empty:
            continue
        blocks.append(narrative_for_ticker(row.iloc[0], df[df["ticker"] == ticker], ticker))

    return "".join(blocks) if blocks else "<p>暂无整体分析内容。</p>"


# =========================
# HTML 辅助
# =========================
def fig_to_html(fig: go.Figure, include_js: bool = False) -> str:
    return fig.to_html(full_html=False, include_plotlyjs="cdn" if include_js else False)


def dataframe_to_html(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "<p>暂无数据。</p>"

    show = df.head(max_rows).copy()
    for col in show.columns:
        if pd.api.types.is_float_dtype(show[col]):
            show[col] = show[col].round(4)

    return show.to_html(index=False, classes="table table-striped", border=0)


# =========================
# 单个数据集页面块
# =========================
def build_dataset_section(df: pd.DataFrame, name: str, include_js: bool = False) -> str:
    summary = summarize_by_ticker(df)
    top = summary.head(TOP_TICKERS).copy()
    top_tickers = top["ticker"].tolist()
    focus_tickers = get_focus_tickers(df, name)

    new_df = df[df["is_new"] == 1].copy()
    new_summary = summarize_by_ticker(new_df)
    new_top_tickers = new_summary.head(TOP_TICKERS)["ticker"].tolist() if not new_summary.empty else []

    # ===== 整体图表 =====
    if not top.empty:
        fig1 = px.bar(
            top,
            x="ticker",
            y="total_est_premium",
            color="call_premium_pct",
            hover_data=["total_volume", "contracts", "new_row_pct", "avg_dte"],
            title=f"{name.upper()}：整体部分主要标的（按估算权利金）",
            labels={"total_est_premium": "估算权利金", "call_premium_pct": "Call 权利金占比"},
        )

        fig2 = go.Figure()
        fig2.add_bar(name="Call 成交量", x=top["ticker"], y=top["call_volume"])
        fig2.add_bar(name="Put 成交量", x=top["ticker"], y=top["put_volume"])
        fig2.update_layout(
            barmode="stack",
            title=f"{name.upper()}：整体部分 Call / Put 成交量对比",
            xaxis_title="标的",
            yaxis_title="期权成交量",
        )
    else:
        fig1 = go.Figure().update_layout(title=f"{name.upper()}：暂无整体数据")
        fig2 = go.Figure().update_layout(title=f"{name.upper()}：暂无整体数据")

    dte_df = dte_profile(df, top_tickers[:15])
    if not dte_df.empty:
        dte_pivot = dte_df.pivot_table(index="ticker", columns="dte_bucket", values="total_est_premium", aggfunc="sum", fill_value=0)
        ordered_cols = [c for c in DTE_LABELS if c in dte_pivot.columns]
        dte_pivot = dte_pivot[ordered_cols]

        fig3 = px.imshow(
            dte_pivot,
            aspect="auto",
            title=f"{name.upper()}：整体部分期限结构",
            labels={"x": "期限区间", "y": "标的", "color": "估算权利金"},
        )
    else:
        fig3 = go.Figure().update_layout(title=f"{name.upper()}：暂无期限结构数据")

    strike_df = strike_profile(df, top_tickers[:12])
    if not strike_df.empty:
        fig4 = px.scatter(
            strike_df,
            x="strike",
            y="ticker",
            size="total_est_premium",
            color="option_type",
            hover_data=["total_volume", "rows", "avg_dte", "max_ratio"],
            title=f"{name.upper()}：整体部分主要 strike 聚集",
            labels={"strike": "Strike", "ticker": "标的"},
        )
    else:
        fig4 = go.Figure().update_layout(title=f"{name.upper()}：暂无 strike 聚集数据")

    # ===== 新增图表 =====
    if not new_summary.empty:
        new_top = new_summary.head(TOP_TICKERS)

        fig5 = px.bar(
            new_top,
            x="ticker",
            y="total_est_premium",
            color="call_premium_pct",
            hover_data=["total_volume", "contracts", "avg_dte"],
            title=f"{name.upper()}：新增部分主要标的（按估算权利金）",
            labels={"total_est_premium": "估算权利金", "call_premium_pct": "Call 权利金占比"},
        )

        fig6 = go.Figure()
        fig6.add_bar(name="Call 成交量", x=new_top["ticker"], y=new_top["call_volume"])
        fig6.add_bar(name="Put 成交量", x=new_top["ticker"], y=new_top["put_volume"])
        fig6.update_layout(
            barmode="stack",
            title=f"{name.upper()}：新增部分 Call / Put 成交量对比",
            xaxis_title="标的",
            yaxis_title="期权成交量",
        )

        new_dte_df = dte_profile(new_df, new_top_tickers[:15])
        if not new_dte_df.empty:
            new_dte_pivot = new_dte_df.pivot_table(index="ticker", columns="dte_bucket", values="total_est_premium", aggfunc="sum", fill_value=0)
            new_ordered_cols = [c for c in DTE_LABELS if c in new_dte_pivot.columns]
            new_dte_pivot = new_dte_pivot[new_ordered_cols]

            fig7 = px.imshow(
                new_dte_pivot,
                aspect="auto",
                title=f"{name.upper()}：新增部分期限结构",
                labels={"x": "期限区间", "y": "标的", "color": "估算权利金"},
            )
        else:
            fig7 = go.Figure().update_layout(title=f"{name.upper()}：暂无新增期限结构数据")

        new_strike_df = strike_profile(new_df, new_top_tickers[:12])
        if not new_strike_df.empty:
            fig8 = px.scatter(
                new_strike_df,
                x="strike",
                y="ticker",
                size="total_est_premium",
                color="option_type",
                hover_data=["total_volume", "rows", "avg_dte", "max_ratio"],
                title=f"{name.upper()}：新增部分主要 strike 聚集",
                labels={"strike": "Strike", "ticker": "标的"},
            )
        else:
            fig8 = go.Figure().update_layout(title=f"{name.upper()}：暂无新增 strike 聚集数据")
    else:
        fig5 = go.Figure().update_layout(title=f"{name.upper()}：暂无新增数据")
        fig6 = go.Figure().update_layout(title=f"{name.upper()}：暂无新增数据")
        fig7 = go.Figure().update_layout(title=f"{name.upper()}：暂无新增期限结构数据")
        fig8 = go.Figure().update_layout(title=f"{name.upper()}：暂无新增 strike 聚集数据")

    # ===== 中文分析摘要 =====
    overall_narrative = build_overall_narrative(summary, df, max_tickers=8)

    focus_overall_blocks = []
    for ticker in focus_tickers:
        row = summary[summary["ticker"] == ticker]
        if row.empty:
            continue

        focus_overall_blocks.append(
            "<div class='card'>"
            f"<h3>整体重点关注 • {ticker}</h3>"
            f"{narrative_for_ticker(row.iloc[0], df[df['ticker'] == ticker], ticker)}"
            f"{dataframe_to_html(focused_contracts(df, ticker, new_only=False, top_n=TOP_FOCUS_CONTRACTS), max_rows=TOP_FOCUS_CONTRACTS)}"
            "</div>"
        )

    focus_new_blocks = []
    if not new_summary.empty:
        for ticker in focus_tickers:
            row = new_summary[new_summary["ticker"] == ticker]
            sub = new_df[new_df["ticker"] == ticker]
            if row.empty or sub.empty:
                continue

            focus_new_blocks.append(
                "<div class='card'>"
                f"<h3>新增重点关注 • {ticker}</h3>"
                f"{narrative_for_ticker(row.iloc[0], sub, ticker)}"
                f"{dataframe_to_html(focused_contracts(df, ticker, new_only=True, top_n=TOP_FOCUS_CONTRACTS), max_rows=TOP_FOCUS_CONTRACTS)}"
                "</div>"
            )

    new_other_table_html = "<p>暂无新增记录。</p>"
    if not new_summary.empty:
        cols = [
            "ticker",
            "rows",
            "contracts",
            "total_volume",
            "total_est_premium",
            "call_volume_pct",
            "put_volume_pct",
            "call_premium_pct",
            "put_premium_pct",
            "avg_dte",
            "max_ratio",
        ]
        cols = [c for c in cols if c in new_summary.columns]
        new_other_table_html = dataframe_to_html(new_summary[cols], max_rows=TOP_TICKERS)

    return f"""
    <section class='dataset-section'>
      <h2>{name.upper()} 仪表板</h2>
      <p class='subtext'>只读分析。本仪表板不会修改你原始的 <code>{name}</code> 表。</p>

      <div class='section-title'>第一部分 • 整体表分析</div>
      <div class='grid grid-2'>
        <div class='card'>{fig_to_html(fig1, include_js=include_js)}</div>
        <div class='card'>{fig_to_html(fig2)}</div>
      </div>
      <div class='grid grid-2'>
        <div class='card'>{fig_to_html(fig3)}</div>
        <div class='card'>{fig_to_html(fig4)}</div>
      </div>
      <div class='grid grid-1'>
        <div class='card'>
          <h3>整体分析摘要</h3>
          {overall_narrative}
        </div>
      </div>
      <div class='grid grid-2'>
        {''.join(focus_overall_blocks) if focus_overall_blocks else "<div class='card'><p>当前数据集中未找到重点关注标的。</p></div>"}
      </div>

      <div class='section-title'>第二部分 • 新增部分分析</div>
      <div class='grid grid-2'>
        <div class='card'>{fig_to_html(fig5)}</div>
        <div class='card'>{fig_to_html(fig6)}</div>
      </div>
      <div class='grid grid-2'>
        <div class='card'>{fig_to_html(fig7)}</div>
        <div class='card'>{fig_to_html(fig8)}</div>
      </div>
      <div class='grid grid-2'>
        {''.join(focus_new_blocks) if focus_new_blocks else "<div class='card'><p>新增部分中未找到重点关注标的。</p></div>"}
        <div class='card'>
          <h3>其他新增标的</h3>
          {new_other_table_html}
        </div>
      </div>
    </section>
    """


def build_full_html(stock_df: pd.DataFrame, etf_df: pd.DataFrame) -> str:
    stock_section = build_dataset_section(stock_df, "stock", include_js=True)
    etf_section = build_dataset_section(etf_df, "etf", include_js=False)

    return f"""
<!doctype html>
<html lang='zh'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>异常期权分析仪表板</title>
  <style>
    body {{
      margin: 0;
      font-family: Arial, sans-serif;
      background: #0f1115;
      color: #e8ecf3;
    }}
    .container {{
      max-width: 1700px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1, h2, h3 {{ margin-top: 0; }}
    .subtext {{ color: #aab4c3; }}
    .section-title {{
      font-size: 20px;
      font-weight: bold;
      margin: 28px 0 14px 0;
      padding-bottom: 8px;
      border-bottom: 1px solid #2a3140;
    }}
    .grid {{
      display: grid;
      gap: 16px;
      margin-bottom: 16px;
    }}
    .grid-1 {{ grid-template-columns: 1fr; }}
    .grid-2 {{ grid-template-columns: 1fr 1fr; }}
    .card {{
      background: #171a21;
      border: 1px solid #2a3140;
      border-radius: 14px;
      padding: 16px;
      overflow: auto;
    }}
    .dataset-section {{ margin-bottom: 40px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    th, td {{
      padding: 8px;
      border-bottom: 1px solid #283041;
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{
      position: sticky;
      top: 0;
      background: #171a21;
      z-index: 1;
    }}
    code {{
      background: #232938;
      padding: 2px 6px;
      border-radius: 6px;
    }}
    @media (max-width: 1100px) {{
      .grid-2 {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class='container'>
    <h1>异常期权分析仪表板</h1>
    <p class='subtext'>输出为单个 HTML 页面，方便刷新和查看。脚本仅以只读模式读取你的 SQLite 数据库，不会修改原始数据。</p>
    <p class='subtext'><strong>数据库路径：</strong><code>{DB_PATH}</code></p>
    {stock_section}
    {etf_section}
  </div>
</body>
</html>
    """


# =========================
# 主函数
# =========================
def main() -> None:
    print("DB_PATH:", DB_PATH)
    print("DB exists:", DB_PATH.exists())

    stock_df = normalize_df(load_table_read_only(DB_PATH, "stock"))
    etf_df = normalize_df(load_table_read_only(DB_PATH, "etf"))

    html = build_full_html(stock_df, etf_df)
    OUTPUT_HTML.write_text(html, encoding="utf-8")

    print(f"仪表板已生成：{OUTPUT_HTML.resolve()}")



if __name__ == "__main__":
    main()
