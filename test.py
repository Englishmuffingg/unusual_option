import requests
import pandas as pd
from io import StringIO
import os
from io import BytesIO

HTML_URL = "https://optioncharts.io/async/unusual_options_activity_stock_contracts"
SHOW_ALL_URL = "https://optioncharts.io/trending/unusual-options-activity-stock-contracts?limit=all"
CSV_URL = "https://optioncharts.io/async/unusual_options_activity_stock_contracts.csv?limit=all"
OUT_CSV = "unusual_options_activity_stock_contracts_all.csv"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://optioncharts.io/",
}

session = requests.Session()
session.headers.update(headers)

cookie_header = os.getenv("OPTIONCHARTS_COOKIE", "").strip()
if cookie_header:
    # 推荐做法：不要把 cookie 写死在代码里，用环境变量注入
    # PowerShell 示例：
    #   $env:OPTIONCHARTS_COOKIE="name=value; name2=value2"
    #   python .\test.py
    session.headers["Cookie"] = cookie_header

def fetch(url: str):
    r = session.get(url, timeout=60, allow_redirects=True)
    print("\nGET:", url)
    print("status:", r.status_code, "bytes:", len(r.content), "content-type:", r.headers.get("content-type"))
    return r


def pick_best_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    if not tables:
        raise RuntimeError("HTML 里没有找到表格")

    required = {"Symbol", "Contract", "Volume/Open Interest", "Option Volume", "Open Interest"}

    def score(df: pd.DataFrame) -> tuple[int, int]:
        cols = set(str(c).strip() for c in df.columns)
        return (len(required & cols), len(df))

    return max(tables, key=score)


def parse_html_table(html_text: str) -> pd.DataFrame:
    # 强制用 lxml，避免依赖 beautifulsoup4
    tables = pd.read_html(StringIO(html_text), flavor="lxml")
    return pick_best_table(tables)


def try_fetch_html_table(url: str) -> pd.DataFrame | None:
    resp = fetch(url)
    if resp.status_code != 200:
        return None
    try:
        return parse_html_table(resp.text)
    except Exception as e:
        print("解析 HTML 表格失败：", type(e).__name__, str(e)[:200])
        return None


def try_fetch_csv(url: str) -> pd.DataFrame | None:
    resp = fetch(url)
    if resp.status_code != 200:
        return None

    content_type = (resp.headers.get("content-type") or "").lower()
    disposition = (resp.headers.get("content-disposition") or "").lower()
    looks_like_csv_header = resp.content.lstrip().startswith(b"Symbol,")

    # 站点可能用 application/octet-stream 返回 csv 下载
    is_csv = (
        content_type.startswith("text/csv")
        or "text/csv" in content_type
        or "octet-stream" in content_type
        or ".csv" in disposition
        or looks_like_csv_header
    )
    if not is_csv:
        return None

    # 用二进制读，避免 resp.text 解码导致的误判/乱码
    return pd.read_csv(BytesIO(resp.content), encoding="utf-8-sig")


def main():
    # 1) 优先拿全量 CSV（最完整）
    df_csv = try_fetch_csv(CSV_URL)
    if df_csv is not None:
        print("CSV 行数:", len(df_csv), "列数:", len(df_csv.columns))
        df = df_csv
    else:
        # 2) 其次尝试 show all 页（可能需要 Cookie，或页面结构变化导致解析失败）
        df_html = try_fetch_html_table(SHOW_ALL_URL)
        if df_html is not None:
            print("Show All 表格行数:", len(df_html), "列数:", len(df_html.columns))
            df = df_html
        else:
            # 3) 最后回退到 async HTML（通常只有前 N 条）
            print("提示：回退到 async HTML（数据可能不全）。")
            df = parse_html_table(fetch(HTML_URL).text)
            print("Async HTML 表格行数:", len(df), "列数:", len(df.columns))

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("已保存 CSV:", OUT_CSV)
    print("最终行数:", len(df))
    print("最后 5 行预览:")
    print(df.tail())


if __name__ == "__main__":
    main()