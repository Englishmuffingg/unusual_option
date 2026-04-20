import requests
import pandas as pd
from io import StringIO
import os

HTML_URL = "https://optioncharts.io/async/unusual_options_activity_stock_contracts"
SHOW_ALL_URL = "https://optioncharts.io/trending/unusual-options-activity-stock-contracts?limit=all"
CSV_URL = "https://optioncharts.io/async/unusual_options_activity_stock_contracts.csv?limit=all"

headers = {
    "User-Agent": "Mozilla/5.0",
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


def parse_html_table(html_text: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html_text))
    if not tables:
        raise RuntimeError("HTML 里没有找到表格")
    return tables[0]


def try_fetch_html_table(url: str) -> pd.DataFrame | None:
    resp = fetch(url)
    if resp.status_code != 200:
        return None
    try:
        return parse_html_table(resp.text)
    except Exception:
        return None


# 先尝试 show all（可能需要登录 cookie）
df_from_html = try_fetch_html_table(SHOW_ALL_URL)
if df_from_html is not None:
    print("Show All 表格行数:", len(df_from_html))
else:
    print("提示：Show All 页面不可用（可能需要 Cookie），回退到 async HTML（数据可能不全）。")
    df_from_html = parse_html_table(fetch(HTML_URL).text)
    print("Async HTML 表格行数:", len(df_from_html))

# 再尝试 csv
csv_resp = fetch(CSV_URL)

is_csv = (
    csv_resp.status_code == 200
    and (
        (csv_resp.headers.get("content-type") or "").lower().startswith("text/csv")
        or csv_resp.text.lstrip().startswith("Symbol,")
    )
)

if is_csv:
    df_csv = pd.read_csv(StringIO(csv_resp.text))
    print("CSV 行数:", len(df_csv))
    df = df_csv if len(df_csv) >= len(df_from_html) else df_from_html
else:
    print("CSV 接口不可用，使用 Show All HTML")
    df = df_from_html

df.to_csv("unusual_options_activity_stock_contracts_all.csv", index=False, encoding="utf-8-sig")
print("已保存 CSV")
print("最终行数:", len(df))
print(df.tail())