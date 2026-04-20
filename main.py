"""
入口：依次执行 stock 与 ETF 全流程（拉取 → 入库 → 维护）。

依赖环境变量（全量 CSV 建议配置）：
  OPTIONCHARTS_COOKIE  浏览器复制的 Cookie
"""

from etf.pipeline import run_full as run_etf_full
from stock.pipeline import run_full as run_stock_full

if __name__ == "__main__":
    run_stock_full()
    run_etf_full()
