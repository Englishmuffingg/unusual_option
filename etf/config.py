"""与 stock 共用同一 SQLite 文件，独立表 `etf`。"""

from stock.config import DATA_DIR, DB_PATH, RETAIN_BUSINESS_DAYS

TABLE_NAME = "etf"

CSV_URL = "https://optioncharts.io/async/unusual_options_activity_etf_contracts.csv?limit=all"
HTML_URL = "https://optioncharts.io/async/unusual_options_activity_etf_contracts"
SHOW_ALL_URL = (
    "https://optioncharts.io/trending/unusual-options-activity-etf-contracts?limit=all"
)
