from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "stock_unusual_options.sqlite"

# 与业务域一致：stock 合约异动主表
TABLE_NAME = "stock"

CSV_URL = "https://optioncharts.io/async/unusual_options_activity_stock_contracts.csv?limit=all"
HTML_URL = "https://optioncharts.io/async/unusual_options_activity_stock_contracts"
SHOW_ALL_URL = (
    "https://optioncharts.io/trending/unusual-options-activity-stock-contracts?limit=all"
)

RETAIN_BUSINESS_DAYS = 3
