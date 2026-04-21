import sqlite3
import pandas as pd
from pathlib import Path

db_path = Path(r"D:\stock_radar\unusual_option\data\stock_unusual_options.sqlite")

with sqlite3.connect(db_path) as conn:
    df = pd.read_sql_query("SELECT * FROM stock", conn)

df.to_csv(r"D:\stock_radar\unusual_option\data\stock_export.csv", index=False, encoding="utf-8-sig")
print("stock 表已导出")