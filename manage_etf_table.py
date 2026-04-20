"""
仅维护本地 etf 表：删除超过最近 N 个工作日窗口的数据，并清除非今日的 NEW 标记。
"""

from etf import config as etf_config
from stock.maintenance import run_maintenance

if __name__ == "__main__":
    run_maintenance(table=etf_config.TABLE_NAME)
