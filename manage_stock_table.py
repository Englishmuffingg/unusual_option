"""
仅维护本地 stock 表：删除超过最近 N 个工作日窗口的数据，并清除非今日的 NEW 标记。

不拉取远程数据，适合定时任务单独调用。
"""

from stock import config
from stock.maintenance import run_maintenance

if __name__ == "__main__":
    run_maintenance(table=config.TABLE_NAME)
