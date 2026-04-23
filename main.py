from etf.pipeline import run_full as run_etf_full
from stock.pipeline import run_full as run_stock_full
from stock.dashboard_api import dashboard


def main():
    stock_ok = False
    etf_ok = False

    # 1) 先跑 stock
    try:
        print("[main] 开始执行 stock 流程...")
        run_stock_full()
        stock_ok = True
        print("[main] stock 流程执行完成")
    except Exception as e:
        print(f"[main] stock 流程执行失败: {e}")

    # 2) 再跑 etf
    try:
        print("[main] 开始执行 etf 流程...")
        run_etf_full()
        etf_ok = True
        print("[main] etf 流程执行完成")
    except Exception as e:
        print(f"[main] etf 流程执行失败: {e}")

    # 3) 条件判断：两个都成功才启动 dashboard
    if stock_ok and etf_ok:
        print("[main] stock 和 etf 都成功，启动 dashboard...")
        try:
            dashboard()
        except Exception as e:
            print(f"[main] dashboard 启动失败: {e}")
    else:
        print("[main] 由于 stock 或 etf 未全部成功，跳过 dashboard 启动")


if __name__ == "__main__":
    main()