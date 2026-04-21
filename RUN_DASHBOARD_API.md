# Dashboard API 启动指南（Windows / macOS / Linux）

## 关键结论
- **不要**执行 `pip install stock`。这会去 PyPI 安装同名第三方包，不是本项目。  
- 必须在**项目根目录**启动，或使用根目录启动脚本。

## 1) 安装依赖
```bash
pip install -r requirements.txt
```

## 2) 启动方式（推荐）
在项目根目录执行：
```bash
python run_dashboard_api.py --host 127.0.0.1 --port 8000 --reload
```

## 3) 访问地址
- 健康检查：`http://127.0.0.1:8000/api/health`
- Stock 数据：`http://127.0.0.1:8000/api/dashboard?table=stock`
- ETF 数据：`http://127.0.0.1:8000/api/dashboard?table=etf`

## 4) 常见错误
### 报错：`ModuleNotFoundError: No module named 'stock'`
原因：你不是在项目根目录运行，或者 Python 没有把项目根目录加入模块搜索路径。

解决：
1. 先 `cd` 到项目根目录；
2. 运行 `python run_dashboard_api.py ...`。

## 5) 说明
`dashboard_api.py` 是输出层（只读 SQLite + 聚合 + JSON 输出），不会修改原始表结构，也不会写回数据。
