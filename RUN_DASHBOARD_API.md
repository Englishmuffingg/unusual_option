# Dashboard API 启动指南（Windows / macOS / Linux）

## 关键结论
- **不要**执行 `pip install stock`。这会去 PyPI 安装同名第三方包，不是本项目。  
- 必须在**项目根目录**启动，或使用根目录启动脚本。

## 1) 安装依赖
```bash
pip install -r requirements.txt
# 如果你之前误装过同名第三方包，建议先卸载，避免导入冲突
pip uninstall -y stock
```

## 2) 启动方式（只用这一条）
在项目根目录执行：
```bash
python run_dashboard_api.py --host 127.0.0.1 --port 8000 --reload
```

## 3) 访问地址
- 健康检查：`http://127.0.0.1:8000/api/health`
- Stock 数据：`http://127.0.0.1:8000/api/dashboard?table=stock`
- ETF 数据：`http://127.0.0.1:8000/api/dashboard?table=etf`
- 可读 JSON（带缩进）：`http://127.0.0.1:8000/api/dashboard?table=stock&pretty=1`
- API 首页（说明 + 快速链接）：`http://127.0.0.1:8000/`

## 4) 常见错误
### 报错：`ModuleNotFoundError: No module named 'stock'`
原因：你不是在项目根目录运行，或者 Python 没有把项目根目录加入模块搜索路径。

解决：
1. 先 `cd` 到项目根目录；
2. 运行 `python run_dashboard_api.py ...`。

### 报错：`GET / 404 Not Found`
原因：常见于模块导入冲突（例如安装了第三方 `stock` 包）或启动命令不是项目根目录入口脚本。  
解决：  
1. `pip uninstall -y stock`  
2. 回到项目根目录  
3. 只用 `python run_dashboard_api.py --host 127.0.0.1 --port 8000 --reload` 启动。

## 5) 说明
`dashboard_api.py` 是输出层（只读 SQLite + 聚合 + JSON 输出），不会修改原始表结构，也不会写回数据。
默认 JSON 是紧凑格式（单行）以减少传输体积，这不是错误；要人类可读格式请用 `pretty=1`。
