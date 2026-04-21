# React 仪表板重构说明（第一版已落地）

## 第一步：现有 Python HTML Dashboard 的主要问题

1. 信息架构上“整体 + 新增”模块重复，阅读路径长，优先级不突出。  
2. `is_new` 与“新增”语义容易被误读，缺乏明确区分“本次刷新新增（is_refreshed）”与“今日新增（is_new）”。  
3. 大段 narrative 文本占空间，盘中扫描效率不高。  
4. 表格列过宽，横向阅读负担大，缺少快速筛选/展开细节。  
5. Focus 标的（SPY/GLD/QQQ/MSFT）没有稳定高可见区域。

## 第二步：新的页面结构草图与组件拆分

- 顶部总览区
  - `SummaryCards`
  - `MetricGuide`
- 第一主区（本次刷新新增）
  - `SectionPanel` + `SummaryTable` + `SectionCharts` + `FocusTickerBlocks`
- 第二主区（今日新增）
  - 同上
- 第三主区（整体异常池）
  - 同上

前后端分层：
- Python/FastAPI：只读 SQLite + 聚合服务（复用 `normalize_df/summarize_by_ticker/dte_profile/strike_profile/focused_contracts/get_focus_tickers`）
- React：仪表板 UI、交互（排序、筛选、行展开、tab 切换）

## 第三步：第一版已实现的模块

- 顶部总览区 + 指标解释区（中文）
- 本次刷新新增区（高优先）
- 今日新增区
- 整体异常池区
- Focus ticker 区（SPY/GLD/QQQ/MSFT）
- 可排序表格（TanStack Table）
- 主图（Bubble 散点）+ 方向图（Diverging Bar）

## 第四步：新增 API 层

- `stock/dashboard_api.py`
- 只读加载 SQLite（复用 `load_table_read_only` 的 `mode=ro`）
- 输出前端直接可消费 JSON

## 第五步：运行方式

1. 后端（API）
   ```bash
   pip install -r requirements.txt
   pip install fastapi uvicorn
   uvicorn stock.dashboard_api:app --reload --host 127.0.0.1 --port 8000
   ```
2. 前端（React）
   ```bash
   cd web/dashboard-react
   npm install
   npm run dev
   ```
3. 打开 `http://127.0.0.1:5173`

