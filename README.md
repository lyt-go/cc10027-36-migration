# 日志采集与事件追踪平台

面向应用日志收集、事件埋点追踪、流式采集与结构化归集的多源日志采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 日志源管理 | `log_source` | 日志源 | 源编号、源名称、日志类型 |
| 日志解析 | `log_parser` | 解析规则 | 规则编号、关联源、正则表达式 |
| 事件埋点 | `event_tracker` | 埋点事件 | 事件编号、事件名称、埋点页面 |
| 流式缓冲 | `stream_buffer` | 缓冲配置 | 配置编号、关联源、缓冲区大小 |
| 字段映射 | `field_mapping` | 映射规则 | 规则编号、源字段、目标字段 |
| 采集代理 | `agent_heartbeat` | 采集代理 | 代理编号、部署主机、代理版本 |
| 事件校验 | `event_validate` | 校验规则 | 规则编号、关联事件、字段检查 |
| 日志轮转 | `log_rotate` | 轮转策略 | 策略编号、关联源、轮转间隔 |
| 日志富化 | `enrich_join` | 富化规则 | 规则编号、目标字段、维表来源 |
| 告警规则 | `alert_rule` | 告警规则 | 规则编号、关联源、触发条件 |
| 日志分区 | `log_partition` | 分区规则 | 规则编号、关联源、分区键 |
| 采集位点 | `collect_checkpoint` | 位点记录 | 记录编号、关联源、文件偏移 |
| 窗口去重 | `dedup_window` | 去重窗口 | 窗口编号、关联源、去重键 |
| 聚合计算 | `aggregate_olap` | 聚合规则 | 规则编号、关联源、聚合维度 |
| 格式输出 | `format_output` | 输出配置 | 配置编号、关联源、输出格式 |
| 回溯采集 | `backfill_range` | 回溯任务 | 任务编号、关联源、时间范围 |
| 采样策略 | `sampling_rule` | 采样规则 | 规则编号、关联源、采样方式 |
| 质量诊断 | `log_quality` | 诊断报告 | 报告编号、关联源、解析成功率 |

## 分区布局代际迁移

日志分区原先按源前缀平铺（一代）：

```text
data/logs/partitions/<source_key>_<partition_key>.jsonl
```

新布局按源、批次、代际分层（二代），迁移与新增归档都落在这里：

```text
data/logs/partitions/gen2/source=<source_key>/batch=<batch_id>/gen=2/
    partition=<partition_key>.jsonl
    _manifest.json
data/logs/partitions/manifests/<source_key>_gen1_to_gen2.json   # 一代 -> 二代迁移清单
data/logs/partitions/manifests/index.json                       # 清单索引（流水线核对入口）
data/logs/partitions/merges/<merge_id>.json                     # 归并证据
```

- **迁移清单**：逐文件列出分区键、行数、摘要、已迁/待迁行数和目标批次，并给出
  当前有效布局（`gen1` / `dual` / `gen2`）。轮转流水线据此核对有效布局和具体文件。
- **存量保留**：迁移是复制，一代文件不删。二代记录带 `_origin`（原文件、原分区键、
  记录下标、载荷摘要），可按记录键回查原记录。
- **不重复入库**：每条记录有稳定 `record_id`（`source_key:line_no` 等），迁移、新归档
  和归并都按它去重；同一段日志不会因为两代并存或重复迁移入库两次。
- **快照归并**：先 `snapshot` 记录一代文件字节范围与二代批次集合，迁移期间新归档继续
  进来时，历史范围归并只读快照范围，行集合与迁移前一致。
- **恢复旧环境**：回退归并包含一代存量 + 已确认的代际新归档批次（`status` 为
  `confirmed`/`migrated`/`verified`），并逐批留下行数、摘要、文件清单和确认信息作为证据。

相关接口（`/api` 前缀）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/partition-migration/{source_key}/manifest` | 读取/刷新一代 -> 二代迁移清单 |
| GET | `/partition-migration/{source_key}/layout` | 按清单核对有效布局与具体文件 |
| POST | `/partition-migration/{source_key}/snapshot` | 记录历史范围快照 |
| POST | `/partition-migration/{source_key}/migrate?fraction=0.5` | 迁移指定比例存量（默认一半） |
| POST | `/partition-migration/{source_key}/new-archive?count=N` | 上线期写入二代新归档 |
| GET | `/partition-migration/{source_key}/merge?mode=current` | 带证据归并（legacy/history/current/restore） |
| GET | `/partition-migration/{source_key}/lookup?record_id=...` | 按记录键回查两代位置 |
| GET | `/partition-migration/{source_key}/generations` | 可查代次与分区覆盖 |
| GET | `/partition-migration/{source_key}/compare` | 对比迁移中/恢复后/回退路径行集合 |
| POST | `/partition-migration/{source_key}/drill` | 完整演练：旧归并 -> 迁移一半 -> 新归档 -> 对比 |

`POST /pipelines/rotate-partition/{source_key}` 也接入了这条链路：轮转 -> 分区 ->
迁移清单 ->（可选）快照/迁移/新归档 -> 布局核对 -> 带证据归并；可用
`migrate_fraction`、`new_archive_count` 查询参数触发迁移与归档。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
