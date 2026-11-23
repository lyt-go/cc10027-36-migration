"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.services.pipeline import pipeline

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# -- 跨模块管道端点 --

@router.post("/pipelines/collect")
def run_collect_pipeline(source_id: int = Query(default=0)) -> dict[str, Any]:
    """执行完整采集管道：采集 -> 解析 -> 映射 -> 输出。"""
    return pipeline.full_collect_pipeline(source_id)


@router.post("/pipelines/event/{event_code}")
def run_event_pipeline(event_code: str) -> dict[str, Any]:
    """执行事件追踪管道：跟踪 -> 缓冲 -> 校验。"""
    return pipeline.event_tracking_pipeline([event_code])


@router.post("/pipelines/backfill/{source_key}")
def run_backfill_pipeline(source_key: str, time_range: str = Query(default="2026-09-01~2026-09-30")) -> dict[str, Any]:
    """执行回溯采集管道：创建任务 -> 分批执行 -> 位点更新。"""
    return pipeline.backfill_pipeline(source_key, time_range)


@router.post("/pipelines/quality-alert/{source_key}")
def run_quality_alert_pipeline(source_key: str) -> dict[str, Any]:
    """执行质量诊断+告警管道：诊断 -> 评估 -> 触发告警 -> 位点回滚。"""
    return pipeline.quality_alert_pipeline(source_key)


@router.post("/pipelines/enrich-aggregate/{source_key}")
def run_enrich_aggregate_pipeline(source_key: str) -> dict[str, Any]:
    """执行富化+聚合管道：构建维表 -> 富化 -> 聚合 -> 告警评估。"""
    return pipeline.enrichment_aggregation_pipeline(source_key)


@router.post("/pipelines/dedup-sample/{source_key}")
def run_dedup_sample_pipeline(source_key: str) -> dict[str, Any]:
    """执行去重+采样管道：窗口去重 -> 采样 -> 格式化输出。"""
    return pipeline.dedup_sample_pipeline(source_key)


@router.post("/pipelines/rotate-partition/{source_key}")
def run_rotate_partition_pipeline(
    source_key: str,
    migrate_fraction: float = Query(default=0.0, ge=0.0, le=1.0),
    new_archive_count: int = Query(default=0, ge=0, le=1000),
) -> dict[str, Any]:
    """执行轮转+分区管道：轮转 -> 分区路由 -> 迁移清单核对 -> 带证据归并。"""
    return pipeline.rotate_partition_pipeline(
        source_key, migrate_fraction=migrate_fraction, new_archive_count=new_archive_count)


@router.get("/partition-migration/{source_key}/manifest")
def get_partition_manifest(source_key: str, refresh: bool = Query(default=True)) -> dict[str, Any]:
    """读取/刷新分区文件一代到二代迁移清单。"""
    return pipeline.partition_migration.ensure_manifest(source_key, refresh=refresh)


@router.get("/partition-migration/{source_key}/layout")
def check_partition_layout(source_key: str) -> dict[str, Any]:
    """按迁移清单核对有效布局和具体文件。"""
    return pipeline.partition_migration.check_layout(source_key)


@router.post("/partition-migration/{source_key}/migrate")
def migrate_partition_layout(
    source_key: str,
    fraction: float = Query(default=0.5, ge=0.0, le=1.0),
    batch_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """把指定比例的存量分区文件复制到二代布局，存量保留。"""
    return pipeline.partition_migration.migrate(source_key, fraction=fraction, batch_id=batch_id)


@router.post("/partition-migration/{source_key}/snapshot")
def snapshot_partition_history(
    source_key: str,
    name: str = Query(default="history"),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    """记录当前一代文件字节范围与二代批次集合，供历史范围归并。"""
    return pipeline.partition_migration.capture_snapshot(source_key, name=name, refresh=refresh)


@router.post("/partition-migration/{source_key}/new-archive")
def ingest_new_archive(
    source_key: str,
    count: int = Query(default=4, ge=0, le=1000),
    batch_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """布局上线期间把新归档写入二代批次。"""
    return pipeline.partition_migration.record_new_archive(
        source_key, count=count, batch_id=batch_id)


@router.get("/partition-migration/{source_key}/merge")
def merge_partition_layout(
    source_key: str,
    mode: str = Query(default="current", pattern="^(legacy|history|current|restore)$"),
    snapshot: str | None = Query(default=None),
    label: str | None = Query(default=None),
) -> dict[str, Any]:
    """按代次归并并写出明细与证据（legacy/history/current/restore）。"""
    return pipeline.partition_migration.merge_with_evidence(
        source_key, mode=mode, snapshot_name=snapshot, label=label)


@router.get("/partition-migration/{source_key}/lookup")
def lookup_partition_record(source_key: str, record_id: str) -> dict[str, Any]:
    """按记录键回查两代布局中的位置，并校验能否回查原键与原记录。"""
    result = pipeline.partition_migration.lookup_record(source_key, record_id)
    if not result["found"]:
        raise HTTPException(status_code=404, detail=f"记录 {record_id} 在两代布局中均未找到")
    return result


@router.get("/partition-migration/{source_key}/generations")
def queryable_generations(source_key: str) -> dict[str, Any]:
    """返回当前可查询的代次和每个分区键的布局覆盖。"""
    return pipeline.partition_migration.queryable_generations(source_key)


@router.get("/partition-migration/{source_key}/compare")
def compare_partition_layouts(
    source_key: str,
    snapshot: str = Query(default="history"),
) -> dict[str, Any]:
    """对比迁移中、恢复后与回退路径的行集合及可查代次。"""
    return pipeline.partition_migration.compare_layouts(source_key, snapshot_name=snapshot)


@router.post("/partition-migration/{source_key}/drill")
def run_partition_layout_drill(
    source_key: str,
    fraction: float = Query(default=0.5, ge=0.0, le=1.0),
    new_archive_count: int = Query(default=4, ge=0, le=1000),
) -> dict[str, Any]:
    """完整演练：旧布局归并 -> 保留存量 -> 迁移一半 -> 新归档 -> 三路径对比。"""
    return pipeline.partition_migration.run_layout_drill(
        source_key, fraction=fraction, new_archive_count=new_archive_count)


@router.post("/pipelines/agent-checkpoint/{agent_code}")
def run_agent_checkpoint_pipeline(agent_code: str) -> dict[str, Any]:
    """执行代理心跳+位点管道：心跳上报 -> 位点同步 -> 离线检测。"""
    return pipeline.agent_checkpoint_pipeline(agent_code)


@router.post("/pipelines/run-all")
def run_all_pipelines() -> dict[str, Any]:
    """运行所有管道组合。"""
    return pipeline.run_all_pipelines()
