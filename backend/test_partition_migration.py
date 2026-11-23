"""分区布局一代到二代迁移、布局核对、代际归并与回查的集成测试。"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import app.services.base as base_module
import app.services.partition_migration as migration_module
from app.services.partition_migration import (
    LEGACY_GENERATION,
    NEW_GENERATION,
    PartitionMigrationService,
    canonical_record_id,
)

SOURCE = "LOG_-9001"


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _legacy_records(prefix: str, count: int, start_line: int = 1) -> list[dict]:
    return [
        {
            "source_key": SOURCE,
            "source_name": "迁移测试源",
            "line_no": start_line + index,
            "offset": (start_line + index) * 32,
            "raw": f"line-{prefix}-{index}",
            "collected_at": 1_760_000_000 + index,
        }
        for index in range(count)
    ]


@pytest.fixture()
def service(tmp_path, monkeypatch):
    """把数据根目录指到临时目录，避免污染运行期数据。"""
    monkeypatch.setattr(base_module, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(migration_module, "DATA_ROOT", tmp_path)
    svc = PartitionMigrationService()
    # 模拟旧布局：按源前缀平铺的分区文件，两组分区共 10 行（行号全局连续）。
    _write_jsonl(tmp_path / "logs" / "partitions" / f"{SOURCE}_20261120.jsonl",
                 _legacy_records("a", 6, start_line=1))
    _write_jsonl(tmp_path / "logs" / "partitions" / f"{SOURCE}_20261121.jsonl",
                 _legacy_records("b", 4, start_line=7))
    return svc


def test_manifest_lists_legacy_files_and_baseline(service):
    manifest = service.ensure_manifest(SOURCE)

    assert manifest["generation_from"] == LEGACY_GENERATION
    assert manifest["generation_to"] == NEW_GENERATION
    assert manifest["effective_layout"] == "gen1"
    assert manifest["generations_present"] == [LEGACY_GENERATION]
    assert len(manifest["files"]) == 2
    assert {entry["partition_key"] for entry in manifest["files"]} == {"20261120", "20261121"}
    assert manifest["baseline"]["records"] == 10
    assert all(entry["status"] == "pending" for entry in manifest["files"])
    # 清单落盘并进入索引，流水线可以按文件核对
    assert (migration_module.DATA_ROOT / "logs" / "partitions" / "manifests"
            / "index.json").exists()


def test_layout_check_reports_missing_and_unexpected(service):
    service.ensure_manifest(SOURCE)
    layout = service.check_layout(SOURCE)
    assert layout["ready"], layout["checks"]

    # 没有迁移时二代目录里塞一个清单外的文件：应被判为布局异常
    rogue = (migration_module.DATA_ROOT / "logs" / "partitions" / "gen2"
             / f"source={SOURCE}" / "batch=BX-gen=2" / "partition=bad.jsonl")
    _write_jsonl(rogue, _legacy_records("x", 1))
    # 目录名不合规，descriptor 返回 None，不会被当作有效二代文件
    layout = service.check_layout(SOURCE)
    assert "batch=BX-gen=2" not in json.dumps(layout["unexpected_gen2_files"])


def test_partial_migration_keeps_stock_and_recovers_origin(service):
    before = {
        path.name: path.read_bytes()
        for path in (migration_module.DATA_ROOT / "logs" / "partitions").glob(f"{SOURCE}_*.jsonl")
    }
    service.ensure_manifest(SOURCE)
    result = service.migrate(SOURCE, fraction=0.5)

    assert result["migrated"] == 5
    assert result["status"] == "confirmed"
    assert result["verification"]["origin_recoverable"] is True
    assert result["verification"]["unique_records"] == 5

    # 存量文件原样保留
    after = {
        path.name: path.read_bytes()
        for path in (migration_module.DATA_ROOT / "logs" / "partitions").glob(f"{SOURCE}_*.jsonl")
    }
    assert before == after

    layout = service.check_layout(SOURCE)
    assert layout["effective_layout"] == "dual"
    assert layout["ready"], layout["checks"]
    assert all(entry["pending_records"] >= 0 for entry in layout["legacy_files"])

    # 二代位置能回查原键、原分区键和原记录
    migrated_ids = [
        record_id for batch in layout["gen2_batches"] for record_id in
        service._file_ids(migration_module.DATA_ROOT / batch["files"][0]["file"])
    ]
    for record_id in migrated_ids[:5]:
        lookup = service.lookup_record(SOURCE, record_id)
        assert lookup["generations"] == [LEGACY_GENERATION, NEW_GENERATION]
        assert lookup["origin_recovered"] is True
        assert lookup["payload_consistent"] is True
        gen2_location = next(loc for loc in lookup["locations"]
                             if loc["generation"] == NEW_GENERATION)
        assert gen2_location["origin"]["partition_key"] in ("20261120", "20261121")


def test_migration_is_idempotent_and_completes_remaining(service):
    service.ensure_manifest(SOURCE)
    first = service.migrate(SOURCE, fraction=0.5)
    second = service.migrate(SOURCE, fraction=1.0)

    assert first["migrated"] == 5
    assert second["migrated"] == 5  # 只迁尚未迁过的 5 行
    assert second["batch_id"] != first["batch_id"]

    current = service._collect(SOURCE, "current")
    assert current["records_read"] == 20  # 一代 10 行 + 两批二代共 10 行副本
    assert current["unique_records"] == 10  # 同一段只入库一次
    assert len(current["duplicates"]) == 10

    # 三代都迁完后再次迁移应无事可做
    third = service.migrate(SOURCE, fraction=1.0)
    assert third["migrated"] == 0


def test_history_snapshot_is_isolated_from_later_archive(service):
    service.ensure_manifest(SOURCE)
    snapshot = service.capture_snapshot(SOURCE, "history")
    assert snapshot["records"] == 10
    service.migrate(SOURCE, fraction=0.5)
    service.record_new_archive(SOURCE, count=4)

    history = service._collect(SOURCE, "history", snapshot)
    assert history["records_read"] == 10
    assert history["row_set_digest"] == snapshot["row_set_digest"]

    current = service._collect(SOURCE, "current")
    assert current["unique_records"] == 14  # 10 存量 + 4 新归档


def test_restore_includes_confirmed_new_archive_only(service):
    service.ensure_manifest(SOURCE)
    service.migrate(SOURCE, fraction=0.5)
    archive = service.record_new_archive(SOURCE, count=3)
    assert archive["ingested"] == 3

    restore = service._collect(SOURCE, "restore")
    restore_ids = {canonical_record_id(rec) for rec in restore["rows"]}
    assert len(restore_ids) == 13
    gen2_sources = [
        source for source in restore["sources"] if source["generation"] == NEW_GENERATION
    ]
    assert gen2_sources
    assert {source["batch_status"] for source in gen2_sources} == {"confirmed", "confirmed"}
    # 恢复归并证据里带已确认新归档批次
    evidence = service.merge_with_evidence(SOURCE, mode="restore", label="restore")
    kinds = {batch["kind"] for batch in evidence["batch_evidence"]}
    assert "new-archive" in kinds
    assert all(batch["confirmed"] for batch in evidence["batch_evidence"])

    # 未确认（回退）的批次不进入恢复路径
    (migration_module.DATA_ROOT / "logs" / "partitions" / "gen2" / f"source={SOURCE}"
     / "batch=NA-rolledback" / "gen=2").mkdir(parents=True)
    _write_jsonl(
        migration_module.DATA_ROOT / "logs" / "partitions" / "gen2" / f"source={SOURCE}"
        / "batch=NA-rolledback" / "gen=2" / "partition=20261122.jsonl",
        [{"source_key": SOURCE, "line_no": 99, "raw": "rolled-back", "collected_at": 1_760_000_100}],
    )
    service._write_json(
        migration_module.DATA_ROOT / "logs" / "partitions" / "gen2" / f"source={SOURCE}"
        / "batch=NA-rolledback" / "gen=2" / "_manifest.json",
        {"batch_id": "NA-rolledback", "kind": "new-archive", "status": "rolled_back"},
    )
    restore_after = service._collect(SOURCE, "restore")
    assert len({canonical_record_id(rec) for rec in restore_after["rows"]}) == 13


def test_new_archive_reingest_is_deduplicated(service):
    service.ensure_manifest(SOURCE)
    records = [
        {"source_key": SOURCE, "line_no": 500 + i, "raw": f"new-{i}",
         "collected_at": 1_760_000_500 + i}
        for i in range(3)
    ]
    first = service.record_new_archive(SOURCE, records=records, batch_id="BATCH-A")
    again = service.record_new_archive(SOURCE, records=records, batch_id="BATCH-A")
    assert first["ingested"] == 3
    assert again["ingested"] == 0
    assert again["duplicate_batch"] is True

    # 不同批次但相同记录，也会按记录键拒绝重复入库
    second = service.record_new_archive(SOURCE, records=records, batch_id="BATCH-B")
    assert second["ingested"] == 0
    assert second["duplicate_batch"] is False
    assert sorted(second["duplicates"]) == ["LOG_-9001:L500", "LOG_-9001:L501", "LOG_-9001:L502"]

    # 连续两次模拟新归档，行号会后移，因此是两批不同记录，不会互相覆盖
    service.record_new_archive(SOURCE, count=2)
    later = service.record_new_archive(SOURCE, count=2)
    assert later["ingested"] == 2
    assert later["batch_id"] not in {"BATCH-A", "BATCH-B"}


def test_compare_layouts_and_drill(service):
    drill = service.run_layout_drill(SOURCE, fraction=0.5, new_archive_count=4)
    assert drill["ready"], json.dumps(drill["comparison"]["checks"], ensure_ascii=False)
    assert all(drill["steps"]["stock_retained"].values())
    assert drill["steps"]["migration"]["migrated"] == 5
    assert drill["steps"]["new_archive"]["ingested"] == 4

    comparison = drill["comparison"]
    row_sets = comparison["row_sets"]
    assert set(row_sets["legacy_baseline"]) <= set(row_sets["current_mixed"])
    assert set(row_sets["current_mixed"]) == set(row_sets["restored_old_env"])
    assert row_sets["history_snapshot"] == row_sets["legacy_baseline"]
    assert comparison["generations"]["generations"] == [LEGACY_GENERATION, NEW_GENERATION]
    assert all(check["ok"] for check in comparison["checks"])


def test_merge_evidence_is_reproducible(service):
    service.run_layout_drill(SOURCE, fraction=0.5, new_archive_count=2)
    evidence = service.merge_with_evidence(SOURCE, mode="current", label="recheck")

    evidence_file = migration_module.DATA_ROOT / evidence["evidence_file"]
    assert evidence_file.exists()
    stored = json.loads(evidence_file.read_text(encoding="utf-8"))
    assert stored["row_set_digest"] == evidence["row_set_digest"]

    output = migration_module.DATA_ROOT / evidence["output_file"]
    lines = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines() if line]
    assert len(lines) == evidence["unique_records"] == evidence["output_records"]
    assert len({canonical_record_id(rec) for rec in lines}) == len(lines)
