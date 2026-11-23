"""分区布局代际迁移：把一代（按源前缀平铺）分区文件迁到二代（源/批次/代际）布局。

一代布局（存量）::

    data/logs/partitions/<source_key>_<partition_key>.jsonl

二代布局（新归档）::

    data/logs/partitions/gen2/source=<source_key>/batch=<batch_id>/gen=2/
        partition=<partition_key>.jsonl
        _manifest.json

迁移清单（一代 -> 二代）落在
``data/logs/partitions/manifests/<source_key>_gen1_to_gen2.json``。
归并证据落在 ``data/logs/partitions/merges/``。

设计要点：

* 每条记录有稳定的 ``record_id``（优先取记录自带的 record_id，其次
  ``source_key:line_no``，最后用规范化 JSON 的哈希）。归并、迁移都按它去重，
  同一段日志不会因为重复分区、重复迁移、两代并存而入库两次。
* 迁移是复制而不是搬移：存量文件保留，迁移后的记录带 ``_origin``，可以从
  二代位置回查原分区键、原文件与原记录。
* 归并支持快照：迁移期间新归档继续进来时，历史范围的归并只读取快照时刻
  的字节范围与批次集合，不受新归档影响。
* 恢复旧环境时，已确认的二代新归档批次也会并入恢复归并，并留下逐批的行数、
  摘要与文件清单作为归并证据。
"""
from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from app.services.base import BaseModuleService, DATA_ROOT

LEGACY_GENERATION = 1
NEW_GENERATION = 2
LEGACY_LAYOUT = "source-prefix-flat"
NEW_LAYOUT = "source=<source_key>/batch=<batch_id>/gen=2"
# 这些状态的二代批次可以被恢复路径采信为“已确认的新归档范围”。
CONFIRMED_BATCH_STATUSES = {"migrated", "verified", "confirmed", "new-archive"}
INACTIVE_BATCH_STATUSES = {"rolled_back", "abandoned"}

_LAYOUT_PART = re.compile(r"^source=(?P<source>[^/]+)$")
_BATCH_PART = re.compile(r"^batch=(?P<batch>[^/]+)$")
_GEN_PART = re.compile(r"^gen=(?P<gen>\d+)$")
_FILE_PART = re.compile(r"^partition=(?P<key>.+)\.jsonl$")

_META_PREFIX = "_"


# -- 记录键与摘要 --


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def canonical_payload(record: dict[str, Any]) -> dict[str, Any]:
    """去掉迁移/代际元数据后的原始记录，用于跨代际比较行集合。"""
    return {key: value for key, value in record.items() if not str(key).startswith(_META_PREFIX)}


def record_payload_hash(record: dict[str, Any]) -> str:
    return _sha(_stable_json(canonical_payload(record)))


def canonical_record_id(record: dict[str, Any]) -> str:
    """生成跨代际稳定的记录键；迁移后的记录保留同一个键。"""
    existing = record.get("_record_id") or record.get("record_id")
    if existing:
        return str(existing)
    source_key = record.get("source_key")
    line_no = record.get("line_no")
    if source_key and line_no is not None:
        return f"{source_key}:L{line_no}"
    if source_key and record.get("offset") is not None:
        return f"{source_key}:O{record.get('offset')}"
    return "H:" + _sha(_stable_json(canonical_payload(record)))[:24]


def _digest_ids(ids: Iterable[str]) -> str:
    return _sha("\n".join(sorted(set(ids))))


def _read_prefix(path: Path, size: int | None) -> bytes:
    """只读文件前 ``size`` 字节，用于读取快照时刻的历史范围。"""
    with open(path, "rb") as handle:
        return handle.read() if size is None else handle.read(size)


def _parse_jsonl_bytes(raw: bytes) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in raw.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


class PartitionMigrationService(BaseModuleService):
    """分区文件一代到二代迁移清单、布局核对与代际归并。"""

    MODULE = "log_partition"
    ENTITY = "分区布局迁移"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["源编号", "布局代次", "批次编号", "分区键", "记录数", "记录摘要", "迁移状态"]
    REQUIRED_FIELDS = ["源编号"]
    STATUS_ORDER = ["计划中", "迁移中", "已迁移", "已核对", "已回退"]
    ACTION_RULES = {"刷新清单": "计划中", "执行迁移": "迁移中", "标记核对": "已核对"}
    NEGATIVE_ACTIONS = []

    # -- 路径 --

    def _partitions_root(self) -> Path:
        return self._data_dir("logs") / "partitions"

    def _legacy_dir(self) -> Path:
        return self._partitions_root()

    def _gen2_root(self) -> Path:
        return self._partitions_root() / "gen2"

    def _manifest_dir(self) -> Path:
        return self._partitions_root() / "manifests"

    def _merge_dir(self) -> Path:
        return self._partitions_root() / "merges"

    def _manifest_path(self, source_key: str) -> Path:
        return self._manifest_dir() / f"{source_key.replace('/', '_')}_gen1_to_gen2.json"

    def _batch_dir(self, source_key: str, batch_id: str) -> Path:
        return (
            self._gen2_root()
            / f"source={source_key}"
            / f"batch={batch_id}"
            / f"gen={NEW_GENERATION}"
        )

    def _rel(self, path: Path) -> str:
        try:
            return str(path.relative_to(DATA_ROOT))
        except ValueError:
            return str(path)

    def _write_json(self, path: Path, value: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        return path

    def _read_json(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, json.JSONDecodeError):
            return None

    def _write_records(self, path: Path, records: list[dict[str, Any]]) -> tuple[int, str]:
        """按稳定顺序原子写入记录，返回 (条数, 内容摘要)。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        ordered = sorted(records, key=canonical_record_id)
        content = "".join(_stable_json(rec) + "\n" for rec in ordered)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(content)
        tmp.replace(path)
        return len(ordered), _sha(content)

    # -- 扫描与清单 --

    def _legacy_files(self, source_key: str) -> list[Path]:
        if not self._legacy_dir().exists():
            return []
        return sorted(
            path for path in self._legacy_dir().glob(f"{source_key}_*.jsonl") if path.is_file()
        )

    def _partition_key(self, source_key: str, path: Path) -> str:
        return path.name[len(source_key) + 1:-len(".jsonl")]

    def _gen2_files(self, source_key: str) -> list[Path]:
        root = self._gen2_root() / f"source={source_key}"
        if not root.exists():
            return []
        return sorted(root.glob(f"batch=*/gen={NEW_GENERATION}/partition=*.jsonl"))

    def _batch_manifest_path(self, source_key: str, batch_id: str) -> Path:
        return self._batch_dir(source_key, batch_id) / "_manifest.json"

    def _batch_manifest(self, source_key: str, batch_id: str) -> dict[str, Any] | None:
        return self._read_json(self._batch_manifest_path(source_key, batch_id))

    def _batch_status(self, source_key: str, batch_id: str) -> str:
        manifest = self._batch_manifest(source_key, batch_id)
        if manifest:
            return str(manifest.get("status", "unknown"))
        return "unknown"

    def _descriptor(self, source_key: str, path: Path) -> dict[str, Any] | None:
        """把物理文件解析成布局描述；二代布局不合规时返回 None。"""
        if path.parent == self._legacy_dir():
            return {
                "path": path,
                "rel": self._rel(path),
                "generation": LEGACY_GENERATION,
                "kind": "legacy",
                "partition_key": self._partition_key(source_key, path),
                "batch_id": None,
                "layout_ok": True,
            }
        try:
            parts = path.relative_to(self._gen2_root()).parts
        except ValueError:
            return None
        # source=... / batch=... / gen=2 / partition=....jsonl
        if len(parts) != 4:
            return None
        source_match = _LAYOUT_PART.match(parts[0])
        batch_match = _BATCH_PART.match(parts[1])
        gen_match = _GEN_PART.match(parts[2])
        file_match = _FILE_PART.match(parts[3])
        if not (source_match and batch_match and gen_match and file_match):
            return None
        if source_match.group("source") != source_key:
            return None
        if gen_match.group("gen") != str(NEW_GENERATION):
            return None
        batch_id = batch_match.group("batch")
        return {
            "path": path,
            "rel": self._rel(path),
            "generation": NEW_GENERATION,
            "kind": "gen2",
            "partition_key": file_match.group("key"),
            "batch_id": batch_id,
            "layout_ok": True,
            "batch_status": self._batch_status(source_key, batch_id),
        }

    def _file_ids(self, path: Path, size: int | None = None) -> list[str]:
        return [
            canonical_record_id(rec)
            for rec in _parse_jsonl_bytes(_read_prefix(path, size))
        ]

    def _scan_batches(self, source_key: str) -> list[dict[str, Any]]:
        batches: dict[str, dict[str, Any]] = {}
        ids_by_batch: dict[str, set[str]] = {}
        for path in self._gen2_files(source_key):
            descriptor = self._descriptor(source_key, path)
            if not descriptor or not descriptor.get("batch_id"):
                continue
            batch_id = descriptor["batch_id"]
            manifest = self._batch_manifest(source_key, batch_id) or {}
            entry = batches.setdefault(batch_id, {
                "batch_id": batch_id,
                "kind": manifest.get("kind", "unknown"),
                "status": manifest.get("status", "unknown"),
                "confirmed": manifest.get("status") in CONFIRMED_BATCH_STATUSES,
                "manifest_file": self._rel(self._batch_manifest_path(source_key, batch_id)),
                "partition_keys": [],
                "files": [],
                "records": 0,
                "unique_records": 0,
                "row_set_digest": None,
            })
            file_ids = self._file_ids(path)
            ids_by_batch.setdefault(batch_id, set()).update(file_ids)
            entry["partition_keys"].append(descriptor["partition_key"])
            entry["files"].append({
                "file": descriptor["rel"],
                "partition_key": descriptor["partition_key"],
                "records": len(file_ids),
            })
            entry["records"] += len(file_ids)
        for batch_id, entry in batches.items():
            ids = ids_by_batch.get(batch_id, set())
            entry["unique_records"] = len(ids)
            entry["row_set_digest"] = _digest_ids(ids)
            entry["partition_keys"] = sorted(set(entry["partition_keys"]))
        return sorted(batches.values(), key=lambda item: item["batch_id"])

    def _generations_present(self, source_key: str) -> list[int]:
        generations = set()
        if self._legacy_files(source_key):
            generations.add(LEGACY_GENERATION)
        if self._gen2_files(source_key):
            generations.add(NEW_GENERATION)
        return sorted(generations)

    def _effective_layout(self, source_key: str) -> str:
        gen1 = bool(self._legacy_files(source_key))
        gen2 = bool(self._gen2_files(source_key))
        if gen1 and gen2:
            return "dual"
        if gen2:
            return "gen2"
        if gen1:
            return "gen1"
        return "empty"

    def ensure_manifest(self, source_key: str, refresh: bool = True) -> dict[str, Any]:
        """建立或刷新一代 -> 二代迁移清单，返回清单内容。"""
        path = self._manifest_path(source_key)
        manifest = self._read_json(path)
        if manifest is None:
            manifest = {
                "manifest_id": f"MG-{source_key}-{int(time.time())}-{uuid.uuid4().hex[:6]}",
                "source_key": source_key,
                "generation_from": LEGACY_GENERATION,
                "generation_to": NEW_GENERATION,
                "legacy_layout": f"{LEGACY_LAYOUT}: <source_key>_<partition_key>.jsonl",
                "target_layout": f"{NEW_LAYOUT}/partition=<partition_key>.jsonl",
                "created_at": time.time(),
                "files": [],
                "batches": [],
                "migrated_record_ids": [],
                "baseline": None,
                "snapshots": {},
            }
        if refresh:
            self._refresh_manifest(manifest, source_key)
            self._write_json(path, manifest)
            self._refresh_index()
        return manifest

    def _refresh_manifest(self, manifest: dict[str, Any], source_key: str) -> None:
        migrated_ids = set(manifest.get("migrated_record_ids") or [])
        by_legacy = {entry["legacy_file"]: entry for entry in manifest.get("files", [])}
        entries: list[dict[str, Any]] = []
        legacy_ids: set[str] = set()
        total = 0

        for path in self._legacy_files(source_key):
            rel = self._rel(path)
            raw = _read_prefix(path, None)
            ids = [canonical_record_id(rec) for rec in _parse_jsonl_bytes(raw)]
            legacy_ids.update(ids)
            total += len(ids)
            entry = by_legacy.get(rel) or {
                "legacy_file": rel,
                "partition_key": self._partition_key(source_key, path),
                "generation": LEGACY_GENERATION,
                "targets": [],
                "migrated_at": None,
            }
            entry["records"] = len(ids)
            entry["unique_records"] = len(set(ids))
            entry["digest"] = _sha(raw.decode("utf-8", errors="replace"))
            # 只统计当前文件里仍然存在的待迁记录，避免把已消失的键算进去。
            pending = set(ids) - migrated_ids
            entry["pending_records"] = len(pending)
            if not entry.get("targets"):
                entry["status"] = "pending"
            elif not pending:
                entry["status"] = "migrated"
            else:
                entry["status"] = "partial"
            entries.append(entry)

        manifest["files"] = entries
        manifest["updated_at"] = time.time()
        manifest["generations_present"] = self._generations_present(source_key)
        manifest["effective_layout"] = self._effective_layout(source_key)
        manifest["batches"] = [
            {key: value for key, value in batch.items() if key != "files"}
            for batch in self._scan_batches(source_key)
        ]
        if not isinstance(manifest.get("baseline"), dict) or not manifest["baseline"].get("row_set_digest"):
            manifest["baseline"] = {
                "records": total,
                "unique_records": len(legacy_ids),
                "row_set_digest": _digest_ids(legacy_ids),
                "captured_at": time.time(),
                "captured_by": "manifest-init",
            }
        manifest["legacy_row_set"] = {
            "records": total,
            "unique_records": len(legacy_ids),
            "row_set_digest": _digest_ids(legacy_ids),
            "files": len(entries),
        }

    def _refresh_index(self) -> None:
        index_entries = []
        for path in sorted(self._manifest_dir().glob("*_gen1_to_gen2.json")):
            manifest = self._read_json(path)
            if manifest is None:
                continue
            index_entries.append({
                "source_key": manifest.get("source_key"),
                "manifest_file": self._rel(path),
                "manifest_id": manifest.get("manifest_id"),
                "effective_layout": manifest.get("effective_layout"),
                "generations_present": manifest.get("generations_present"),
                "updated_at": manifest.get("updated_at"),
            })
        self._write_json(self._manifest_dir() / "index.json", {
            "updated_at": time.time(),
            "manifests": index_entries,
        })

    def _save_manifest(self, source_key: str, manifest: dict[str, Any]) -> None:
        self._refresh_manifest(manifest, source_key)
        self._write_json(self._manifest_path(source_key), manifest)
        self._refresh_index()

    # -- 布局核对 --

    def check_layout(self, source_key: str) -> dict[str, Any]:
        """按迁移清单核对有效布局与具体文件，供轮转分区流水线上线期核对。"""
        manifest = self.ensure_manifest(source_key, refresh=True)
        legacy_entries = manifest["files"]
        batches = self._scan_batches(source_key)

        expected_targets: dict[str, dict[str, Any]] = {}
        for entry in legacy_entries:
            for target in entry.get("targets", []):
                for target_file in target.get("files", []):
                    expected_targets[target_file] = target

        actual_gen2 = set()
        unexpected_files: list[str] = []
        layout_problems: list[str] = []
        for descriptor in (
            self._descriptor(source_key, path) for path in self._gen2_files(source_key)
        ):
            if descriptor is None:
                continue
            actual_gen2.add(descriptor["rel"])
            manifest_batch = next(
                (b for b in batches if b["batch_id"] == descriptor["batch_id"]), None)
            if manifest_batch and manifest_batch["kind"] == "migration":
                if descriptor["rel"] not in expected_targets:
                    unexpected_files.append(descriptor["rel"])
            if descriptor["batch_status"] == "unknown":
                layout_problems.append(f"批次 {descriptor['batch_id']} 缺少清单")

        missing_targets = sorted(set(expected_targets) - actual_gen2)

        checks = [
            {
                "check": "迁移清单存在",
                "ok": self._manifest_path(source_key).exists(),
                "detail": self._rel(self._manifest_path(source_key)),
            },
            {
                "check": "有效布局可判定",
                "ok": manifest["effective_layout"] in ("gen1", "gen2", "dual"),
                "detail": manifest["effective_layout"],
            },
            {
                "check": "存量文件保留",
                "ok": all(path.exists() for path in self._legacy_files(source_key)),
                "detail": f"{len(legacy_entries)} 个一代文件",
            },
            {
                "check": "二代目标文件齐备",
                "ok": not missing_targets,
                "detail": f"缺失 {len(missing_targets)} 个",
            },
            {
                "check": "无清单外迁移文件",
                "ok": not unexpected_files,
                "detail": f"{len(unexpected_files)} 个",
            },
            {
                "check": "批次清单齐备",
                "ok": not layout_problems,
                "detail": "；".join(layout_problems) or "全部批次有清单",
            },
        ]

        return {
            "source_key": source_key,
            "manifest_id": manifest["manifest_id"],
            "manifest_file": self._rel(self._manifest_path(source_key)),
            "effective_layout": manifest["effective_layout"],
            "generations_present": manifest["generations_present"],
            "legacy_files": [
                {
                    "file": entry["legacy_file"],
                    "partition_key": entry["partition_key"],
                    "records": entry["records"],
                    "unique_records": entry["unique_records"],
                    "pending_records": entry["pending_records"],
                    "status": entry["status"],
                    "targets": entry.get("targets", []),
                    "digest": entry.get("digest"),
                }
                for entry in legacy_entries
            ],
            "gen2_batches": batches,
            "manifest_batches": manifest.get("batches", []),
            "expected_targets": sorted(expected_targets),
            "missing_targets": missing_targets,
            "unexpected_gen2_files": sorted(unexpected_files),
            "checks": checks,
            "ready": all(check["ok"] for check in checks),
        }

    # -- 快照 --

    def capture_snapshot(self, source_key: str, name: str = "history", refresh: bool = False) -> dict[str, Any]:
        """记录此刻的一代文件字节范围与二代批次集合，供历史范围归并使用。"""
        manifest = self.ensure_manifest(source_key, refresh=True)
        snapshots = manifest.setdefault("snapshots", {})
        if name in snapshots and not refresh:
            return snapshots[name]

        legacy: dict[str, dict[str, Any]] = {}
        ids: list[str] = []
        for path in self._legacy_files(source_key):
            raw = _read_prefix(path, None)
            file_ids = [canonical_record_id(rec) for rec in _parse_jsonl_bytes(raw)]
            ids.extend(file_ids)
            legacy[path.name] = {
                "file": self._rel(path),
                "size": len(raw),
                "records": len(file_ids),
                "row_set_digest": _digest_ids(file_ids),
            }
        snapshot = {
            "name": name,
            "source_key": source_key,
            "captured_at": time.time(),
            "manifest_id": manifest["manifest_id"],
            "legacy": legacy,
            "batches": [
                {
                    "batch_id": batch["batch_id"],
                    "kind": batch["kind"],
                    "status": batch["status"],
                }
                for batch in self._scan_batches(source_key)
            ],
            "row_set_digest": _digest_ids(ids),
            "records": len(ids),
        }
        snapshots[name] = snapshot
        self._write_json(self._manifest_path(source_key), manifest)
        return snapshot

    # -- 迁移 --

    def migrate(self, source_key: str, fraction: float = 0.5, batch_id: str | None = None) -> dict[str, Any]:
        """把存量的 ``fraction`` 记录复制到二代批次；存量文件保持不动。"""
        manifest = self.ensure_manifest(source_key, refresh=True)
        migrated_ids = set(manifest.get("migrated_record_ids") or [])

        candidates: list[tuple[Path, str, int, dict[str, Any], str]] = []
        candidate_ids: set[str] = set()
        for entry in manifest["files"]:
            path = DATA_ROOT / entry["legacy_file"]
            if not path.exists():
                continue
            for index, rec in enumerate(_parse_jsonl_bytes(_read_prefix(path, None))):
                record_id = canonical_record_id(rec)
                if record_id in migrated_ids or record_id in candidate_ids:
                    continue
                candidate_ids.add(record_id)
                candidates.append((path, entry["legacy_file"], index, rec, record_id))

        if not candidates:
            return {
                "source_key": source_key,
                "migrated": 0,
                "reason": "存量记录已全部迁移",
                "batch": None,
            }

        fraction = min(max(fraction, 0.0), 1.0)
        take = max(1, int(round(len(candidates) * fraction))) if fraction > 0 else 0
        selected = candidates[:take]
        if not selected:
            return {"source_key": source_key, "migrated": 0, "reason": "迁移比例为 0"}

        batch_id = batch_id or f"B2-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        by_partition: dict[str, list[dict[str, Any]]] = {}
        origin_index: list[dict[str, Any]] = []
        seen_batch: set[str] = set()

        for path, rel, index, rec, record_id in selected:
            if record_id in seen_batch:
                continue
            seen_batch.add(record_id)
            partition_key = self._partition_key(source_key, path)
            payload_hash = record_payload_hash(rec)
            target = {
                **rec,
                "_record_id": record_id,
                "_generation": NEW_GENERATION,
                "_origin": {
                    "generation": LEGACY_GENERATION,
                    "legacy_file": rel,
                    "partition_key": partition_key,
                    "record_index": index,
                    "payload_sha256": payload_hash,
                },
            }
            by_partition.setdefault(partition_key, []).append(target)
            origin_index.append({
                "record_id": record_id,
                "partition_key": partition_key,
                "legacy_file": rel,
                "record_index": index,
                "payload_sha256": payload_hash,
            })

        files: list[dict[str, Any]] = []
        written_ids: list[str] = []
        for partition_key, records in sorted(by_partition.items()):
            out_file = self._batch_dir(source_key, batch_id) / f"partition={partition_key}.jsonl"
            count, digest = self._write_records(out_file, records)
            files.append({
                "partition_key": partition_key,
                "file": self._rel(out_file),
                "records": count,
                "content_digest": digest,
                "origin_files": sorted({item["legacy_file"] for item in origin_index
                                        if item["partition_key"] == partition_key}),
            })
            written_ids.extend(item["record_id"] for item in origin_index
                               if item["partition_key"] == partition_key)

        verification = self._verify_batch(source_key, batch_id)
        batch_manifest = {
            "batch_id": batch_id,
            "source_key": source_key,
            "generation": NEW_GENERATION,
            "kind": "migration",
            "status": "confirmed" if verification["ok"] else "needs_review",
            "layout": f"{NEW_LAYOUT}/partition=<partition_key>.jsonl",
            "created_at": time.time(),
            "source_generation": LEGACY_GENERATION,
            "fraction": fraction,
            "files": files,
            "record_count": len(written_ids),
            "unique_records": len(set(written_ids)),
            "row_set_digest": _digest_ids(written_ids),
            "verification": verification,
            "origins": origin_index,
        }
        self._write_json(self._batch_manifest_path(source_key, batch_id), batch_manifest)

        for entry in manifest["files"]:
            moved = [item for item in origin_index if item["legacy_file"] == entry["legacy_file"]]
            if not moved:
                continue
            moved_partitions = sorted({item["partition_key"] for item in moved})
            moved_files = sorted(
                file["file"] for file in files if file["partition_key"] in moved_partitions
            )
            entry.setdefault("targets", []).append({
                "batch_id": batch_id,
                "generation": NEW_GENERATION,
                "partition_keys": moved_partitions,
                "files": moved_files,
                "records": len(moved),
                "migrated_at": time.time(),
            })
            entry["migrated_at"] = time.time()

        manifest["migrated_record_ids"] = sorted(migrated_ids | set(written_ids))
        self._save_manifest(source_key, manifest)

        return {
            "source_key": source_key,
            "batch_id": batch_id,
            "migrated": len(written_ids),
            "fraction": fraction,
            "candidates": len(candidates),
            "files": files,
            "status": batch_manifest["status"],
            "verification": verification,
            "manifest_file": self._rel(self._manifest_path(source_key)),
            "batch_manifest_file": self._rel(self._batch_manifest_path(source_key, batch_id)),
        }

    def _verify_batch(self, source_key: str, batch_id: str) -> dict[str, Any]:
        """重新读取二代批次，核对行数、摘要与来源回查能力。"""
        manifest = self._batch_manifest(source_key, batch_id) or {}
        expected_digest = manifest.get("row_set_digest")
        records: list[dict[str, Any]] = []
        for path in sorted(self._batch_dir(source_key, batch_id).glob("partition=*.jsonl")):
            records.extend(_parse_jsonl_bytes(_read_prefix(path, None)))
        ids = [canonical_record_id(rec) for rec in records]
        digest = _digest_ids(ids)
        origin_ok = all(
            isinstance(rec.get("_origin"), dict)
            and rec["_origin"].get("legacy_file")
            and rec["_origin"].get("partition_key") is not None
            and rec["_origin"].get("payload_sha256")
            for rec in records
        )
        return {
            "ok": bool(records) and (expected_digest in (None, digest)) and origin_ok,
            "records": len(records),
            "unique_records": len(set(ids)),
            "row_set_digest": digest,
            "expected_digest": expected_digest,
            "origin_recoverable": origin_ok,
        }

    # -- 新归档入库 --

    def record_new_archive(
        self,
        source_key: str,
        records: list[dict[str, Any]] | None = None,
        count: int = 4,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """布局上线期间把新归档写入二代批次；相同内容重复入库会被识别。"""
        source_key = str(source_key)
        if records is None:
            records = self._simulate_new_archive(source_key, count)

        payload = _stable_json([canonical_payload(rec) for rec in records])
        batch_id = batch_id or f"NA-{_sha(payload)[:12]}"
        batch_dir = self._batch_dir(source_key, batch_id)
        existing = self._batch_manifest(source_key, batch_id)
        if existing is not None:
            return {
                "source_key": source_key,
                "batch_id": batch_id,
                "ingested": 0,
                "deduplicated": len(records),
                "duplicate_batch": True,
                "status": existing.get("status"),
                "batch_manifest_file": self._rel(self._batch_manifest_path(source_key, batch_id)),
            }

        known_ids: set[str] = set()
        for path in self._legacy_files(source_key):
            known_ids.update(self._file_ids(path))
        for path in self._gen2_files(source_key):
            known_ids.update(self._file_ids(path))

        accepted: list[dict[str, Any]] = []
        duplicates: list[str] = []
        by_partition: dict[str, list[dict[str, Any]]] = {}
        for rec in records:
            record_id = canonical_record_id(rec)
            if record_id in known_ids:
                duplicates.append(record_id)
                continue
            known_ids.add(record_id)
            target = {
                **rec,
                "_record_id": record_id,
                "_generation": NEW_GENERATION,
                "_origin": {"generation": NEW_GENERATION, "archive_batch": batch_id},
            }
            partition_key = str(rec.get("partition_key") or self._archive_partition_key(rec))
            by_partition.setdefault(partition_key, []).append(target)
            accepted.append(record_id)

        files: list[dict[str, Any]] = []
        for partition_key, part_records in sorted(by_partition.items()):
            out_file = batch_dir / f"partition={partition_key}.jsonl"
            written, digest = self._write_records(out_file, part_records)
            files.append({
                "partition_key": partition_key,
                "file": self._rel(out_file),
                "records": written,
                "content_digest": digest,
            })

        batch_manifest = {
            "batch_id": batch_id,
            "source_key": source_key,
            "generation": NEW_GENERATION,
            "kind": "new-archive",
            "status": "confirmed",
            "layout": f"{NEW_LAYOUT}/partition=<partition_key>.jsonl",
            "created_at": time.time(),
            "confirmed_at": time.time(),
            "confirmation": {
                "by": "upstream-archive",
                "layout": NEW_LAYOUT,
                "row_set_digest": _digest_ids(accepted),
                "records": len(accepted),
            },
            "files": files,
            "record_count": len(accepted),
            "unique_records": len(set(accepted)),
            "row_set_digest": _digest_ids(accepted),
            "duplicates_dropped": duplicates,
        }
        self._write_json(self._batch_manifest_path(source_key, batch_id), batch_manifest)
        self.ensure_manifest(source_key, refresh=True)

        return {
            "source_key": source_key,
            "batch_id": batch_id,
            "ingested": len(accepted),
            "deduplicated": len(duplicates),
            "duplicates": duplicates,
            "duplicate_batch": False,
            "files": files,
            "status": batch_manifest["status"],
            "batch_manifest_file": self._rel(self._batch_manifest_path(source_key, batch_id)),
        }

    @staticmethod
    def _archive_partition_key(record: dict[str, Any]) -> str:
        collected_at = record.get("collected_at")
        if isinstance(collected_at, (int, float)):
            return time.strftime("%Y%m%d", time.localtime(collected_at))
        return str(record.get("partition_key") or "default")

    def _simulate_new_archive(self, source_key: str, count: int) -> list[dict[str, Any]]:
        """模拟布局上线期间新归档进来的日志行（行号接在两代数据之后）。"""
        max_line = 0
        for path in [*self._legacy_files(source_key), *self._gen2_files(source_key)]:
            for rec in _parse_jsonl_bytes(_read_prefix(path, None)):
                try:
                    max_line = max(max_line, int(rec.get("line_no") or 0))
                except (TypeError, ValueError):
                    continue
        records = []
        now = time.time()
        for offset in range(count):
            line_no = max_line + offset + 1
            collected_at = now + offset
            records.append({
                "source_key": source_key,
                "source_name": f"{source_key}-archive",
                "line_no": line_no,
                "offset": line_no * 64,
                "raw": f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(collected_at))} "
                       f"[new-archive] INFO source={source_key} seq={line_no}",
                "collected_at": collected_at,
            })
        return records

    # -- 归并 --

    def _read_legacy_sources(
        self, source_key: str, snapshot: dict[str, Any] | None
    ) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[str]]:
        sources: list[dict[str, Any]] = []
        rows_by_file: dict[str, list[dict[str, Any]]] = {}
        missing: list[str] = []
        snapshot_files = (snapshot or {}).get("legacy") or {}

        for path in self._legacy_files(source_key):
            meta = snapshot_files.get(path.name) if snapshot else None
            if snapshot and meta is None:
                continue  # 快照之后才出现的文件不属于历史范围
            size = meta["size"] if meta else None
            raw = _read_prefix(path, size)
            records = _parse_jsonl_bytes(raw)
            ids = [canonical_record_id(rec) for rec in records]
            rel = self._rel(path)
            rows_by_file[rel] = records
            sources.append({
                "generation": LEGACY_GENERATION,
                "kind": "legacy",
                "file": rel,
                "partition_key": self._partition_key(source_key, path),
                "batch_id": None,
                "bytes_read": len(raw),
                "snapshot_bytes": size,
                "records_read": len(records),
                "unique_records": len(set(ids)),
                "row_set_digest": _digest_ids(ids),
            })
        if snapshot:
            for meta in snapshot_files.values():
                if not (DATA_ROOT / meta["file"]).exists():
                    missing.append(meta["file"])
        return rows_by_file, sources, missing

    def _read_gen2_sources(
        self, source_key: str, batch_ids: set[str] | None
    ) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
        rows_by_file: dict[str, list[dict[str, Any]]] = {}
        sources: list[dict[str, Any]] = []
        for path in self._gen2_files(source_key):
            descriptor = self._descriptor(source_key, path)
            if descriptor is None:
                continue
            batch_id = descriptor["batch_id"] or ""
            if batch_ids is not None and batch_id not in batch_ids:
                continue
            records = _parse_jsonl_bytes(_read_prefix(path, None))
            ids = [canonical_record_id(rec) for rec in records]
            rows_by_file[descriptor["rel"]] = records
            sources.append({
                "generation": NEW_GENERATION,
                "kind": "gen2",
                "file": descriptor["rel"],
                "partition_key": descriptor["partition_key"],
                "batch_id": batch_id,
                "batch_status": descriptor["batch_status"],
                "records_read": len(records),
                "unique_records": len(set(ids)),
                "row_set_digest": _digest_ids(ids),
            })
        return rows_by_file, sources

    def _collect(
        self,
        source_key: str,
        mode: str,
        snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """按模式收集待归并行，并按稳定记录键去重。"""
        batch_ids: set[str] | None = None
        legacy_snapshot: dict[str, Any] | None = None

        if mode == "legacy":
            batch_ids = set()
        elif mode == "history":
            if snapshot is None:
                raise ValueError("history 模式需要快照")
            legacy_snapshot = snapshot
            batch_ids = {item["batch_id"] for item in snapshot.get("batches", [])}
        elif mode == "current":
            batch_ids = {
                batch["batch_id"]
                for batch in self._scan_batches(source_key)
                if batch["status"] not in INACTIVE_BATCH_STATUSES
            }
        elif mode == "restore":
            batch_ids = {
                batch["batch_id"]
                for batch in self._scan_batches(source_key)
                if batch["kind"] == "new-archive"
                and batch["status"] in CONFIRMED_BATCH_STATUSES
                and batch["status"] not in INACTIVE_BATCH_STATUSES
            }
        else:
            raise ValueError(f"未知归并模式: {mode}")

        legacy_rows, legacy_files, missing = self._read_legacy_sources(source_key, legacy_snapshot)
        gen2_rows, gen2_files = (
            self._read_gen2_sources(source_key, batch_ids) if batch_ids else ({}, [])
        )

        merged: list[dict[str, Any]] = []
        seen: set[str] = set()
        duplicates: list[dict[str, Any]] = []
        added: dict[str, int] = {}

        def consume(rows_by_file: dict[str, list[dict[str, Any]]], generation: int) -> None:
            for file_key, records in rows_by_file.items():
                contributed = 0
                for record in records:
                    record_id = canonical_record_id(record)
                    if record_id in seen:
                        duplicates.append({
                            "record_id": record_id,
                            "file": file_key,
                            "generation": generation,
                            "reason": "already-merged",
                        })
                        continue
                    seen.add(record_id)
                    merged.append(record)
                    contributed += 1
                added[file_key] = contributed

        # 一代行先入库（历史范围优先），二代仅补齐未出现过的记录。
        consume(legacy_rows, LEGACY_GENERATION)
        consume(gen2_rows, NEW_GENERATION)

        sources = [*legacy_files, *gen2_files]
        for source in sources:
            source["records_added"] = added.get(source["file"], 0)

        ids = [canonical_record_id(record) for record in merged]
        return {
            "mode": mode,
            "rows": merged,
            "sources": sources,
            "missing_snapshot_files": missing,
            "duplicates": duplicates,
            "records_read": sum(source["records_read"] for source in sources),
            "unique_records": len(ids),
            "row_set_digest": _digest_ids(ids),
        }

    def merge_with_evidence(
        self,
        source_key: str,
        mode: str = "current",
        snapshot_name: str | None = None,
        label: str | None = None,
    ) -> dict[str, Any]:
        """按代次归并并写出可复查的明细与证据。"""
        snapshot = None
        if mode == "history":
            snapshot_name = snapshot_name or "history"
            manifest = self.ensure_manifest(source_key, refresh=True)
            snapshot = (manifest.get("snapshots") or {}).get(snapshot_name)
            if snapshot is None:
                snapshot = self.capture_snapshot(source_key, snapshot_name)

        collected = self._collect(source_key, mode, snapshot)
        merge_id = f"MRG-{source_key}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        label = label or mode
        out_file = self._merge_dir() / f"{source_key}_{merge_id}_{label}.jsonl"
        count, digest = self._write_records(out_file, collected["rows"])

        batch_evidence = []
        batch_ids = {
            source["batch_id"] for source in collected["sources"]
            if source.get("generation") == NEW_GENERATION and source.get("batch_id")
        }
        for batch_id in sorted(batch_ids):
            batch = self._batch_manifest(source_key, batch_id) or {}
            evidence = {
                "batch_id": batch_id,
                "kind": batch.get("kind"),
                "status": batch.get("status"),
                "confirmed": batch.get("status") in CONFIRMED_BATCH_STATUSES,
                "batch_manifest_file": self._rel(self._batch_manifest_path(source_key, batch_id)),
                "files": batch.get("files", []),
                "record_count": batch.get("record_count", 0),
                "unique_records": batch.get("unique_records", 0),
                "row_set_digest": batch.get("row_set_digest"),
                "confirmation": batch.get("confirmation"),
                "verification": batch.get("verification"),
            }
            if mode == "restore" and not (evidence["confirmed"]):
                continue
            batch_evidence.append(evidence)

        evidence = {
            "merge_id": merge_id,
            "label": label,
            "source_key": source_key,
            "mode": mode,
            "snapshot": snapshot_name if mode == "history" else None,
            "created_at": time.time(),
            "manifest_file": self._rel(self._manifest_path(source_key)),
            "read_generations": sorted({source["generation"] for source in collected["sources"]}),
            "sources": collected["sources"],
            "batch_evidence": batch_evidence,
            "missing_snapshot_files": collected["missing_snapshot_files"],
            "records_read": collected["records_read"],
            "unique_records": collected["unique_records"],
            "duplicates_dropped": len(collected["duplicates"]),
            "duplicate_samples": collected["duplicates"][:20],
            "row_set_digest": collected["row_set_digest"],
            "output_file": self._rel(out_file),
            "output_digest": digest,
            "output_records": count,
        }
        evidence_file = self._merge_dir() / f"{merge_id}.json"
        self._write_json(evidence_file, evidence)
        evidence["evidence_file"] = self._rel(evidence_file)
        return evidence

    # -- 回查 --

    def lookup_record(self, source_key: str, record_id: str) -> dict[str, Any]:
        """按记录键回查两代布局中的位置；迁入二代的行能指回原分区键与原文。"""
        locations: list[dict[str, Any]] = []
        payload_hashes: dict[int, str] = {}

        for path in self._legacy_files(source_key):
            for index, rec in enumerate(_parse_jsonl_bytes(_read_prefix(path, None))):
                if canonical_record_id(rec) != record_id:
                    continue
                payload_hashes[LEGACY_GENERATION] = record_payload_hash(rec)
                locations.append({
                    "generation": LEGACY_GENERATION,
                    "file": self._rel(path),
                    "partition_key": self._partition_key(source_key, path),
                    "record_index": index,
                    "payload_sha256": record_payload_hash(rec),
                    "record": rec,
                })
                break

        for path in self._gen2_files(source_key):
            descriptor = self._descriptor(source_key, path)
            if descriptor is None:
                continue
            for index, rec in enumerate(_parse_jsonl_bytes(_read_prefix(path, None))):
                if canonical_record_id(rec) != record_id:
                    continue
                payload_hashes[NEW_GENERATION] = record_payload_hash(rec)
                locations.append({
                    "generation": NEW_GENERATION,
                    "file": descriptor["rel"],
                    "batch_id": descriptor["batch_id"],
                    "batch_status": descriptor["batch_status"],
                    "partition_key": descriptor["partition_key"],
                    "record_index": index,
                    "payload_sha256": record_payload_hash(rec),
                    "origin": rec.get("_origin"),
                    "record": rec,
                })
                break

        origin_recovered = False
        origin_checked = 0
        for location in locations:
            origin = location.get("origin")
            if not isinstance(origin, dict) or "legacy_file" not in origin:
                continue
            origin_checked += 1
            legacy_path = DATA_ROOT / str(origin.get("legacy_file"))
            if not legacy_path.exists():
                continue
            legacy_records = _parse_jsonl_bytes(_read_prefix(legacy_path, None))
            position = origin.get("record_index")
            if isinstance(position, int) and 0 <= position < len(legacy_records):
                original = legacy_records[position]
                origin_recovered = origin_recovered or (
                    record_payload_hash(original) == origin.get("payload_sha256")
                    and canonical_record_id(original) == record_id
                )

        consistent = len(set(payload_hashes.values())) <= 1
        return {
            "source_key": source_key,
            "record_id": record_id,
            "generations": sorted({location["generation"] for location in locations}),
            "locations": locations,
            "payload_consistent": consistent,
            "origin_checked": origin_checked,
            "origin_recovered": origin_recovered,
            "found": bool(locations),
        }

    def queryable_generations(self, source_key: str) -> dict[str, Any]:
        """返回该源当前可查询的代次以及每个分区键的布局覆盖情况。"""
        partitions: dict[str, dict[str, Any]] = {}
        for path in self._legacy_files(source_key):
            key = self._partition_key(source_key, path)
            entry = partitions.setdefault(key, {"partition_key": key, "generations": [], "files": []})
            entry["generations"] = sorted(set(entry["generations"]) | {LEGACY_GENERATION})
            entry["files"].append({
                "generation": LEGACY_GENERATION,
                "file": self._rel(path),
                "records": len(_parse_jsonl_bytes(_read_prefix(path, None))),
            })
        for path in self._gen2_files(source_key):
            descriptor = self._descriptor(source_key, path)
            if descriptor is None:
                continue
            key = descriptor["partition_key"]
            entry = partitions.setdefault(key, {"partition_key": key, "generations": [], "files": []})
            entry["generations"] = sorted(set(entry["generations"]) | {NEW_GENERATION})
            entry["files"].append({
                "generation": NEW_GENERATION,
                "batch_id": descriptor["batch_id"],
                "batch_status": descriptor["batch_status"],
                "file": descriptor["rel"],
                "records": len(_parse_jsonl_bytes(_read_prefix(path, None))),
            })
        return {
            "source_key": source_key,
            "generations": self._generations_present(source_key),
            "batches": self._scan_batches(source_key),
            "partitions": sorted(partitions.values(), key=lambda item: item["partition_key"]),
        }

    # -- 场景对比 --

    def compare_layouts(self, source_key: str, snapshot_name: str = "history") -> dict[str, Any]:
        """对比迁移中、恢复后与回退路径的行集合和可查代次。"""
        manifest = self.ensure_manifest(source_key, refresh=True)
        snapshot = (manifest.get("snapshots") or {}).get(snapshot_name)
        if snapshot is None:
            snapshot = self.capture_snapshot(source_key, snapshot_name)

        legacy = self._collect(source_key, "legacy")
        history = self._collect(source_key, "history", snapshot)
        current = self._collect(source_key, "current")
        restore = self._collect(source_key, "restore")

        legacy_ids = {canonical_record_id(rec) for rec in legacy["rows"]}
        history_ids = {canonical_record_id(rec) for rec in history["rows"]}
        current_ids = {canonical_record_id(rec) for rec in current["rows"]}
        restore_ids = {canonical_record_id(rec) for rec in restore["rows"]}

        migrated_ids: set[str] = set()
        confirmed_archive_ids: set[str] = set()
        unconfirmed_archive_ids: set[str] = set()
        for path in self._gen2_files(source_key):
            descriptor = self._descriptor(source_key, path)
            if descriptor is None:
                continue
            manifest_batch = next(
                (batch for batch in self._scan_batches(source_key)
                 if batch["batch_id"] == descriptor["batch_id"]),
                None,
            )
            if manifest_batch is None:
                continue
            file_ids = set(self._file_ids(path))
            if manifest_batch["kind"] == "migration":
                migrated_ids.update(file_ids)
            elif manifest_batch["kind"] == "new-archive":
                if (manifest_batch["status"] in CONFIRMED_BATCH_STATUSES
                        and manifest_batch["status"] not in INACTIVE_BATCH_STATUSES):
                    confirmed_archive_ids.update(file_ids)
                else:
                    unconfirmed_archive_ids.update(file_ids)

        # 二代副本中与存量同键的行会在归并时被去重，其余是新增行。
        gen2_ids = migrated_ids | confirmed_archive_ids
        expected_duplicates = len(gen2_ids & legacy_ids)
        expected_restore = legacy_ids | confirmed_archive_ids

        recoverable = {
            record_id: self.lookup_record(source_key, record_id)
            for record_id in list(sorted(migrated_ids))[:200]
        }
        origin_ok = all(item["origin_recovered"] for item in recoverable.values())
        consistent_ok = all(item["payload_consistent"] for item in recoverable.values())

        scenarios = {
            "legacy_baseline": self._scenario(legacy, "回退路径（仅一代存量）"),
            "history_snapshot": self._scenario(history, "迁移中（历史范围快照）"),
            "current_mixed": self._scenario(current, "迁移中（两代并用）"),
            "restored_old_env": self._scenario(restore, "恢复旧环境（存量+已确认新归档）"),
        }

        checks = [
            {
                "check": "历史范围归并锁定快照行集合",
                "ok": history["row_set_digest"] == snapshot["row_set_digest"],
                "detail": f"{history['unique_records']} 行，摘要 {history['row_set_digest'][:12]}",
            },
            {
                "check": "历史范围不受迁后新归档影响",
                "ok": history_ids == legacy_ids,
                "detail": f"历史 {len(history_ids)}，当前存量 {len(legacy_ids)}",
            },
            {
                "check": "迁移中两代并用不重复入库",
                "ok": len(current["duplicates"]) == expected_duplicates
                and current["unique_records"] == len(current_ids),
                "detail": f"二代副本 {len(migrated_ids | confirmed_archive_ids)} 行，"
                          f"去重 {len(current['duplicates'])} 行，预期 {expected_duplicates} 行",
            },
            {
                "check": "迁移中行集合覆盖存量",
                "ok": legacy_ids <= current_ids,
                "detail": f"存量 {len(legacy_ids)}，当前 {len(current_ids)}",
            },
            {
                "check": "恢复旧环境包含已确认新归档",
                "ok": restore_ids == expected_restore,
                "detail": f"恢复 {len(restore_ids)}，预期 存量+已确认归档 {len(expected_restore)}",
            },
            {
                "check": "未确认新归档不进入恢复路径",
                "ok": not (unconfirmed_archive_ids & restore_ids),
                "detail": f"未确认 {len(unconfirmed_archive_ids)} 行",
            },
            {
                "check": "回退路径行集合等于迁移前存量",
                "ok": legacy["row_set_digest"] == history["row_set_digest"],
                "detail": f"{len(legacy_ids)} 行",
            },
            {
                "check": "二代记录可回查原键与原记录",
                "ok": origin_ok and consistent_ok,
                "detail": f"抽查 {len(recoverable)} 条，回查 {origin_ok}，载荷一致 {consistent_ok}",
            },
            {
                "check": "存量文件保留",
                "ok": all((DATA_ROOT / entry["legacy_file"]).exists() for entry in manifest["files"]),
                "detail": f"{len(manifest['files'])} 个文件",
            },
        ]

        return {
            "source_key": source_key,
            "manifest_file": self._rel(self._manifest_path(source_key)),
            "snapshot": {
                "name": snapshot_name,
                "captured_at": snapshot["captured_at"],
                "records": snapshot["records"],
                "row_set_digest": snapshot["row_set_digest"],
                "batches": snapshot["batches"],
            },
            "scenarios": scenarios,
            "row_sets": {
                "legacy_baseline": sorted(legacy_ids),
                "history_snapshot": sorted(history_ids),
                "current_mixed": sorted(current_ids),
                "restored_old_env": sorted(restore_ids),
            },
            "generations": self.queryable_generations(source_key),
            "checks": checks,
            "ready": all(check["ok"] for check in checks),
        }

    @staticmethod
    def _scenario(collected: dict[str, Any], description: str) -> dict[str, Any]:
        return {
            "description": description,
            "mode": collected["mode"],
            "records": collected["unique_records"],
            "records_read": collected["records_read"],
            "duplicates_dropped": len(collected["duplicates"]),
            "row_set_digest": collected["row_set_digest"],
            "read_generations": sorted({source["generation"] for source in collected["sources"]}),
        }

    @staticmethod
    def _evidence_summary(evidence: dict[str, Any], description: str) -> dict[str, Any]:
        return {
            "description": description,
            "merge_id": evidence["merge_id"],
            "mode": evidence["mode"],
            "records": evidence["unique_records"],
            "records_read": evidence["records_read"],
            "duplicates_dropped": evidence["duplicates_dropped"],
            "row_set_digest": evidence["row_set_digest"],
            "read_generations": evidence["read_generations"],
            "output_file": evidence["output_file"],
            "evidence_file": evidence["evidence_file"],
        }

    # -- 演练 --

    def run_layout_drill(
        self,
        source_key: str,
        fraction: float = 0.5,
        new_archive_count: int = 4,
        snapshot_name: str = "history",
    ) -> dict[str, Any]:
        """完整演练：旧布局先归并 -> 保留存量 -> 迁移一半 -> 新归档 -> 对比三条路径。"""
        manifest = self.ensure_manifest(source_key, refresh=True)
        stock_before = {
            entry["legacy_file"]: (DATA_ROOT / entry["legacy_file"]).stat().st_size
            for entry in manifest["files"]
            if (DATA_ROOT / entry["legacy_file"]).exists()
        }

        baseline_merge = self.merge_with_evidence(source_key, mode="legacy", label="baseline")
        manifest = self.ensure_manifest(source_key, refresh=True)
        manifest["baseline"] = {
            "records": baseline_merge["unique_records"],
            "unique_records": baseline_merge["unique_records"],
            "row_set_digest": baseline_merge["row_set_digest"],
            "captured_at": baseline_merge["created_at"],
            "captured_by": "baseline-merge",
            "evidence_file": baseline_merge["evidence_file"],
        }
        self._write_json(self._manifest_path(source_key), manifest)

        snapshot = self.capture_snapshot(source_key, snapshot_name, refresh=True)
        migration = self.migrate(source_key, fraction=fraction)
        archive = self.record_new_archive(source_key, count=new_archive_count)

        stock_after = {
            entry["legacy_file"]: (DATA_ROOT / entry["legacy_file"]).stat().st_size
            for entry in self.ensure_manifest(source_key, refresh=True)["files"]
            if (DATA_ROOT / entry["legacy_file"]).exists()
        }
        stock_retained = {
            file: stock_before.get(file) == size for file, size in stock_after.items()
        }

        mid_merge = self.merge_with_evidence(source_key, mode="current", label="migration")
        history_merge = self.merge_with_evidence(
            source_key, mode="history", snapshot_name=snapshot_name, label="history")
        restore_merge = self.merge_with_evidence(source_key, mode="restore", label="restore")
        rollback_merge = self.merge_with_evidence(source_key, mode="legacy", label="rollback")
        comparison = self.compare_layouts(source_key, snapshot_name=snapshot_name)

        return {
            "source_key": source_key,
            "manifest_file": self._rel(self._manifest_path(source_key)),
            "steps": {
                "baseline_merge": self._evidence_summary(baseline_merge, "旧布局归并"),
                "snapshot": {
                    "name": snapshot["name"],
                    "records": snapshot["records"],
                    "row_set_digest": snapshot["row_set_digest"],
                    "batches": snapshot["batches"],
                },
                "stock_retained": stock_retained,
                "migration": {
                    "batch_id": migration.get("batch_id"),
                    "migrated": migration.get("migrated"),
                    "fraction": migration.get("fraction"),
                    "status": migration.get("status"),
                    "verification": migration.get("verification"),
                },
                "new_archive": {
                    "batch_id": archive.get("batch_id"),
                    "ingested": archive.get("ingested"),
                    "deduplicated": archive.get("deduplicated"),
                    "status": archive.get("status"),
                },
            },
            "merges": {
                "migration": mid_merge,
                "history": history_merge,
                "restore": restore_merge,
                "rollback": rollback_merge,
            },
            "comparison": comparison,
            "ready": comparison["ready"] and all(stock_retained.values()),
        }
