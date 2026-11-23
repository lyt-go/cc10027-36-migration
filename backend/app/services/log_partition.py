"""日志分区：按分区规则将日志数据路由到不同分区路径。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LogPartitionService(BaseModuleService):
    MODULE = "log_partition"
    ENTITY = "分区规则"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["规则编号", "关联源", "分区键", "分区粒度", "路由策略", "保留天数", "归并策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联源", "分区键"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def partition_source(self, source_key: str) -> dict[str, Any]:
        """对指定日志源的已解析数据按分区规则路由写入。"""
        parsed_file = self._data_dir("logs") / f"{source_key}_parsed.jsonl"
        if not parsed_file.exists():
            return {"error": "无已解析数据", "partitions": 0}

        records = self._read_jsonl(parsed_file)
        rules = [r for r in store.rows(self.MODULE) if r.get("关联源") == source_key and r.get("status") == "已生效"]

        from collections import defaultdict
        partitions: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for rec in records:
            partition_key = self._compute_partition_key(rec, rules)
            partitions[partition_key].append(rec)

        partition_dir = self._data_dir("logs") / "partitions"
        partition_dir.mkdir(parents=True, exist_ok=True)

        written = 0
        partition_list = []
        for key, part_records in partitions.items():
            safe_key = key.replace("/", "_").replace(":", "_")
            out_file = partition_dir / f"{source_key}_{safe_key}.jsonl"
            # 覆盖写：同一批解析数据重复分区时不会把整段再追加一遍。
            self._replace_jsonl(out_file, part_records)
            partition_list.append({"key": key, "count": len(part_records), "file": str(out_file)})
            written += len(part_records)

        self._write_checkpoint(f"partition_{source_key}", {
            "total_records": len(records), "partitions": len(partitions),
            "written": written, "completed_at": time.time(),
        })

        return {
            "source_key": source_key, "total_records": len(records),
            "partitions": len(partitions), "written": written, "details": partition_list,
        }

    def _compute_partition_key(self, rec: dict[str, Any], rules: list[dict[str, Any]]) -> str:
        if not rules:
            ts = rec.get("collected_at", time.time())
            if isinstance(ts, (int, float)):
                return time.strftime("%Y%m%d", time.localtime(ts))
            return "default"

        rule = rules[0]
        key_field = rule.get("分区键", "")
        granularity = rule.get("分区粒度", "day")

        if key_field and key_field not in ("", "日志分区样例1"):
            val = rec.get(key_field) or rec.get("fields", {}).get(key_field, "default")
            return str(val)

        ts = rec.get("collected_at", time.time())
        if isinstance(ts, (int, float)):
            if granularity == "hour":
                return time.strftime("%Y%m%d_%H", time.localtime(ts))
            return time.strftime("%Y%m%d", time.localtime(ts))
        return "default"

    def merge_partitions(self, source_key: str) -> dict[str, Any]:
        """将分区数据归并为一个数据集。"""
        partition_dir = self._data_dir("logs") / "partitions"
        if not partition_dir.exists():
            return {"merged": 0}

        all_records = []
        for f in sorted(partition_dir.glob(f"{source_key}_*.jsonl")):
            all_records.extend(self._read_jsonl(f))

        merged_file = self._data_dir("logs") / f"{source_key}_merged.jsonl"
        written = self._write_jsonl(merged_file, all_records)
        return {"source_key": source_key, "merged": written, "files_consumed": len(list(partition_dir.glob(f"{source_key}_*.jsonl")))}