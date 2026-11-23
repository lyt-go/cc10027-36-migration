"""字段映射：将解析后的字段按映射规则转换为标准化输出字段。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService, DATA_ROOT
from app.store import store


class FieldMappingService(BaseModuleService):
    MODULE = "field_mapping"
    ENTITY = "映射规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源字段", "目标字段", "转换函数", "默认值", "冲突策略", "映射优先级", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "源字段", "目标字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def apply_mappings(self, source_key: str) -> dict[str, Any]:
        """对已解析的日志文件应用字段映射规则。"""
        parsed_file = self._data_dir("logs") / f"{source_key}_parsed.jsonl"
        if not parsed_file.exists():
            return {"error": f"日志源 {source_key} 未解析，请先执行解析", "mapped": 0}

        rules = [m for m in store.rows(self.MODULE) if m.get("status") == "已生效"]
        records = self._read_jsonl(parsed_file)

        mapped = []
        for rec in records:
            mapped_entry = self._transform_record(rec, rules)
            mapped.append(mapped_entry)

        output_file = self._data_dir("logs") / f"{source_key}_mapped.jsonl"
        written = self._replace_jsonl(output_file, mapped)

        self._write_checkpoint(f"{source_key}_map", {
            "total": len(records),
            "mapped": written,
            "rules_applied": len(rules),
            "completed_at": time.time(),
        })

        return {"source_key": source_key, "total": len(records), "mapped": written, "rules": len(rules)}

    def _transform_record(self, rec: dict[str, Any], rules: list[dict[str, Any]]) -> dict[str, Any]:
        result = {
            "source_key": rec.get("source_key"),
            "line_no": rec.get("line_no"),
            "collected_at": rec.get("collected_at"),
            "standardized": {},
        }

        fields = rec.get("fields", rec)
        for rule in rules:
            src = rule.get("源字段", "")
            dst = rule.get("目标字段", "")
            fallback = rule.get("默认值", "")
            priority = rule.get("映射优先级", "medium")
            conflict = rule.get("冲突策略", "skip")

            val = None
            if src in fields:
                val = fields[src]
            else:
                val = fallback if fallback not in ("", "字段映射样例1") else None

            if val is not None:
                if dst in result["standardized"] and conflict == "skip":
                    continue
                result["standardized"][dst] = val
                result["standardized"][f"_src_{dst}"] = rule["规则编号"]

        # 保留上游来源
        result["parsed_fields"] = {k: v for k, v in fields.items() if not k.startswith("_")}
        return result

    def get_mapping_conflicts(self) -> list[dict[str, Any]]:
        """检测同目标字段存在多个映射规则的冲突。"""
        by_target: dict[str, list[dict[str, Any]]] = {}
        for m in store.rows(self.MODULE):
            if m.get("status") == "已生效":
                dst = m.get("目标字段", "")
                by_target.setdefault(dst, []).append(m)
        return [{"target": k, "rules": v, "count": len(v)} for k, v in by_target.items() if len(v) > 1]