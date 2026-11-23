"""格式输出：将标准化数据按输出格式配置写入指定路径。"""
from __future__ import annotations

import json
import time
import gzip
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class FormatOutputService(BaseModuleService):
    MODULE = "format_output"
    ENTITY = "输出配置"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["配置编号", "关联源", "输出格式", "输出路径", "压缩方式", "分片大小", "命名规则", "配置版本"]
    REQUIRED_FIELDS = ["配置编号", "关联源", "输出格式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def export_source(self, source_key: str, format_type: str = "jsonl") -> dict[str, Any]:
        """将指定日志源的标准化数据导出为指定格式。"""
        mapped_file = self._data_dir("logs") / f"{source_key}_mapped.jsonl"
        if not mapped_file.exists():
            return {"error": f"日志源 {source_key} 无映射数据，请先执行字段映射", "exported": 0}

        records = self._read_jsonl(mapped_file)
        exports_dir = self._data_dir("exports")

        if format_type == "jsonl":
            output_file = exports_dir / f"{source_key}_export.jsonl"
            written = self._replace_jsonl(output_file, records)

        elif format_type == "json":
            output_file = exports_dir / f"{source_key}_export.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2, default=str)
            written = len(records)

        elif format_type == "gz":
            output_file = exports_dir / f"{source_key}_export.jsonl.gz"
            with gzip.open(output_file, "wt", encoding="utf-8", compresslevel=6) as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            written = len(records)

        elif format_type == "csv":
            output_file = exports_dir / f"{source_key}_export.csv"
            if records:
                keys = list(records[0].get("standardized", records[0]).keys())
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(",".join(keys) + "\n")
                    for rec in records:
                        std = rec.get("standardized", rec)
                        f.write(",".join(str(std.get(k, "")) for k in keys) + "\n")
                written = len(records)
            else:
                written = 0
        else:
            return {"error": f"不支持的输出格式: {format_type}"}

        self._write_checkpoint(f"{source_key}_export", {
            "format": format_type,
            "records": written,
            "output_file": str(output_file),
            "exported_at": time.time(),
        })

        return {
            "source_key": source_key,
            "format": format_type,
            "records": written,
            "output_file": str(output_file),
        }

    def export_batch(self, source_keys: list[str], format_type: str = "jsonl") -> dict[str, Any]:
        """批量导出多个日志源的数据。"""
        results = []
        total = 0
        for key in source_keys:
            r = self.export_source(key, format_type)
            results.append(r)
            total += r.get("records", 0)
        return {"total": total, "sources": len(results), "details": results}

    def get_export_manifest(self, source_key: str) -> dict[str, Any]:
        """获取指定日志源的导出清单。"""
        cp = self._read_checkpoint(f"{source_key}_export")
        mapped_cp = self._read_checkpoint(f"{source_key}_map")
        parsed_cp = self._read_checkpoint(f"{source_key}_parse")
        collect_cp = self._read_checkpoint(source_key)

        return {
            "source_key": source_key,
            "collect": collect_cp,
            "parse": parsed_cp,
            "mapping": mapped_cp,
            "export": cp,
        }