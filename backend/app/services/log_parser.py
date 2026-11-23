"""日志解析：按规则解析原始日志行，提取结构化字段。"""
from __future__ import annotations

import json
import re
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LogParserService(BaseModuleService):
    MODULE = "log_parser"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则编号", "关联源", "正则表达式", "字段提取", "时间格式", "日志级别", "多行合并", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联源", "正则表达式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def parse_log_file(self, source_key: str) -> dict[str, Any]:
        """解析指定日志源已采集的日志文件。"""
        log_file = self._data_dir("logs") / f"{source_key}.jsonl"
        if not log_file.exists():
            return {"error": f"日志源 {source_key} 无已采集数据", "parsed": 0}

        parsers = [p for p in store.rows(self.MODULE) if p.get("status") == "已生效"]
        records = self._read_jsonl(log_file)

        parsed = []
        errors = []
        for rec in records:
            parsed_entry = self._apply_parsers(rec, parsers)
            if parsed_entry.get("parse_error"):
                errors.append(parsed_entry)
            else:
                parsed.append(parsed_entry)

        output_file = self._data_dir("logs") / f"{source_key}_parsed.jsonl"
        # 覆盖写：重复触发解析不应把同一段日志再追加一遍。
        written = self._replace_jsonl(output_file, parsed)

        if errors:
            self._replace_jsonl(
                self._data_dir("logs") / f"{source_key}_parse_errors.jsonl",
                errors
            )

        self._write_checkpoint(f"{source_key}_parse", {
            "total": len(records),
            "parsed": written,
            "errors": len(errors),
            "completed_at": time.time(),
        })

        return {
            "source_key": source_key,
            "total": len(records),
            "parsed": written,
            "errors": len(errors),
            "output": str(output_file),
        }

    def _apply_parsers(self, rec: dict[str, Any], parsers: list[dict[str, Any]]) -> dict[str, Any]:
        raw = rec.get("raw", "")
        result = dict(rec)
        result["fields"] = {}
        result["parse_error"] = None

        if not parsers:
            # 无规则时使用默认解析
            m = re.match(r"(\S+) (\S+) \[(\w+)\] (\S+) (\S+) (.+)", raw)
            if m:
                result["fields"] = {
                    "timestamp": m.group(1),
                    "host": m.group(2),
                    "source": m.group(3),
                    "level": m.group(4),
                    "module": m.group(5),
                    "message": m.group(6),
                }
            else:
                result["parse_error"] = "默认正则未匹配"
            return result

        for parser in parsers:
            try:
                pattern = parser.get("正则表达式", "")
                if pattern and pattern not in ("", "日志解析样例1"):
                    m = re.search(pattern, raw)
                    if m:
                        result["fields"] = m.groupdict() if m.groupdict() else {str(i): g for i, g in enumerate(m.groups())}
                        result["parser_rule"] = parser["规则编号"]
                        return result
            except re.error:
                continue

        # 默认解析兜底
        result["fields"]["raw_message"] = raw
        result["fields"]["collected_at"] = rec.get("collected_at")
        return result

    def match_source_to_parser(self, source_id: int) -> list[dict[str, Any]]:
        """查找匹配指定日志源的解析规则。"""
        source_code = f"LOG_-{source_id:04d}"
        matches = []
        for p in store.rows(self.MODULE):
            if p.get("status") == "已生效" and (p.get("关联源") == source_code or p.get("关联源") == str(source_id)):
                matches.append(p)
        return matches