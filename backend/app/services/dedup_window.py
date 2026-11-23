"""窗口去重：基于滑动窗口对日志记录进行去重处理。"""
from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DedupWindowService(BaseModuleService):
    MODULE = "dedup_window"
    ENTITY = "去重窗口"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["窗口编号", "关联源", "去重键", "窗口大小", "滑动间隔", "指纹算法", "冲突策略", "窗口状态"]
    REQUIRED_FIELDS = ["窗口编号", "关联源", "去重键"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用窗口": "已停用"}
    NEGATIVE_ACTIONS = ["停用窗口"]

    # 窗口内指纹缓存：key → (timestamp, record_id)
    _fingerprints: dict[str, OrderedDict] = {}

    def deduplicate_source(self, source_key: str) -> dict[str, Any]:
        """对指定日志源的已采集数据执行窗口去重。"""
        log_file = self._data_dir("logs") / f"{source_key}.jsonl"
        if not log_file.exists():
            return {"error": "无采集数据", "deduped": 0}

        records = self._read_jsonl(log_file)
        windows = [w for w in store.rows(self.MODULE) if w.get("关联源") == source_key and w.get("status") == "已生效"]

        window_size = 1000
        dedup_key_field = "raw"
        if windows:
            window_size = int(windows[0].get("窗口大小", "1000"))
            dedup_key_field = windows[0].get("去重键", "raw")

        seen: OrderedDict = self._fingerprints.setdefault(source_key, OrderedDict())
        duplicates = []
        kept = []

        now = time.time()
        for rec in records:
            dedup_key = str(rec.get(dedup_key_field, rec.get("raw", "")))
            fp = self._compute_fingerprint(rec, dedup_key_field)

            # 滑动窗口清理
            while seen and list(seen.values())[0][0] < now - window_size:
                seen.popitem(last=False)

            if fp in seen:
                duplicates.append(rec)
            else:
                seen[fp] = (now, rec.get("line_no", 0))
                kept.append(rec)

        deduped_file = self._data_dir("logs") / f"{source_key}_deduped.jsonl"
        written = self._replace_jsonl(deduped_file, kept)

        if duplicates:
            self._replace_jsonl(
                self._data_dir("logs") / f"{source_key}_duplicates.jsonl",
                duplicates
            )

        self._write_checkpoint(f"dedup_{source_key}", {
            "total": len(records), "kept": written,
            "duplicates": len(duplicates), "dedup_ratio": round(len(duplicates) / max(len(records), 1) * 100, 2),
            "window_size": window_size, "completed_at": time.time(),
        })

        return {
            "source_key": source_key, "total": len(records),
            "kept": written, "duplicates": len(duplicates),
            "dedup_ratio": round(len(duplicates) / max(len(records), 1) * 100, 2),
        }

    def _compute_fingerprint(self, rec: dict[str, Any], key_field: str) -> str:
        val = str(rec.get(key_field, rec.get("raw", "")))
        fields = rec.get("fields", {})
        if key_field == "fields" and fields:
            val = str(sorted(fields.items()))
        return hashlib.md5(val.encode("utf-8")).hexdigest()[:16]

    def clear_window(self, source_key: str) -> dict[str, Any]:
        """清空指定窗口的指纹缓存。"""
        if source_key in self._fingerprints:
            del self._fingerprints[source_key]
        return {"source_key": source_key, "cleared": True}