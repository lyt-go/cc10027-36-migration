"""采样策略：管理采样规则，对日志数据进行降采样处理。"""
from __future__ import annotations

import hashlib
import random
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SamplingRuleService(BaseModuleService):
    MODULE = "sampling_rule"
    ENTITY = "采样规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "关联源", "采样方式", "采样比例", "保留条件", "降级策略", "生效时段", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联源", "采样方式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def sample_source(self, source_key: str) -> dict[str, Any]:
        """对指定日志源的已去重数据进行采样。"""
        deduped_file = self._data_dir("logs") / f"{source_key}_deduped.jsonl"
        if not deduped_file.exists():
            log_file = self._data_dir("logs") / f"{source_key}.jsonl"
            if not log_file.exists():
                return {"error": "无可用数据", "sampled": 0}
            records = self._read_jsonl(log_file)
        else:
            records = self._read_jsonl(deduped_file)

        rules = [r for r in store.rows(self.MODULE) if r.get("关联源") == source_key and r.get("status") == "已生效"]

        if rules:
            rule = rules[0]
            sample_method = rule.get("采样方式", "random")
            try:
                sample_rate = float(str(rule.get("采样比例", "0.5")))
            except (ValueError, TypeError):
                sample_rate = 0.5
        else:
            sample_method = "random"
            sample_rate = 0.5

        sampled = self._apply_sampling(records, sample_method, sample_rate, source_key)

        output_file = self._data_dir("logs") / f"{source_key}_sampled.jsonl"
        written = self._replace_jsonl(output_file, sampled)

        self._write_checkpoint(f"sample_{source_key}", {
            "total": len(records), "sampled": written,
            "rate": sample_rate, "method": sample_method,
            "completed_at": time.time(),
        })

        return {
            "source_key": source_key, "total": len(records),
            "sampled": written, "sample_rate": sample_rate,
            "method": sample_method,
        }

    def _apply_sampling(
        self, records: list[dict[str, Any]], method: str, rate: float, source_key: str
    ) -> list[dict[str, Any]]:
        if rate >= 1.0:
            return records
        if rate <= 0:
            return []

        if method == "random":
            return [r for r in records if random.random() < rate]

        if method == "systematic":
            step = max(1, int(1 / rate))
            return [r for i, r in enumerate(records) if i % step == 0]

        if method == "hash":
            sampled = []
            for r in records:
                raw = str(r.get("raw", ""))
                rand_seed = int(hashlib.md5((raw + source_key).encode()).hexdigest()[:8], 16)
                if (rand_seed % 100) < rate * 100:
                    sampled.append(r)
            return sampled

        if method == "head":
            count = max(1, int(len(records) * rate))
            return records[:count]

        return records

    def get_sampling_stats(self, source_key: str) -> dict[str, Any]:
        """获取采样统计。"""
        cp = self._read_checkpoint(f"sample_{source_key}")
        if cp is None:
            return {"source_key": source_key, "status": "未采样"}
        return {"source_key": source_key, **cp}