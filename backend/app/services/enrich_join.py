"""日志富化：通过维表关联为日志数据补充维度字段。"""
from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class EnrichJoinService(BaseModuleService):
    MODULE = "enrich_join"
    ENTITY = "富化规则"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["规则编号", "目标字段", "维表来源", "关联键", "富化字段", "缺失策略", "缓存TTL", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "目标字段", "维表来源"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # 模拟维表缓存
    _dim_cache: dict[str, dict[str, Any]] = {}
    _cache_updated: dict[str, float] = {}

    def build_dimension_cache(self, rule_id: str) -> dict[str, Any]:
        """根据规则构建维表缓存。"""
        rule = None
        for r in store.rows(self.MODULE):
            if r.get("规则编号") == rule_id:
                rule = r
                break
        if rule is None:
            return {"error": f"富化规则 {rule_id} 不存在"}

        dim_source = rule.get("维表来源", "")
        cache_key = f"dim_{dim_source}"
        # 模拟维表数据：从 log_source 和 agent_heartbeat 中构造维度数据
        dim_data: dict[str, Any] = {
            "source_info": {},
            "agent_info": {},
        }

        for src in store.rows("log_source"):
            dim_data["source_info"][src.get("源编号", "")] = {
                "源名称": src.get("源名称"), "日志类型": src.get("日志类型"),
                "接入方式": src.get("接入方式"), "编码": src.get("字符编码"),
            }

        for agent in store.rows("agent_heartbeat"):
            dim_data["agent_info"][agent.get("代理编号", "")] = {
                "主机": agent.get("部署主机"), "版本": agent.get("代理版本"),
                "状态": agent.get("代理状态"),
            }

        self._dim_cache[cache_key] = dim_data
        self._cache_updated[cache_key] = time.time()
        return {"rule_id": rule_id, "cache_size": len(dim_data), "sources": list(dim_data.keys())}

    def enrich_source(self, source_key: str) -> dict[str, Any]:
        """对指定日志源的已解析数据进行维表富化。"""
        mapped_file = self._data_dir("logs") / f"{source_key}_mapped.jsonl"
        if not mapped_file.exists():
            return {"error": "无映射数据", "enriched": 0}

        records = self._read_jsonl(mapped_file)
        rules = [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]

        # 确保维表缓存存在
        for rule in rules:
            cache_key = f"dim_{rule.get('维表来源', '')}"
            if cache_key not in self._dim_cache:
                self.build_dimension_cache(rule["规则编号"])

        enriched = []
        missing_count = 0
        for rec in records:
            entry = dict(rec)
            entry["enriched_fields"] = {}
            for rule in rules:
                join_key = rule.get("关联键", "")
                cache_key = f"dim_{rule.get('维表来源', '')}"
                dim_data = self._dim_cache.get(cache_key, {})

                enrichment = self._lookup_dimension(join_key, source_key, dim_data)
                if enrichment:
                    entry["enriched_fields"][rule["规则编号"]] = enrichment
                else:
                    missing_strategy = rule.get("缺失策略", "skip")
                    if missing_strategy == "mark":
                        entry["enriched_fields"][rule["规则编号"]] = {"_missing": True}
                        missing_count += 1

            enriched.append(entry)

        output_file = self._data_dir("logs") / f"{source_key}_enriched.jsonl"
        written = self._replace_jsonl(output_file, enriched)

        self._write_checkpoint(f"enrich_{source_key}", {
            "total": len(records), "enriched": written,
            "missing": missing_count, "rules_applied": len(rules),
            "completed_at": time.time(),
        })

        result = {
            "source_key": source_key, "total": len(records), "enriched": written,
            "missing": missing_count, "rules_applied": len(rules),
        }

        # 富化完成后自动触发聚合计算
        aggregate_r = self._trigger_aggregate_for_source(source_key)
        if "error" not in aggregate_r:
            result["auto_aggregate"] = {"status": "triggered", "groups": aggregate_r.get("groups", 0)}

        return result

    def _lookup_dimension(self, join_key: str, source_key: str, dim_data: dict[str, Any]) -> dict[str, Any] | None:
        for source_name, dims in dim_data.items():
            for k, v in dims.items():
                if k == source_key or (isinstance(v, str) and join_key in v):
                    return v
        return None