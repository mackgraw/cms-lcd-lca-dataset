from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Tuple

from scripts.coverage_api import (
    _env,
    _debug,
    ensure_license_acceptance,
    list_final_lcds,
    list_articles,
    discover_ids_from_report_row,
    get_codes_table_any,
    get_icd10_covered_any,
    get_icd10_noncovered_any,
    get_hcpc_codes_any,
    get_hcpc_modifiers_any,
    get_revenue_codes_any,
    get_bill_codes_any,
)

def _chunk(items: List[Any], nshards: int, shard_index: int) -> List[Any]:
    n = len(items)
    if nshards <= 1:
        return items
    size = math.ceil(n / nshards)
    start = shard_index * size
    end = min(n, start + size)
    return items[start:end]

def main() -> None:
    STATES = _env("COVERAGE_STATES")
    STATUS = _env("COVERAGE_STATUS")
    CONTRACTORS = _env("COVERAGE_CONTRACTORS")
    MAX_DOCS = int(_env("COVERAGE_MAX_DOCS") or "0")
    TIMEOUT = int(_env("COVERAGE_TIMEOUT") or "30")
    SHARDS = int(_env("COVERAGE_SHARDS") or "1")
    SHARD_INDEX = int(_env("COVERAGE_SHARD_INDEX") or "0")

    ensure_license_acceptance(timeout=TIMEOUT)

    lcds = list_final_lcds(states=STATES, status=STATUS, contractors=CONTRACTORS, timeout=TIMEOUT)
    arts = list_articles(states=STATES, status=STATUS, contractors=CONTRACTORS, timeout=TIMEOUT)
    docs = [("Article", r) for r in arts] + [("LCD", r) for r in lcds]
    if MAX_DOCS > 0:
        docs = docs[:MAX_DOCS]

    shard = _chunk(docs, SHARDS, SHARD_INDEX)
    _debug(f"shard {SHARD_INDEX}/{SHARDS} -> {len(shard)} of {len(docs)} items")

    for i, (doctype, row) in enumerate(shard, start=1):
        ids = discover_ids_from_report_row(row)
        display = row.get("document_display_id")
        _debug(f"[{doctype} {i}/{len(shard)}] {display} ids={ids}")

        if doctype == "Article":
            for fn in (
                get_codes_table_any,
                get_icd10_covered_any,
                get_icd10_noncovered_any,
                get_hcpc_modifiers_any,
                get_revenue_codes_any,
                get_bill_codes_any,
            ):
                fn(ids, timeout=TIMEOUT)
        get_hcpc_codes_any(ids, timeout=TIMEOUT)

if __name__ == "__main__":
    main()
