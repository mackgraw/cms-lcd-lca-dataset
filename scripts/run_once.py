# scripts/run_once.py
from __future__ import annotations

import csv
import os
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from scripts.coverage_api import (
    _debug,
    ensure_license_acceptance,   # <-- add this line
    list_final_lcds,
    list_articles,
    get_codes_table_any,
    get_icd10_covered_any,
    get_icd10_noncovered_any,
    get_hcpc_codes_any,
    get_hcpc_modifiers_any,
    get_revenue_codes_any,
    get_bill_types_any,
)


OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dataset")
OUT_CSV = os.path.join(OUT_DIR, "document_codes_latest.csv")

def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name, "")
    print(f"[PY-ENV] {name:<16}= {v}", flush=True)
    return v or default

def _mk_ids(row: Mapping[str, Any]) -> Dict[str, Any]:
    # reports rows contain id and display_id fields; title/name may vary
    return {
        "article_id": row.get("article_id"),
        "document_id": row.get("id") or row.get("document_id"),
        "document_display_id": row.get("display_id") or row.get("document_display_id"),
    }

def _mk_label(row: Mapping[str, Any]) -> str:
    title = row.get("title") or row.get("name") or row.get("display_name") or ""
    disp  = row.get("display_id") or row.get("document_display_id") or ""
    rid   = row.get("article_id") or row.get("id") or row.get("document_id") or ""
    return f"{title or '?'}  id={rid}  display={disp}"

def _collect_for_ids(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    def tag_and_extend(endpoint: str, got: List[Dict[str, Any]]):
        for r in got:
            r = dict(r)
            r["_endpoint"] = endpoint
            rows.append(r)

    # Order: table, icd10 (covered/non), then hcpc/revenue/bill types
    # Each function already handles LCD vs Article and swallows 400s safely.
    tag_and_extend("code-table",         get_codes_table_any(ids, timeout))
    tag_and_extend("icd10-covered",      get_icd10_covered_any(ids, timeout))
    tag_and_extend("icd10-noncovered",   get_icd10_noncovered_any(ids, timeout))
    tag_and_extend("hcpc-code",          get_hcpc_codes_any(ids, timeout))
    tag_and_extend("hcpc-modifier",      get_hcpc_modifiers_any(ids, timeout))
    tag_and_extend("revenue-code",       get_revenue_codes_any(ids, timeout))
    tag_and_extend("bill-codes",         get_bill_types_any(ids, timeout))

    return rows

def _write_csv(rows: List[Dict[str, Any]]) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    # Collect all keys to make a stable header
    header: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                header.append(k)
    if not header:
        header = ["_endpoint"]  # minimal header

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main() -> None:
    # Read env
    STATES      = _env("COVERAGE_STATES")
    STATUS      = _env("COVERAGE_STATUS")
    CONTRACTORS = _env("COVERAGE_CONTRACTORS")
    MAX_DOCS    = int(_env("COVERAGE_MAX_DOCS") or "0")
    TIMEOUT     = int(_env("COVERAGE_TIMEOUT")  or "30")

    try:
        ensure_license_acceptance()
    except NameError:
        print("[WARN] ensure_license_acceptance() not available, skipping.")


    # Discover
    lcds = list_final_lcds(STATES, STATUS, CONTRACTORS, timeout=TIMEOUT)
    arts = list_articles(STATES, STATUS, CONTRACTORS, timeout=TIMEOUT)
    _debug(f"[DEBUG] discovered {len(lcds)} LCDs, {len(arts)} Articles (total {len(lcds)+len(arts)})")

    # Prefer processing Articles first (LCD tables often 400 or empty)
    work: List[Tuple[str, Dict[str, Any]]] = []
    for a in arts:
        work.append(("Article", a))
    for l in lcds:
        work.append(("LCD", l))

    if MAX_DOCS and len(work) > MAX_DOCS:
        work = work[:MAX_DOCS]

    all_rows: List[Dict[str, Any]] = []
    processed = 0
    for idx, (kind, row) in enumerate(work, start=1):
        label = _mk_label(row)
        _debug(f"[DEBUG] [{kind} {idx}/{len(work)}] {label}")
        ids = _mk_ids(row)

        try:
            got = _collect_for_ids(ids, TIMEOUT)
        except Exception as e:
            _debug(f"[DEBUG]   -> collect errored: {e} (continue)")
            got = []

        if got:
            # add basic document metadata to each row
            for r in got:
                r["_document_display_id"] = ids.get("document_display_id")
                r["_document_id"]        = ids.get("document_id")
                r["_kind"]               = kind
                r["_title"]              = row.get("title") or row.get("name") or ""
            _debug(f"        -> aggregated rows: {len(got)}")
            all_rows.extend(got)
        else:
            _debug(f"        -> aggregated rows: 0")
        processed += 1

    _write_csv(all_rows)
    print(f"[SUMMARY] processed items: {processed}, total rows written: {len(all_rows)}")
    print(f"[SUMMARY] CSV: {OUT_CSV}")

if __name__ == "__main__":
    main()
