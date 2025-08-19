from __future__ import annotations

import csv
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

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


OUT_DIR = "dataset"
LOG_DIR = ".harvest_logs"


def _write_csv(path: str, rows: List[Mapping[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        # create an empty file so artifact upload can succeed
        open(path, "w").close()
        return
    header: List[str] = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})


def main() -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

    STATES = _env("COVERAGE_STATES")
    STATUS = _env("COVERAGE_STATUS")
    CONTRACTORS = _env("COVERAGE_CONTRACTORS")
    MAX_DOCS = int(_env("COVERAGE_MAX_DOCS") or "0")
    TIMEOUT = int(_env("COVERAGE_TIMEOUT") or "30")

    # CMS license
    try:
        ensure_license_acceptance(timeout=TIMEOUT)
    except Exception as e:
        print(f"[WARN] ensure_license_acceptance() failed but continuing: {e}")

    # Pull reports
    lcds = list_final_lcds(states=STATES, status=STATUS, contractors=CONTRACTORS, timeout=TIMEOUT)
    arts = list_articles(states=STATES, status=STATUS, contractors=CONTRACTORS, timeout=TIMEOUT)
    _debug(f"discovered {len(lcds)} LCDs, {len(arts)} Articles (total {len(lcds)+len(arts)})")

    # Work list = Articles first (those are where most code tables live), then LCDs
    docs = [("Article", r) for r in arts] + [("LCD", r) for r in lcds]
    if MAX_DOCS > 0:
        docs = docs[:MAX_DOCS]
    _debug(f"processing {len(docs)} documents")

    code_rows: List[Dict[str, Any]] = []
    nocode_rows: List[Dict[str, Any]] = []

    for idx, (doctype, row) in enumerate(docs, start=1):
        display = row.get("document_display_id") or row.get("title") or "?"
        _debug(f"[{doctype} {idx}/{len(docs)}] {display} ids={{{'document_id':row.get('document_id'),'document_display_id':row.get('document_display_id'),'document_version':row.get('document_version')}}}")

        ids = discover_ids_from_report_row(row)

        # Collect results (only call endpoints that actually exist for each doc type)
        agg: List[Dict[str, Any]] = []

        # Article-only endpoints
        if doctype == "Article":
            for fn in (
                get_codes_table_any,
                get_icd10_covered_any,
                get_icd10_noncovered_any,
                get_hcpc_modifiers_any,
                get_revenue_codes_any,
                get_bill_codes_any,
            ):
                try:
                    agg.extend(fn(ids, timeout=TIMEOUT))
                except Exception as e:
                    print(f"[WARN] {fn.__name__} failed: {e}")

        # Shared endpoint (Article + LCD): hcpc-code
        try:
            agg.extend(get_hcpc_codes_any(ids, timeout=TIMEOUT))
        except Exception as e:
            print(f"[WARN] get_hcpc_codes_any failed: {e}")

        if agg:
            for r in agg:
                r["_document_display_id"] = row.get("document_display_id", "")
                r["_document_id"] = row.get("document_id", "")
                r["_document_version"] = row.get("document_version", "")
                r["_type"] = doctype
            code_rows.extend(agg)
        else:
            nocode_rows.append(
                {
                    "document_display_id": row.get("document_display_id", ""),
                    "document_id": row.get("document_id", ""),
                    "document_version": row.get("document_version", ""),
                    "type": doctype,
                    "title": row.get("title", ""),
                }
            )

    # Write outputs
    codes_csv = os.path.join(OUT_DIR, "document_codes_latest.csv")
    nocodes_csv = os.path.join(OUT_DIR, "document_nocodes_latest.csv")
    _write_csv(codes_csv, code_rows)
    _write_csv(nocodes_csv, nocode_rows)

    print(f"[note] Wrote {len(code_rows)} code rows to {codes_csv}")
    print(f"[note] Wrote {len(nocode_rows)} no-code rows to {nocodes_csv}")


if __name__ == "__main__":
    main()
