from __future__ import annotations

import csv
import os

import datetime as dt
from pathlib import Path
from typing import Dict, Any, List

def _debug_env():
    print("[PY-ENV] COVERAGE_STATES   =", os.getenv("COVERAGE_STATES"))
    print("[PY-ENV] COVERAGE_STATUS   =", os.getenv("COVERAGE_STATUS"))
    print("[PY-ENV] COVERAGE_CONTRACTORS =", os.getenv("COVERAGE_CONTRACTORS"))
    print("[PY-ENV] COVERAGE_MAX_DOCS =", os.getenv("COVERAGE_MAX_DOCS"))
    print("[PY-ENV] COVERAGE_TIMEOUT  =", os.getenv("COVERAGE_TIMEOUT"))


from scripts.coverage_api import (
    list_final_lcds,
    list_articles,
    get_article_icd10_covered,
    get_article_icd10_noncovered,
    get_article_hcpc_codes,
    get_article_hcpc_modifiers,
    get_article_revenue_codes,
    get_article_bill_types,
)
from scripts.normalize import norm_doc_row, norm_article_code_row
from scripts.diff_changes import compute_code_changes

# ---------- Paths ----------
ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "dataset"
DATASET_DIR.mkdir(parents=True, exist_ok=True)

DOCS_FILE = DATASET_DIR / "documents_latest.csv"
CODES_FILE = DATASET_DIR / "document_codes_latest.csv"
CHANGES_FILE = DATASET_DIR / f"changes_{dt.date.today().strftime('%Y-%m')}.csv"


def _article_id_from_stub(stub: Dict[str, Any]) -> str | None:
    return (
        stub.get("article_id")
        or stub.get("articleId")
        or stub.get("id")
        or stub.get("document_id")
        or stub.get("doc_id")
        or stub.get("mcd_id")
        or stub.get("mcdId")
        or stub.get("articleNumber")
    )


def _read_prev_codes(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({
                "article_id": row.get("article_id", ""),
                "code_system": row.get("code_system", ""),
                "code": row.get("code", ""),
                "coverage_flag": row.get("coverage_flag", ""),
            })
    return rows


def main():
    _debug_env()  # <-- prints what Python actually sees
    run_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")

    # Step 1: Discover documents
    lcds = list_final_lcds(status="all")
    arts = list_articles(status="all")
    docs = lcds + arts
    print(f"[DEBUG] discovered {len(lcds)} LCDs, {len(arts)} Articles (total {len(docs)})")

    # Save documents with source_url (via normalize.py)
    if docs:
        # Establish header from first normalized row
        sample = norm_doc_row(docs[0])
        with open(DOCS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(sample.keys()))
            writer.writeheader()
            for d in docs:
                writer.writerow(norm_doc_row(d))

    # Step 2: Pull codes from Articles
    rows: List[Dict[str, str]] = []
    print(f"[DEBUG] articles discovered: {len(arts)}")
    for stub in arts:
        aid = _article_id_from_stub(stub)
        if not aid:
            print(f"[WARN] Article stub without obvious id keys; keys={list(stub.keys())[:8]}")
            continue

        for row in get_article_icd10_covered(aid):
            r = norm_article_code_row(aid, row)
            r["coverage_flag"] = "covered"
            r["code_system"] = "ICD10-CM"
            rows.append(r)
        for row in get_article_icd10_noncovered(aid):
            r = norm_article_code_row(aid, row)
            r["coverage_flag"] = "noncovered"
            r["code_system"] = "ICD10-CM"
            rows.append(r)
        for row in get_article_hcpc_codes(aid):
            r = norm_article_code_row(aid, row)
            r["code_system"] = "HCPCS/CPT"
            rows.append(r)
        for row in get_article_revenue_codes(aid):
            r = norm_article_code_row(aid, row)
            r["code_system"] = "Revenue"
            rows.append(r)
        for row in get_article_bill_types(aid):
            r = norm_article_code_row(aid, row)
            r["code_system"] = "Bill Type"
            rows.append(r)
        for row in get_article_hcpc_modifiers(aid):
            r = norm_article_code_row(aid, row)
            r["code_system"] = "HCPCS Modifier"
            rows.append(r)

    # Write current codes
    if rows:
        with open(CODES_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
    else:
        # Ensure file exists even if empty (helps downstream)
        with open(CODES_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["article_id", "code", "description", "coverage_flag", "code_system"])
            writer.writeheader()

    # Step 3: Change tracking (compare with previous committed dataset if present)
    prev_rows = _read_prev_codes(CODES_FILE)  # read BEFORE it was overwritten on prior commit (from last run)
    # NOTE: In Actions, the repo is fresh each run and CODES_FILE reflects last committed dataset when we get here.
    # After we write the new CODES_FILE above, prev_rows holds the "before" snapshot from last run.

    changes = compute_code_changes(prev_rows=prev_rows, curr_rows=rows)

    with open(CHANGES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["change_type", "article_id", "code_system", "code", "prev_flag", "curr_flag"])
        writer.writeheader()
        for ch in changes:
            writer.writerow(ch)

    print(
        {
            "run_time": run_time,
            "docs": len(docs),
            "articles": len(arts),
            "codes": len(rows),
            "changes": len(changes),
        }
    )


if __name__ == "__main__":
    main()
