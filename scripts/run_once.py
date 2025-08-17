from __future__ import annotations

import csv
import os
import datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Optional

from scripts.coverage_api import (
    list_final_lcds,
    list_articles,
    get_article_icd10_covered,
    get_article_icd10_noncovered,
    get_article_hcpc_codes,
    get_article_hcpc_modifiers,
    get_article_revenue_codes,
    get_article_bill_types,
    get_article_codes_table,
)
from scripts.normalize import norm_doc_row, norm_article_code_row
from scripts.diff_changes import compute_code_changes

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "dataset"
DATASET_DIR.mkdir(parents=True, exist_ok=True)

DOCS_FILE = DATASET_DIR / "documents_latest.csv"
CODES_FILE = DATASET_DIR / "document_codes_latest.csv"
CHANGES_FILE = DATASET_DIR / f"changes_{dt.date.today().strftime('%Y-%m')}.csv"


def _debug_env():
    print("[PY-ENV] COVERAGE_STATES   =", os.getenv("COVERAGE_STATES"))
    print("[PY-ENV] COVERAGE_STATUS   =", os.getenv("COVERAGE_STATUS"))
    print("[PY-ENV] COVERAGE_CONTRACTORS =", os.getenv("COVERAGE_CONTRACTORS"))
    print("[PY-ENV] COVERAGE_MAX_DOCS =", os.getenv("COVERAGE_MAX_DOCS"))
    print("[PY-ENV] COVERAGE_TIMEOUT  =", os.getenv("COVERAGE_TIMEOUT"))


def _parse_csv_list(val: Optional[str]) -> Optional[List[str]]:
    if not val:
        return None
    parts = [p.strip() for p in val.split(",")]
    parts = [p for p in parts if p]
    return parts or None


def _env_cfg():
    states = _parse_csv_list((os.getenv("COVERAGE_STATES") or "").strip())
    contractors = _parse_csv_list((os.getenv("COVERAGE_CONTRACTORS") or "").strip())
    status = (os.getenv("COVERAGE_STATUS") or "all").strip()
    try:
        max_docs = int((os.getenv("COVERAGE_MAX_DOCS") or "0").strip())
    except ValueError:
        max_docs = 0
    return states, status, contractors, max_docs


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


def _append_rows(rows: List[Dict[str, str]], article_id: str, payload_rows: List[Dict[str, Any]], *, code_system: Optional[str] = None, coverage_flag: Optional[str] = None):
    for row in payload_rows:
        r = norm_article_code_row(article_id, row)
        if code_system:
            r["code_system"] = code_system
        if coverage_flag:
            r["coverage_flag"] = coverage_flag
        rows.append(r)


def main():
    _debug_env()
    run_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S ")

    states, status, contractors, max_docs = _env_cfg()

    # Discover
    lcds = list_final_lcds(states=states, status=status, contractors=contractors)
    arts = list_articles(states=states, status=status, contractors=contractors)
    print(f"[DEBUG] discovered (pre-limit) LCDs={len(lcds)} Articles={len(arts)}")

    # Enforce MAX immediately
    if max_docs and max_docs > 0:
        lcds = lcds[: max(1, max_docs // 2)]
        arts = arts[: max_docs]
    print(f"[DEBUG] discovered (post-limit) LCDs={len(lcds)} Articles={len(arts)}")

    docs = lcds + arts

    # Save docs
    if docs:
        sample = norm_doc_row(docs[0])
        with open(DOCS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(sample.keys()))
            writer.writeheader()
            for d in docs:
                writer.writerow(norm_doc_row(d))

    prev_rows = _read_prev_codes(CODES_FILE)

    # Fetch codes
    rows: List[Dict[str, str]] = []
    print(f"[DEBUG] articles to process: {len(arts)}")

    # quick sanity probe on the first article
    if arts:
        probe_id = _article_id_from_stub(arts[0])
        if probe_id:
            icd_c = get_article_icd10_covered(probe_id)
            icd_n = get_article_icd10_noncovered(probe_id)
            hcpc = get_article_hcpc_codes(probe_id)
            rev = get_article_revenue_codes(probe_id)
            bill = get_article_bill_types(probe_id)
            mod = get_article_hcpc_modifiers(probe_id)
            table = get_article_codes_table(probe_id)
            print(f"[DEBUG] probe article {probe_id}: covered={len(icd_c)} noncovered={len(icd_n)} hcpc={len(hcpc)} rev={len(rev)} bill={len(bill)} mod={len(mod)} table={len(table)}")

    for stub in arts:
        aid = _article_id_from_stub(stub)
        if not aid:
            print(f"[WARN] Article stub missing id; keys={list(stub.keys())[:8]}")
            continue

        _append_rows(rows, aid, get_article_icd10_covered(aid),    code_system="ICD10-CM",  coverage_flag="covered")
        _append_rows(rows, aid, get_article_icd10_noncovered(aid), code_system="ICD10-CM",  coverage_flag="noncovered")
        _append_rows(rows, aid, get_article_hcpc_codes(aid),       code_system="HCPCS/CPT")
        _append_rows(rows, aid, get_article_revenue_codes(aid),    code_system="Revenue")
        _append_rows(rows, aid, get_article_bill_types(aid),       code_system="Bill Type")
        _append_rows(rows, aid, get_article_hcpc_modifiers(aid),   code_system="HCPCS Modifier")

        # code-table (catch-all)
        table_rows = get_article_codes_table(aid)
        for tr in table_rows:
            r = norm_article_code_row(aid, tr)
            if not r.get("code_system"):
                r["code_system"] = tr.get("codeSystem") or tr.get("table") or tr.get("type") or ""
            if not r.get("coverage_flag"):
                r["coverage_flag"] = tr.get("coverageFlag") or tr.get("coverage") or ""
            rows.append(r)

    # Write codes
    code_headers = ["article_id", "code", "description", "coverage_flag", "code_system"]
    with open(CODES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=code_headers)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in code_headers})

    # Changes
    changes = compute_code_changes(prev_rows=prev_rows, curr_rows=rows)
    with open(CHANGES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["change_type", "article_id", "code_system", "code", "prev_flag", "curr_flag"])
        writer.writeheader()
        for ch in changes:
            writer.writerow(ch)

    print({"run_time": run_time, "docs": len(docs), "articles": len(arts), "codes": len(rows), "changes": len(changes)})


if __name__ == "__main__":
    main()
