from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path
from typing import Dict, Any

from scripts.coverage_api import (
    list_final_lcds,
    list_articles,
    get_lcd,
    get_lcd_revision_history,
    get_article,
    get_article_revision_history,
    get_article_icd10_covered,
    get_article_icd10_noncovered,
    get_article_hcpc_codes,
    get_article_hcpc_modifiers,
    get_article_revenue_codes,
    get_article_bill_types,
)

# ---------- Paths ----------
DATASET_DIR = Path(__file__).resolve().parent.parent / "dataset"
DATASET_DIR.mkdir(parents=True, exist_ok=True)

DOCS_FILE = DATASET_DIR / "documents_latest.csv"
CODES_FILE = DATASET_DIR / "document_codes_latest.csv"
CHANGES_FILE = DATASET_DIR / f"changes_{dt.date.today().strftime('%Y-%m')}.csv"


# ---------- Normalizers ----------
def norm_doc_row(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": doc.get("lcd_id")
        or doc.get("article_id")
        or doc.get("articleId")
        or doc.get("id")
        or doc.get("document_id")
        or doc.get("mcd_id"),
        "type": doc.get("document_type") or doc.get("type") or "",
        "title": doc.get("title") or doc.get("lcd_title") or doc.get("article_title") or "",
        "contractor": doc.get("contractor") or doc.get("contractorName") or "",
        "state": doc.get("state") or "",
        "status": doc.get("lcdStatus") or doc.get("articleStatus") or doc.get("status") or "",
    }


def norm_article_code_row(article_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "article_id": article_id,
        "code": row.get("code") or row.get("Code") or "",
        "description": row.get("description") or row.get("Description") or "",
        "coverage_flag": row.get("coverage_flag") or "",
        "code_system": row.get("code_system") or "",
    }


# ---------- Main ----------
def main():
    run_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")

    # Step 1: Discover documents
    lcds = list_final_lcds(status="all")
    arts = list_articles(status="all")
    docs = lcds + arts
    print(f"[DEBUG] discovered {len(lcds)} LCDs, {len(arts)} Articles (total {len(docs)})")

    # Save docs
    if docs:
        with open(DOCS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(norm_doc_row(docs[0]).keys()))
            writer.writeheader()
            for d in docs:
                writer.writerow(norm_doc_row(d))

    # Step 2: Pull codes from Articles
    rows = []
    print(f"[DEBUG] articles discovered: {len(arts)}")
    for stub in arts:
        aid = (
            stub.get("article_id")
            or stub.get("articleId")
            or stub.get("id")
            or stub.get("document_id")
            or stub.get("doc_id")
            or stub.get("mcd_id")
            or stub.get("mcdId")
            or stub.get("articleNumber")
        )
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

    if rows:
        with open(CODES_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    # Step 3: Changes (currently placeholder: no diff logic yet)
    with open(CHANGES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["change_type", "id", "notes"])
        # would be filled by diffing logic

    print(
        {
            "run_time": run_time,
            "docs": len(docs),
            "articles": len(arts),
            "codes": len(rows),
            "changes": 0,
        }
    )


if __name__ == "__main__":
    main()
