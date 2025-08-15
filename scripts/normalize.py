from __future__ import annotations
import hashlib
def norm_doc_stub(stub: dict) -> dict:
    doc_id = (stub.get("lcd_id") or stub.get("article_id") or
              stub.get("id") or stub.get("document_id") or stub.get("doc_id"))
    return {
        "doc_id": doc_id,
        "doc_type": "LCD" if (stub.get("lcd_id") or str(doc_id).startswith("L")) else "Article",
        "title": stub.get("title") or stub.get("document_title") or "",
        "contractor": stub.get("contractor") or stub.get("contractor_name") or "",
        "jurisdiction": stub.get("jurisdiction") or stub.get("primary_jurisdiction") or "",
        "state": stub.get("state") or stub.get("state_abbrev") or "",
        "status": stub.get("status") or stub.get("document_status") or "",
        "effective_date": stub.get("effective_date") or stub.get("finalized_on") or stub.get("effectiveOn") or "",
        "last_updated": stub.get("last_updated") or stub.get("lastUpdated") or "",
        "parent_lcd_id": stub.get("lcd_id_ref") or stub.get("reference_lcd_id") or stub.get("parent_lcd_id") or "",
        "source_url": stub.get("url") or stub.get("source_url") or ""
    }

def norm_article_code_row(article_id: str, row: dict) -> dict:
    code = row.get("code") or row.get("hcpc_code") or row.get("icd10_code") or row.get("revenue_code") or row.get("bill_type_code") or ""
    system = row.get("code_system") or row.get("system") or _infer_system(row)
    flag = row.get("coverage_flag") or row.get("covered_flag") or _infer_coverage(row)
    notes = row.get("notes") or row.get("description") or row.get("long_description") or ""
    return {
        "doc_id": article_id,
        "code_system": system,
        "code": code,
        "coverage_flag": flag,
        "notes": notes,
        "from_section": row.get("from_section") or "",
    }

def _infer_system(row: dict) -> str:
    keys = " ".join(row.keys()).lower()
    if "icd10" in keys:
        return "ICD10-CM"
    if "hcpc" in keys or "cpt" in keys:
        return "HCPCS/CPT"
    if "revenue" in keys:
        return "Revenue"
    if "bill" in keys:
        return "Bill Type"
    return "UNKNOWN"

def _infer_coverage(row: dict) -> str:
    v = str(row.get("covered") or row.get("is_covered") or row.get("covered_flag") or "").lower()
    if v in ("true","1","yes","y"):
        return "covered"
    if v in ("false","0","no","n"):
        return "noncovered"
    return row.get("coverage_flag") or "n/a"

def hash_key(*vals: str) -> str:
    s = "||".join([str(v) for v in vals])
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
