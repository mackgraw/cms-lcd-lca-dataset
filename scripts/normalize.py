from __future__ import annotations
from typing import Any, Dict

LCD_URL_TMPL = "https://www.cms.gov/medicare-coverage-database/view/lcd.aspx?LCDId={id}"
ART_URL_TMPL = "https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleId={id}"


def _extract_id(stub: Dict[str, Any]) -> str | None:
    return (
        stub.get("lcd_id")
        or stub.get("LCDId")
        or stub.get("article_id")
        or stub.get("articleId")
        or stub.get("id")
        or stub.get("document_id")
        or stub.get("doc_id")
        or stub.get("mcd_id")
        or stub.get("mcdId")
        or stub.get("articleNumber")
    )


def _detect_doc_type(stub: Dict[str, Any], doc_id: str | None) -> str:
    if stub.get("document_type"):
        return str(stub["document_type"])
    if "lcd_id" in stub or "LCDId" in stub:
        return "LCD"
    if "article_id" in stub or "articleId" in stub:
        return "Article"
    if doc_id and str(doc_id).upper().startswith("L"):
        return "LCD"
    return "Article"


def _build_source_url(doc_type: str, doc_id: str | None) -> str:
    if not doc_id:
        return ""
    if doc_type.upper() == "LCD":
        return LCD_URL_TMPL.format(id=doc_id)
    return ART_URL_TMPL.format(id=doc_id)


def norm_doc_row(stub: Dict[str, Any]) -> Dict[str, Any]:
    doc_id = _extract_id(stub)
    doc_type = _detect_doc_type(stub, doc_id)
    return {
        "doc_id": doc_id or "",
        "doc_type": doc_type,
        "title": stub.get("title") or stub.get("lcd_title") or stub.get("article_title") or "",
        "contractor": stub.get("contractor") or stub.get("contractorName") or "",
        "state": stub.get("state") or "",
        "status": stub.get("lcdStatus") or stub.get("articleStatus") or stub.get("status") or "",
        "source_url": _build_source_url(doc_type, doc_id),
    }


def norm_article_code_row(article_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
    # coverage_flag and code_system may be set by caller for covered/noncovered tables
    return {
        "article_id": str(article_id),
        "code": row.get("code") or row.get("Code") or "",
        "description": row.get("description") or row.get("Description") or "",
        "coverage_flag": row.get("coverage_flag") or "",
        "code_system": row.get("code_system") or "",
    }
