from __future__ import annotations

from typing import List, Optional, Dict, Any
import requests
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

BASE_URL = "https://api.coverage.cms.gov"

# -----------------------
# Helpers
# -----------------------
def _status_code(status: Optional[str]) -> str:
    if not status:
        return "all"
    s = str(status).strip().lower()
    return {
        "active": "A", "a": "A",
        "retired": "R", "r": "R",
        "future": "F", "f": "F",
        "future effective": "F",
        "all": "all",
    }.get(s, "all")

def _join(vals: Optional[List[str]]) -> Optional[str]:
    return ",".join(vals) if vals else None

def _unwrap(payload: Any) -> List[dict]:
    """
    Most endpoints return {"meta": {...}, "data": [...]}
    Some examples show "items"/"results". Just give back a list.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "results"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
    return []

@retry(stop=stop_after_attempt(4), wait=wait_exponential_jitter(initial=1, max=10))
def _get(path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> dict:
    url = f"{BASE_URL}{path}"
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

# -----------------------
# Report discovery
# -----------------------
def list_final_lcds(
    states: Optional[List[str]] = None,
    status: str = "all",
    contractors: Optional[List[str]] = None,
    timeout: int = 30,
) -> List[dict]:
    params: Dict[str, Any] = {}
    if _join(states):       params["state"] = _join(states)
    if _join(contractors):  params["contractor"] = _join(contractors)
    sc = _status_code(status)
    if sc != "all":
        params["lcdStatus"] = sc
    resp = _get("/v1/reports/local-coverage-final-lcds", params, timeout)
    return _unwrap(resp)

def list_articles(
    states: Optional[List[str]] = None,
    status: str = "all",
    contractors: Optional[List[str]] = None,
    timeout: int = 30,
) -> List[dict]:
    params: Dict[str, Any] = {}
    if _join(states):       params["state"] = _join(states)
    if _join(contractors):  params["contractor"] = _join(contractors)
    sc = _status_code(status)
    if sc != "all":
        params["articleStatus"] = sc
    resp = _get("/v1/reports/local-coverage-articles", params, timeout)
    return _unwrap(resp)

# -----------------------
# Detail endpoints
# -----------------------
def get_lcd(lcd_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/lcd", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)

def get_lcd_revision_history(lcd_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/lcd/revision-history", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)
    return _unwrap(resp)

def _get_article_detail_any(
    *,
    article_id: str | None = None,
    document_id: str | None = None,
    document_display_id: str | None = None,
    timeout: int = 30,
) -> dict:
    """
    Some report rows don’t expose articleId. This helper asks the article detail
    endpoint with *any* identifier the row might have. We pass multiple params
    (the API will ignore unknown/empty ones).
    """
    params: Dict[str, Any] = {}
    if article_id:          params["article_id"] = article_id; params["articleId"] = article_id
    if document_id:         params["document_id"] = document_id
    if document_display_id: params["document_display_id"] = document_display_id; params["documentDisplayId"] = document_display_id
    return _get("/v1/data/article", params, timeout)

def resolve_article_id_from_stub(stub: Dict[str, Any], *, timeout: int = 30) -> str | None:
    """
    Try common keys first; if missing, hit the detail endpoint using document ids
    to retrieve canonical articleId from the returned payload.
    """
    # Direct keys that sometimes exist
    for k in ("article_id", "articleId", "id", "mcd_id", "mcdId", "articleNumber"):
        v = stub.get(k)
        if v:
            return str(v)

    # If we only have document info, ask detail API to learn the articleId
    doc_id  = stub.get("document_id")
    doc_disp = stub.get("document_display_id")

    if doc_id or doc_disp:
        detail = _get_article_detail_any(
            article_id=None,
            document_id=str(doc_id) if doc_id else None,
            document_display_id=str(doc_disp) if doc_disp else None,
            timeout=timeout,
        )
        rows = _unwrap(detail)
        if rows:
            # articleId could be in row["articleId"] or row["id"], etc.
            row = rows[0]
            for k in ("article_id", "articleId", "id", "mcd_id", "mcdId", "articleNumber"):
                v = row.get(k)
                if v:
                    return str(v)

    return None

# -----------------------
# Article code tables
# -----------------------
def get_article(article_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/article", {"article_id": article_id, "articleId": article_id}, timeout)

def get_article_revision_history(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/revision-history", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_article_codes_table(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/code-table", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_article_icd10_covered(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/icd10-covered", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_article_icd10_noncovered(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/icd10-noncovered", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_article_hcpc_codes(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/hcpc-code", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_article_hcpc_modifiers(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/hcpc-modifier", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_article_revenue_codes(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/revenue-code", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_article_bill_types(article_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/article/bill-codes", {"article_id": article_id, "articleId": article_id}, timeout)
    return _unwrap(resp)

def get_update_period(timeout: int = 30) -> dict:
    return _get("/v1/metadata/update-period/", None, timeout)
