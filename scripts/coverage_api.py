from __future__ import annotations

from typing import List, Optional, Dict, Any
import requests
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

# --- add this helper near the top (under imports) ---
def _extract(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("data") or payload.get("items") or []
    return []

BASE_URL = "https://api.coverage.cms.gov"

def _status_code(status: Optional[str]) -> str:
    if not status:
        return "all"
    s = str(status).strip().lower()
    return {
        "active": "A",
        "a": "A",
        "retired": "R",
        "r": "R",
        "future": "F",
        "f": "F",
        "future effective": "F",
        "all": "all",
    }.get(s, "all")

def _join(vals: Optional[List[str]]) -> Optional[str]:
    return ",".join(vals) if vals else None

def _unwrap(payload: Any) -> List[dict]:
    """
    CMS coverage endpoints commonly return:
      { "meta": {...}, "data": [...] }   <-- most endpoints
    Some older examples (or docs) show "items".
    This helper safely returns a list from any of:
      list, dict['data'], dict['items'], dict['results'].
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

def get_lcd(lcd_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/lcd", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)

def get_lcd_revision_history(lcd_id: str, timeout: int = 30) -> List[dict]:
    resp = _get("/v1/data/lcd/revision-history", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)
    return _unwrap(resp)

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

def get_article(article_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/article", {"article_id": article_id}, timeout)

def get_article_revision_history(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/revision-history", {"article_id": article_id}, timeout))

def get_article_codes_table(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/code-table", {"article_id": article_id}, timeout))

def get_article_icd10_covered(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/icd10-covered", {"article_id": article_id}, timeout))

def get_article_icd10_noncovered(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/icd10-noncovered", {"article_id": article_id}, timeout))

def get_article_hcpc_codes(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/hcpc-code", {"article_id": article_id}, timeout))

def get_article_hcpc_modifiers(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/hcpc-modifier", {"article_id": article_id}, timeout))

def get_article_revenue_codes(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/revenue-code", {"article_id": article_id}, timeout))

def get_article_bill_types(article_id: str, timeout: int = 30) -> List[dict]:
    return _extract(_get("/v1/data/article/bill-codes", {"article_id": article_id}, timeout))
