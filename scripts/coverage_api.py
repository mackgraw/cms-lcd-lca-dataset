from __future__ import annotations
import os
from typing import List, Optional, Dict
import requests
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

BASE_URL = "https://api.coverage.cms.gov"

def build_filters(states: Optional[List[str]] = None,
                  status: Optional[str] = None,
                  contractors: Optional[List[str]] = None) -> Dict[str, str]:
    params: Dict[str, str] = {}
    if states:
        params["state"] = ",".join(states)
    if status and status.lower() != "all":
        params["status"] = status
    if contractors:
        params["contractor"] = ",".join(contractors)
    return params

@retry(stop=stop_after_attempt(4), wait=wait_exponential_jitter(initial=1, max=10))
def _get(path: str, params: Optional[Dict[str, str]] = None, timeout: int = 30) -> dict:
    url = f"{BASE_URL}{path}"
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def list_final_lcds(states: Optional[List[str]] = None,
                    status: Optional[str] = "Active",
                    contractors: Optional[List[str]] = None,
                    timeout: int = 30) -> List[dict]:
    # Reports endpoint uses different status param; to stay compatible,
    # we simply DO NOT send a status filter here unless you later map to A/R/F.
    params: Dict[str, str] = {}
    if states:
        params["state"] = ",".join(states)
    if contractors:
        params["contractor"] = ",".join(contractors)
    # no 'status' param sent; avoids 400
    data = _get("/v1/reports/local-coverage-final-lcds", params, timeout)
    return data if isinstance(data, list) else data.get("items", [])


def list_articles(states: Optional[List[str]] = None,
                  status: Optional[str] = "Active",
                  contractors: Optional[List[str]] = None,
                  timeout: int = 30) -> List[dict]:
    params: Dict[str, str] = {}
    if states:
        params["state"] = ",".join(states)
    if contractors:
        params["contractor"] = ",".join(contractors)
    # no 'status' param sent; avoids 400
    data = _get("/v1/reports/local-coverage-articles", params, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_lcd(lcd_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/lcd", {"lcd_id": lcd_id}, timeout)

def get_lcd_revision_history(lcd_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/lcd/revision-history", {"lcd_id": lcd_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article(article_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/article", {"article_id": article_id}, timeout)

def get_article_revision_history(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/revision-history", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article_codes_table(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/code-table", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article_icd10_covered(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/icd10-covered", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article_icd10_noncovered(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/icd10-noncovered", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article_hcpc_codes(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/hcpc-code", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article_hcpc_modifiers(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/hcpc-modifier", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article_revenue_codes(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/revenue-code", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_article_bill_types(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/bill-codes", {"article_id": article_id}, timeout)
    return data if isinstance(data, list) else data.get("items", [])

def get_update_period(timeout: int = 30) -> dict:
    return _get("/v1/metadata/update-period/", None, timeout)
