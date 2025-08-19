# scripts/coverage_api.py
from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional, List

import requests


BASE_URL = "https://api.coverage.cms.gov/v1"


# ---------------------- util ----------------------

def _debug(msg: str) -> None:
    print(msg, flush=True)


def _headers() -> Dict[str, str]:
    # keep it simple + be friendly on server logs
    return {"User-Agent": "cms-lcd-lca-starter/harvester"}


def _safe_get(path: str, params: Optional[Mapping[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    try:
        r = requests.get(url, params=params or {}, headers=_headers(), timeout=timeout)
        r.raise_for_status()
        try:
            j = r.json()
        except Exception:
            j = {}
        keys = list(j.keys()) if isinstance(j, dict) else []
        _debug(f"[DEBUG] GET {url} -> {r.status_code}; keys={keys}")
        return j if isinstance(j, dict) else {}
    except Exception as e:
        # include first 120 chars of the body if present
        body = ""
        try:
            body = (r.text or "")[:120]  # type: ignore[name-defined]
        except Exception:
            body = str(e)
        _debug(f"[ERROR] GET {url} params={dict(params or {})} -> {body}")
        raise


# ---------------------- discovery ----------------------

def ensure_license_acceptance(timeout: int = 30) -> None:
    _debug(f"[DEBUG] GET {BASE_URL}/metadata/license-agreement -> ...")
    j = _safe_get("/metadata/license-agreement", timeout=timeout)
    # If a token ever appears here, you could set COVERAGE_LICENSE_TOKEN = token (not required per Swagger)
    if j:
        _debug("[note] CMS license agreement acknowledged (no token provided).")


def list_final_lcds(states: str, status: str, contractors: str, timeout: int = 30) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if states:      params["states"] = states
    if status:      params["status"] = status
    if contractors: params["contractors"] = contractors
    _debug("[DEBUG] trying /reports/local-coverage-final-lcds" + ("" if status else " (no status)"))
    j = _safe_get("/reports/local-coverage-final-lcds", params=params, timeout=timeout)
    return list((j.get("data") or []))  # type: ignore[return-value]


def list_articles(states: str, status: str, contractors: str, timeout: int = 30) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if states:      params["states"] = states
    if status:      params["status"] = status
    if contractors: params["contractors"] = contractors
    _debug("[DEBUG] trying /reports/local-coverage-articles" + ("" if status else " (no status)"))
    j = _safe_get("/reports/local-coverage-articles", params=params, timeout=timeout)
    return list((j.get("data") or []))  # type: ignore[return-value]


# ---------------------- Article endpoints ----------------------

def article_code_table(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/article/code-table", params=ids, timeout=timeout).get("data")) or [])

def article_icd10_covered(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/article/icd10-covered", params=ids, timeout=timeout).get("data")) or [])

def article_icd10_noncovered(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/article/icd10-noncovered", params=ids, timeout=timeout).get("data")) or [])

def article_hcpc_codes(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/article/hcpc-code", params=ids, timeout=timeout).get("data")) or [])

def article_hcpc_modifiers(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/article/hcpc-modifier", params=ids, timeout=timeout).get("data")) or [])

def article_revenue_codes(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/article/revenue-code", params=ids, timeout=timeout).get("data")) or [])

def article_bill_types(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/article/bill-codes", params=ids, timeout=timeout).get("data")) or [])

def article_urls(ids: Mapping[str, Any], timeout: int = 30):
    # lightweight “sanity probe” to see what sections exist for the article
    return list((_safe_get("/data/article/urls", params=ids, timeout=timeout).get("data")) or [])


# ---------------------- LCD endpoints ----------------------

def lcd_hcpc_codes(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/lcd/hcpc-code", params=ids, timeout=timeout).get("data")) or [])

def lcd_hcpc_modifiers(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/lcd/hcpc-modifier", params=ids, timeout=timeout).get("data")) or [])

def lcd_revenue_codes(ids: Mapping[str, Any], timeout: int = 30):
    return list((_safe_get("/data/lcd/revenue-code", params=ids, timeout=timeout).get("data")) or [])

# Swagger doesn’t list bill types for LCDs; keep stub if you want symmetry.
def lcd_bill_types(ids: Mapping[str, Any], timeout: int = 30):
    return []
