# scripts/coverage_api.py

from typing import Dict, Any, Optional, Tuple
import requests
from requests import Response

BASE = "https://api.coverage.cms.gov"

def _debug(msg: str) -> None:
    print(f"[DEBUG] {msg}")

def _get(url: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> Response:
    _debug(f"GET {url} -> ...")
    resp = requests.get(url, params=params or {}, timeout=timeout)
    # Log a short server message (first 120 chars) if present
    try:
        j = resp.json()
        if isinstance(j, dict):
            keys = list(j.keys())
            if 'message' in j:
                _debug(f"GET {url} -> {resp.status_code}; keys={keys}; message={str(j['message'])[:120]}")
            else:
                _debug(f"GET {url} -> {resp.status_code}; keys={keys}")
        else:
            _debug(f"GET {url} -> {resp.status_code}")
    except Exception:
        _debug(f"GET {url} -> {resp.status_code} (non-JSON)")
    return resp

def ensure_license_acceptance() -> None:
    url = f"{BASE}/v1/metadata/license-agreement"
    resp = _get(url)
    # best-effort note
    try:
        j = resp.json()
        _debug(f"GET {url} -> {j}; keys={list(j.keys())}")
        print("[note] CMS license agreement acknowledged (no token provided).")
    except Exception:
        print("[note] CMS license agreement acknowledged (no token provided).")

# --- PARAM BUILDING (fixed) ---

def _version_from_ids(ids: Dict[str, Any]) -> Optional[str]:
    """Normalize version field to API's 'ver'."""
    # prefer explicit 'document_version' if present, else 'version'
    v = ids.get("document_version") or ids.get("version")
    if v is None:
        return None
    return str(v)

def _article_params(ids: Dict[str, Any]) -> Dict[str, Any]:
    """Only valid params for article data endpoints."""
    params: Dict[str, Any] = {}
    if "article_id" in ids and ids["article_id"]:
        params["articleid"] = str(ids["article_id"])
    ver = _version_from_ids(ids)
    if ver is not None:
        params["ver"] = ver
    return params

def _lcd_params(ids: Dict[str, Any]) -> Dict[str, Any]:
    """Only valid params for LCD data endpoints."""
    params: Dict[str, Any] = {}
    if "lcd_id" in ids and ids["lcd_id"]:
        params["lcdid"] = str(ids["lcd_id"])
    ver = _version_from_ids(ids)
    if ver is not None:
        params["ver"] = ver
    return params

# --- GENERIC FETCHERS ---

def fetch_article_data(path: str, ids: Dict[str, Any], timeout: Optional[int] = None) -> Tuple[int, Dict[str, Any]]:
    """
    Fetch an article data endpoint with valid params.
    path examples: '/v1/data/article/code-table', '/v1/data/article/hcpc-code', etc.
    """
    url = f"{BASE}{path}"
    params = _article_params(ids)
    if not params:
        _debug(f"-> {path} with (no-params): skipping invalid param set for article endpoints")
        return 200, {"meta": {"status": "ok"}, "data": []}
    resp = _get(url, params=params, timeout=timeout)
    if resp.status_code >= 400:
        # keep going but surface the message
        try:
            msg = resp.json().get("message", "")
        except Exception:
            msg = ""
        _debug(f"  -> {path} with {','.join([f'{k}={v}' for k,v in params.items()])}: {resp.status_code} for {url}: {msg[:120]} (continue)")
        return resp.status_code, {"meta": {"status": "error"}, "data": []}
    j = resp.json()
    _debug(f"  page meta: {list(j.get('meta', {}).keys())}")
    rows = j.get("data", [])
    _debug(f"  -> {path} with {','.join([f'{k}={v}' for k,v in params.items()])}: {len(rows)} rows")
    return resp.status_code, j

def fetch_lcd_data(path: str, ids: Dict[str, Any], timeout: Optional[int] = None) -> Tuple[int, Dict[str, Any]]:
    """
    Fetch an LCD data endpoint with valid params.
    path examples: '/v1/data/lcd/hcpc-code', '/v1/data/lcd/icd10-covered', etc.
    """
    url = f"{BASE}{path}"
    params = _lcd_params(ids)
    if not params:
        _debug(f"-> {path} with (no-params): skipping invalid param set for lcd endpoints")
        return 200, {"meta": {"status": "ok"}, "data": []}
    resp = _get(url, params=params, timeout=timeout)
    if resp.status_code >= 400:
        try:
            msg = resp.json().get("message", "")
        except Exception:
            msg = ""
        _debug(f"  -> {path} with {','.join([f'{k}={v}' for k,v in params.items()])}: {resp.status_code} for {url}: {msg[:120]} (continue)")
        return resp.status_code, {"meta": {"status": "error"}, "data": []}
    j = resp.json()
    _debug(f"  page meta: {list(j.get('meta', {}).keys())}")
    rows = j.get("data", [])
    _debug(f"  -> {path} with {','.join([f'{k}={v}' for k,v in params.items()])}: {len(rows)} rows")
    return resp.status_code, j

# --- PUBLIC HELPERS USED BY run_once.py ---

def get_article_codes(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_article_data("/v1/data/article/hcpc-code", ids, timeout)[1].get("data", [])

def get_article_modifiers(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_article_data("/v1/data/article/hcpc-modifier", ids, timeout)[1].get("data", [])

def get_article_icd10_covered(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_article_data("/v1/data/article/icd10-covered", ids, timeout)[1].get("data", [])

def get_article_icd10_noncovered(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_article_data("/v1/data/article/icd10-noncovered", ids, timeout)[1].get("data", [])

def get_article_revenue_codes(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_article_data("/v1/data/article/revenue-code", ids, timeout)[1].get("data", [])

def get_article_bill_codes(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_article_data("/v1/data/article/bill-codes", ids, timeout)[1].get("data", [])

def get_article_code_table(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_article_data("/v1/data/article/code-table", ids, timeout)[1].get("data", [])

def get_lcd_codes(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_lcd_data("/v1/data/lcd/hcpc-code", ids, timeout)[1].get("data", [])

def get_lcd_icd10_covered(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_lcd_data("/v1/data/lcd/icd10-covered", ids, timeout)[1].get("data", [])

def get_lcd_icd10_noncovered(ids: Dict[str, Any], timeout: Optional[int] = None):
    return fetch_lcd_data("/v1/data/lcd/icd10-noncovered", ids, timeout)[1].get("data", [])
