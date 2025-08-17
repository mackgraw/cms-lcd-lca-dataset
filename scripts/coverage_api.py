# scripts/coverage_api.py
from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE_URL = "https://api.coverage.cms.gov/v1"

# -------------------------------------------------------------------
# small helpers
# -------------------------------------------------------------------
def _debug(msg: str) -> None:
    print(msg, file=sys.stdout, flush=True)

class _HTTPError(RuntimeError):
    pass

def _q(params: Optional[Mapping[str, Any]]) -> str:
    if not params:
        return ""
    # for debug printing only
    return "?" + "&".join(f"{k}={v}" for k, v in params.items())

Session = requests.Session()

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.25, min=0.25, max=2),
    retry=retry_if_exception_type(_HTTPError),
    reraise=True,
)
def _get(path: str, params: Optional[Mapping[str, Any]], timeout: int) -> Dict[str, Any]:
    full = f"{BASE_URL}{path}{_q(params)}"
    _debug(f"[DEBUG] GET {full} -> ...")
    try:
        r = Session.get(f"{BASE_URL}{path}", params=params, timeout=timeout)
        # Raise for non-2xx
        r.raise_for_status()
        data = r.json()
        keys = list(data.keys())
        _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys!r}")
        return data
    except requests.HTTPError as e:
        # log shape if possible
        try:
            j = r.json()
            _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={list(j.keys())!r}")
        except Exception:
            pass
        # Convert to our error type so tenacity can retry selectively
        raise _HTTPError(f"{r.status_code} Client Error: Bad Request for url: {full}") from e
    except requests.RequestException as e:
        raise _HTTPError(str(e)) from e

def _first_nonempty(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v:
            return v
    return None

def _ids_variants(input_ids: Union[str, int, Mapping[str, Any]]) -> Dict[str, Any]:
    """
    Normalizes an id token into all supported query keys we might need.
    Accepts:
      - display ids like 'A59636' or 'L36668'
      - numeric values 59636/36668
      - dicts: {'article_id': '59636'}, {'document_id': '36668'}, {'document_display_id': 'A59636'}
    """
    if isinstance(input_ids, (str, int)):
        token = str(input_ids)
        if token.upper().startswith("A"):
            return {"article_id": None, "document_id": None, "document_display_id": token}
        if token.upper().startswith("L"):
            return {"article_id": None, "document_id": None, "document_display_id": token}
        # numeric… could be either; we’ll try both depending on endpoint
        return {"article_id": token, "document_id": token, "document_display_id": None}
    # mapping
    d = dict(input_ids)
    # Normalize common aliases
    if "id" in d and "document_id" not in d and "article_id" not in d:
        # ambiguous; expose as both
        d.setdefault("document_id", d["id"])
        d.setdefault("article_id", d["id"])
    return {"article_id": d.get("article_id"),
            "document_id": d.get("document_id"),
            "document_display_id": d.get("document_display_id")}

def _is_lcd(ids: Mapping[str, Any]) -> bool:
    disp = ids.get("document_display_id") or ""
    return isinstance(disp, str) and disp.upper().startswith("L")

def _is_article(ids: Mapping[str, Any]) -> bool:
    disp = ids.get("document_display_id") or ""
    return isinstance(disp, str) and disp.upper().startswith("A")

def ensure_license_acceptance(timeout: int = 30) -> None:
    """
    Preflight call to CMS license endpoint.
    Ensures downstream code-table endpoints will return data
    instead of being blocked by the license gate.
    """
    url = f"{BASE_URL}/metadata/license-agreement"
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.ok:
            print("[note] CMS license agreement acknowledged.")
        else:
            print(f"[warn] CMS license preflight got {resp.status_code}")
    except Exception as e:
        print(f"[warn] CMS license preflight failed: {e}")


# -------------------------------------------------------------------
# discovery (reports)
# -------------------------------------------------------------------
def _try_report(path: str, params: Optional[Mapping[str, Any]], timeout: int) -> Optional[List[Dict[str, Any]]]:
    try:
        payload = _get(path, params, timeout)
        meta = payload.get("meta") or {}
        data = payload.get("data") or []
        _debug(f"[DEBUG] {'final-lcds' if 'final-lcd' in path else 'articles'} discovered: {len(data)}")
        return data
    except _HTTPError:
        return None

def list_final_lcds(states: str, status: str, contractors: str, timeout: int) -> List[Dict[str, Any]]:
    params = {}
    if states:       params["states"] = states
    if contractors:  params["contractors"] = contractors

    # Try with status only if provided (status=all seems to 400)
    if status:
        _debug("[DEBUG] trying /reports/local-coverage-final-lcds (with status)")
        params_with = dict(params)
        params_with["status"] = status
        got = _try_report("/reports/local-coverage-final-lcds", params_with, timeout)
        if got is not None:
            return got
        _debug("[DEBUG] /reports/local-coverage-final-lcds (with status) failed: 400 …")
    _debug("[DEBUG] trying /reports/local-coverage-final-lcds (no status)")
    got = _try_report("/reports/local-coverage-final-lcds", params, timeout)
    return got or []

def list_articles(states: str, status: str, contractors: str, timeout: int) -> List[Dict[str, Any]]:
    params = {}
    if states:       params["states"] = states
    if contractors:  params["contractors"] = contractors

    if status:
        _debug("[DEBUG] trying /reports/local-coverage-articles (with status)")
        params_with = dict(params)
        params_with["status"] = status
        got = _try_report("/reports/local-coverage-articles", params_with, timeout)
        if got is not None:
            return got
        _debug("[DEBUG] /reports/local-coverage-articles (with status) failed: 400 …")
    _debug("[DEBUG] trying /reports/local-coverage-articles (no status)")
    got = _try_report("/reports/local-coverage-articles", params, timeout)
    return got or []

# -------------------------------------------------------------------
# detail fetchers
# -------------------------------------------------------------------
def get_article_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> Dict[str, Any]:
    ids = _ids_variants(input_ids)
    for key in ("article_id", "document_id", "document_display_id"):
        if ids.get(key):
            params = {key: ids[key]}
            payload = _get("/data/article", params, timeout)
            return payload
    # default: empty-ish structure
    return {"meta": {}, "data": []}

def get_final_lcd_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> Dict[str, Any]:
    ids = _ids_variants(input_ids)
    last_err = None
    for key in ("document_id", "document_display_id"):
        if ids.get(key):
            try:
                params = {key: ids[key]}
                return _get("/data/final-lcd", params, timeout)
            except _HTTPError as e:
                last_err = e
    if last_err:
        raise last_err
    return {"meta": {}, "data": []}

# -------------------------------------------------------------------
# generic table helpers
# -------------------------------------------------------------------
def _page_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not payload:
        return []
    meta = payload.get("meta") or {}
    data = payload.get("data") or []
    if meta:
        _debug(f"[DEBUG]   page meta: {list(meta.keys())!r}")
    # normalize: API returns list of dict rows or empty list
    return list(data) if isinstance(data, list) else []

def _try_table(path: str, ids: Mapping[str, Any], try_keys: Sequence[str], timeout: int) -> List[Dict[str, Any]]:
    for key in try_keys:
        if not ids.get(key):
            continue
        params = {key: ids[key]}
        try:
            payload = _get(path, params, timeout)
        except _HTTPError:
            # table not available (e.g., LCD variant 400s) -> continue
            continue
        rows = _page_rows(payload)
        _debug(f"[DEBUG]   -> {path} with {key}={ids[key]}: {len(rows)} rows")
        if rows:
            return rows
    _debug(f"[DEBUG]   -> {path}: no rows (after trying all id keys)")
    return []

def _article_then_lcd_rows(article_path: str, lcd_path: str, ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    # Try article flavor first
    rows = _try_table(article_path, ids, ("article_id", "document_id", "document_display_id"), timeout)
    if rows:
        return rows
    # Try LCD flavor if we look like an LCD or if the caller passed LCD ids
    try:
        lcd_rows = _try_table(lcd_path, ids, ("document_id", "document_display_id"), timeout)
        return lcd_rows
    except Exception:
        return []

# -------------------------------------------------------------------
# specific tables (article+lcd safe)
# -------------------------------------------------------------------
def get_article_codes_table_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    return _try_table("/data/article/code-table", ids, ("article_id", "document_id", "document_display_id"), timeout)

def get_final_lcd_codes_table(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    # The LCD code-table often 400s; just attempt and swallow.
    return _try_table("/data/final-lcd/code-table", ids, ("document_id", "document_display_id"), timeout)

def get_codes_table_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    # Prefer article table, then LCD table (swallow 400s)
    rows = get_article_codes_table_any(input_ids, timeout)
    if rows:
        return rows
    return get_final_lcd_codes_table(input_ids, timeout)

def get_icd10_covered_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    return _article_then_lcd_rows("/data/article/icd10-covered", "/data/final-lcd/icd10-covered", ids, timeout)

def get_icd10_noncovered_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    return _article_then_lcd_rows("/data/article/icd10-noncovered", "/data/final-lcd/icd10-noncovered", ids, timeout)

def get_hcpc_codes_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    return _article_then_lcd_rows("/data/article/hcpc-code", "/data/final-lcd/hcpc-code", ids, timeout)

def get_hcpc_modifiers_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    return _article_then_lcd_rows("/data/article/hcpc-modifier", "/data/final-lcd/hcpc-modifier", ids, timeout)

def get_revenue_codes_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    return _article_then_lcd_rows("/data/article/revenue-code", "/data/final-lcd/revenue-code", ids, timeout)

def get_bill_types_any(input_ids: Union[str, int, Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    ids = _ids_variants(input_ids)
    return _article_then_lcd_rows("/data/article/bill-codes", "/data/final-lcd/bill-codes", ids, timeout)

# Run once when the module is first imported
try:
    ensure_license_acceptance()
except Exception:
    pass

