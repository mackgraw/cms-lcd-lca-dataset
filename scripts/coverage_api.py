from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Mapping, Optional

import requests
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential, retry_if_exception_type

_BASE = "https://api.coverage.cms.gov/v1"

def _debug(msg: str) -> None:
    print(msg, flush=True)

class _HTTPError(RuntimeError):
    pass

def _headers() -> Dict[str, str]:
    return {"User-Agent": "cms-lcd-lca-starter/harvester"}

def _params_with_license(params: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    tok = os.environ.get("COVERAGE_LICENSE_TOKEN", "").strip()
    out = dict(params or {})
    if tok:
        out["license_token"] = tok
    return out

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=3),
    retry=retry_if_exception_type(_HTTPError),
)
def _get(path: str, params: Optional[Mapping[str, Any]], timeout: int) -> Dict[str, Any]:
    full = f"{_BASE}{path}"
    prms = _params_with_license(params)
    _debug(f"[DEBUG] GET {full} -> ...")
    r = requests.get(full, params=prms, headers=_headers(), timeout=timeout)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        msg = ""
        try:
            j = r.json()
            msg = str(j.get("message", "") or j.get("error", "")).strip()
            keys = list(j.keys())
        except Exception:
            keys = []
        _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}; message={msg!r}")
        detail = f"{r.status_code} for {full}"
        if msg:
            detail += f" — {msg}"
        raise _HTTPError(detail) from e
    j = r.json()
    try:
        keys = list(j.keys())
    except Exception:
        keys = []
    _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}")
    return j

# ---- license agreement -------------------------------------------------------

def ensure_license_acceptance(timeout: int = 30) -> None:
    existing = os.environ.get("COVERAGE_LICENSE_TOKEN", "").strip()
    if existing:
        print("[note] CMS license agreement acknowledged (existing token).", flush=True)
        return
    try:
        j = _get("/metadata/license-agreement", None, timeout)
    except RetryError:
        print("[warn] license-agreement endpoint not reachable; proceeding without token.", flush=True)
        return
    except _HTTPError:
        print("[warn] license-agreement endpoint returned an error; proceeding without token.", flush=True)
        return
    token = ""
    if isinstance(j, dict):
        if "data" in j and isinstance(j["data"], list) and j["data"]:
            maybe = j["data"][0]
            if isinstance(maybe, dict):
                token = str(maybe.get("token", "")).strip()
        if not token:
            token = str(j.get("token", "")).strip()
    if token:
        os.environ["COVERAGE_LICENSE_TOKEN"] = token
        print("[note] CMS license agreement acknowledged.", flush=True)
    else:
        print("[note] CMS license agreement acknowledged (no token provided).", flush=True)

# ---- discovery ---------------------------------------------------------------

def _paged_report(path: str, timeout: int, params: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
    payload = _get(path, params, timeout)
    return list(payload.get("data") or [])

def list_final_lcds(states: str, status: str, contractors: str, timeout: int) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if states.strip():
        params["states"] = states
    if status.strip():
        params["status"] = status
    if contractors.strip():
        params["contractors"] = contractors
    _debug("[DEBUG] trying /reports/local-coverage-final-lcds" + ("" if status else " (no status)"))
    return _paged_report("/reports/local-coverage-final-lcds", timeout, params)

def list_articles(states: str, status: str, contractors: str, timeout: int) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if states.strip():
        params["states"] = states
    if status.strip():
        params["status"] = status
    if contractors.strip():
        params["contractors"] = contractors
    _debug("[DEBUG] trying /reports/local-coverage-articles" + ("" if status else " (no status)"))
    return _paged_report("/reports/local-coverage-articles", timeout, params)

# ---- parameter builders & family detection ----------------------------------

def _is_article_ids(ids: Mapping[str, Any]) -> bool:
    return bool(ids.get("article_id") or ids.get("article_display_id"))

def _is_lcd_ids(ids: Mapping[str, Any]) -> bool:
    return bool(ids.get("document_id") or ids.get("document_display_id"))

def _article_param_sets(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    article_id = ids.get("article_id") or ids.get("id") or ids.get("document_id")
    if article_id not in (None, "", 0):
        out.append({"article_id": article_id})
    display = ids.get("article_display_id") or ids.get("display_id") or ids.get("document_display_id")
    if display:
        out.append({"article_display_id": str(display)})
    return out or [{}]

def _lcd_param_sets(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    document_id = ids.get("document_id") or ids.get("id") or ids.get("article_id")
    if document_id not in (None, "", 0):
        out.append({"document_id": document_id})
    display = ids.get("document_display_id") or ids.get("display_id") or ids.get("article_display_id")
    if display:
        out.append({"document_display_id": str(display)})
    return out or [{}]

def _try_one(path: str, param_sets: Iterable[Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    for params in param_sets:
        try:
            payload = _get(path, params, timeout)
        except RetryError as e:
            _debug(f"[DEBUG]   -> {path} with {','.join(params.keys())}: {e} (continue)")
            continue
        except _HTTPError as e:
            _debug(f"[DEBUG]   -> {path} with {','.join(params.keys())}: {e} (continue)")
            continue
        meta = payload.get("meta") or {}
        data = payload.get("data") or []
        if isinstance(data, list) and data:
            return data
        _debug(f"[DEBUG]   page meta: {list(meta.keys()) or []}")
        _debug(f"[DEBUG]   -> {path} with {','.join(params.keys())}: {len(data)} rows")
    return []

# ---- LCD detail (optional sanity) -------------------------------------------

def get_final_lcd_any(ids: Mapping[str, Any], timeout: int) -> Dict[str, Any]:
    params = next(iter(_lcd_param_sets(ids)), {})
    return _get("/data/final-lcd", params or None, timeout)

# ---- harvesting: respect family hint; otherwise fall back -------------------

def _family_routed(article_path: str, lcd_path: str, ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    if _is_article_ids(ids):
        return _try_one(article_path, _article_param_sets(ids), timeout)
    if _is_lcd_ids(ids):
        return _try_one(lcd_path, _lcd_param_sets(ids), timeout)
    # If ambiguous, try article first then LCD (rare once run_once passes kind-aware ids)
    rows = _try_one(article_path, _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one(lcd_path, _lcd_param_sets(ids), timeout)

def get_codes_table_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _family_routed("/data/article/code-table", "/data/final-lcd/code-table", ids, timeout)

def get_icd10_covered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _family_routed("/data/article/icd10-covered", "/data/final-lcd/icd10-covered", ids, timeout)

def get_icd10_noncovered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _family_routed("/data/article/icd10-noncovered", "/data/final-lcd/icd10-noncovered", ids, timeout)

def get_hcpc_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _family_routed("/data/article/hcpc-code", "/data/final-lcd/hcpc-code", ids, timeout)

def get_hcpc_modifiers_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _family_routed("/data/article/hcpc-modifier", "/data/final-lcd/hcpc-modifier", ids, timeout)

def get_revenue_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _family_routed("/data/article/revenue-code", "/data/final-lcd/revenue-code", ids, timeout)

def get_bill_types_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _family_routed("/data/article/bill-codes", "/data/final-lcd/bill-codes", ids, timeout)
