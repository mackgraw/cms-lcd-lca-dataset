from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Mapping, Optional

import requests
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ----- basic plumbing ---------------------------------------------------------

_BASE = "https://api.coverage.cms.gov/v1"

def _debug(msg: str) -> None:
    print(msg, flush=True)

class _HTTPError(RuntimeError):
    pass

def _headers() -> Dict[str, str]:
    # keep lean; add User-Agent for friendlier logs on the server side
    return {"User-Agent": "cms-lcd-lca-starter/harvester"}

def _params_with_license(params: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Attach license token param if present (after acceptance)."""
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
        # Surface the API's error message if present
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

# ----- license acceptance -----------------------------------------------------

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

# ----- discovery endpoints ----------------------------------------------------

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

# ----- correct parameter builders --------------------------------------------

def _article_param_sets(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """
    Accept IDs under multiple keys and canonicalize to:
      - article_id
      - article_display_id (e.g., A58017)
    """
    out: List[Dict[str, Any]] = []

    # Numeric ID can arrive under a few names
    article_id = (
        ids.get("article_id")
        or ids.get("id")
        or ids.get("document_id")
    )
    if article_id not in (None, "", 0):
        out.append({"article_id": article_id})

    # Display ID can arrive under various names
    display = (
        ids.get("article_display_id")
        or ids.get("display_id")
        or ids.get("document_display_id")
    )
    if display:
        out.append({"article_display_id": str(display)})

    return out or [{}]


def _lcd_param_sets(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """
    Canonicalize to:
      - document_id
      - document_display_id (e.g., L35000)
    """
    out: List[Dict[str, Any]] = []

    document_id = (
        ids.get("document_id")
        or ids.get("id")
        or ids.get("article_id")  # be liberal: if someone passed numeric under article_id
    )
    if document_id not in (None, "", 0):
        out.append({"document_id": document_id})

    display = (
        ids.get("document_display_id")
        or ids.get("display_id")
        or ids.get("article_display_id")
    )
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

# ----- LCD detail (used by sanity probe) -------------------------------------

def get_final_lcd_any(ids: Mapping[str, Any], timeout: int) -> Dict[str, Any]:
    # Prefer exact id, then display id
    param_sets = _lcd_param_sets(ids)
    # Try the first set only; this is a detail call
    params = next(iter(param_sets), {})
    return _get("/data/final-lcd", params or None, timeout)

# ----- harvesting families (article-first, then LCD) --------------------------

def get_codes_table_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows = _try_one("/data/article/code-table", _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one("/data/final-lcd/code-table", _lcd_param_sets(ids), timeout)

def get_icd10_covered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows = _try_one("/data/article/icd10-covered", _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one("/data/final-lcd/icd10-covered", _lcd_param_sets(ids), timeout)

def get_icd10_noncovered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows = _try_one("/data/article/icd10-noncovered", _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one("/data/final-lcd/icd10-noncovered", _lcd_param_sets(ids), timeout)

def get_hcpc_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows = _try_one("/data/article/hcpc-code", _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one("/data/final-lcd/hcpc-code", _lcd_param_sets(ids), timeout)

def get_hcpc_modifiers_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows = _try_one("/data/article/hcpc-modifier", _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one("/data/final-lcd/hcpc-modifier", _lcd_param_sets(ids), timeout)

def get_revenue_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows = _try_one("/data/article/revenue-code", _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one("/data/final-lcd/revenue-code", _lcd_param_sets(ids), timeout)

def get_bill_types_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    rows = _try_one("/data/article/bill-codes", _article_param_sets(ids), timeout)
    if rows:
        return rows
    return _try_one("/data/final-lcd/bill-codes", _lcd_param_sets(ids), timeout)
