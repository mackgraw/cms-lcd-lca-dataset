from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

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
    _debug(f"[DEBUG] GET {full}{' -> ...' if True else ''}")
    r = requests.get(full, params=prms, headers=_headers(), timeout=timeout)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        # Normalize to our error class (so tenacity handles it)
        # and show a small line with the json body keys (helps debugging).
        try:
            j = r.json()
            keys = list(j.keys())
        except Exception:
            keys = []
        _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}")
        raise _HTTPError(f"{r.status_code} Client Error: Bad Request for url: {full}") from e
    j = r.json()
    try:
        keys = list(j.keys())
    except Exception:
        keys = []
    _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}")
    return j


# ----- license acceptance -----------------------------------------------------

def ensure_license_acceptance(timeout: int = 30) -> None:
    """
    One-time: POST (or GET, per API contract) to /metadata/license-agreement to
    acknowledge license and capture a token in env COVERAGE_LICENSE_TOKEN.

    If the token is already present, we just print a note and return.
    """
    existing = os.environ.get("COVERAGE_LICENSE_TOKEN", "").strip()
    if existing:
        print("[note] CMS license agreement acknowledged (existing token).", flush=True)
        return

    # The coverage API supports a simple GET to /metadata/license-agreement which
    # returns the license text and (often) a token to pass on licensed endpoints.
    # If it does not return a token (some deployments), we still set a benign
    # env so downstream calls include it if needed.
    try:
        j = _get("/metadata/license-agreement", None, timeout)
    except RetryError:
        print("[warn] license-agreement endpoint not reachable; proceeding without token.", flush=True)
        return
    except _HTTPError:
        print("[warn] license-agreement endpoint returned an error; proceeding without token.", flush=True)
        return

    token = ""
    # Common shapes:
    # { meta: {...}, data: [{ token: "..." }]}  OR  { token: "..." }  OR none
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
    """Simple fetch (API returns everything in one page for these report endpoints)."""
    payload = _get(path, params, timeout)
    return list(payload.get("data") or [])

def list_final_lcds(states: str, status: str, contractors: str, timeout: int) -> List[Dict[str, Any]]:
    # These three filters are optional; pass through only if non-empty
    params: Dict[str, Any] = {}
    if states.strip():
        params["states"] = states
    if status.strip():
        params["status"] = status
    if contractors.strip():
        params["contractors"] = contractors
    _debug("[DEBUG] trying /reports/local-coverage-final-lcds (no status)" if not status else "[DEBUG] trying /reports/local-coverage-final-lcds")
    return _paged_report("/reports/local-coverage-final-lcds", timeout, params)

def list_articles(states: str, status: str, contractors: str, timeout: int) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if states.strip():
        params["states"] = states
    if status.strip():
        params["status"] = status
    if contractors.strip():
        params["contractors"] = contractors
    _debug("[DEBUG] trying /reports/local-coverage-articles (no status)" if not status else "[DEBUG] trying /reports/local-coverage-articles")
    return _paged_report("/reports/local-coverage-articles", timeout, params)


# ----- helpers to try Article first, then LCD --------------------------------

def _ids_to_param_sets(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """
    The API supports both numeric ids and display ids; try a few keys.
    We preserve the exact names used in previous logs for parity.
    """
    out: List[Dict[str, Any]] = []
    did  = ids.get("document_id") or ids.get("id")
    disp = ids.get("document_display_id") or ids.get("display_id")
    if did:
        out.append({"document_id": did})
    if disp:
        out.append({"document_display_id": disp})
    return out or [{}]

def _try_many(paths: Iterable[str], ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    """
    Try each path with each id key variant. Return [] on any 400s / errors.
    """
    for path in paths:
        for params in _ids_to_param_sets(ids):
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
            # Some endpoints return {meta: {fields/children/...}} even with 0 rows;
            # normalize to a list.
            if isinstance(data, list) and data:
                return data
            _debug(f"[DEBUG]   page meta: {list(meta.keys()) or []}")
            _debug(f"[DEBUG]   -> {path} with {','.join(params.keys())}: {len(data)} rows")
    # nothing worked
    return []

# ----- LCD detail (used by sanity probe) -------------------------------------

def get_final_lcd_any(ids: Mapping[str, Any], timeout: int) -> Dict[str, Any]:
    """
    Fetch LCD detail if available. If the endpoint 400s, let the caller decide.
    """
    params = None
    for p in _ids_to_param_sets(ids):
        params = p
        break
    return _get("/data/final-lcd", params or None, timeout)

# ----- harvesting families ----------------------------------------------------

def get_codes_table_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    # Try Article first then LCD
    return _try_many(
        ["/data/article/code-table", "/data/final-lcd/code-table"],
        ids, timeout
    )

def get_icd10_covered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/icd10-covered", "/data/final-lcd/icd10-covered"],
        ids, timeout
    )

def get_icd10_noncovered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/icd10-noncovered", "/data/final-lcd/icd10-noncovered"],
        ids, timeout
    )

def get_hcpc_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/hcpc-code", "/data/final-lcd/hcpc-code"],
        ids, timeout
    )

def get_hcpc_modifiers_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/hcpc-modifier", "/data/final-lcd/hcpc-modifier"],
        ids, timeout
    )

def get_revenue_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/revenue-code", "/data/final-lcd/revenue-code"],
        ids, timeout
    )

def get_bill_types_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/bill-codes", "/data/final-lcd/bill-codes"],
        ids, timeout
    )
