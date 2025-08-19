# scripts/coverage_api.py
from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import requests
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --------------------------------------------------------------------------- #
# Basic plumbing
# --------------------------------------------------------------------------- #

_BASE = "https://api.coverage.cms.gov/v1"

def _debug(msg: str) -> None:
    print(msg, flush=True)

class _HTTPError(RuntimeError):
    pass

def _headers() -> Dict[str, str]:
    return {"User-Agent": "cms-lcd-lca-harvester/1.0"}

def _params_with_license(params: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    tok = os.environ.get("COVERAGE_LICENSE_TOKEN", "").strip()
    out = dict(params or {})
    if tok:
        out["license_token"] = tok
    return out

def _shorten(s: str, n: int = 120) -> str:
    s = s or ""
    return s if len(s) <= n else s[:n] + "…"

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.6, min=0.6, max=3.5),
    retry=retry_if_exception_type(_HTTPError),
)
def _get(path: str, params: Optional[Mapping[str, Any]], timeout: int) -> Dict[str, Any]:
    full = f"{_BASE}{path}"
    prms = _params_with_license(params)
    _debug(f"[DEBUG] GET {full} -> ...")
    try:
        r = requests.get(full, params=prms, headers=_headers(), timeout=timeout)
        r.raise_for_status()
        j = r.json()
        keys = list(j.keys()) if isinstance(j, dict) else []
        _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}")
        return j
    except requests.HTTPError as e:
        # Try to extract a concise server message
        msg = ""
        try:
            jj = r.json()
            if isinstance(jj, dict):
                # Common shapes: {"message": "...", "id": "..."} or {"errors":[...]}
                if "message" in jj:
                    msg = str(jj.get("message") or "")
                elif "errors" in jj:
                    msg = str(jj.get("errors"))
        except Exception:
            pass
        msg_short = _shorten(msg, 120) if msg else ""
        _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={[k for k in (list(jj.keys()) if isinstance(jj, dict) else [])]}{('; message=' + msg_short) if msg_short else ''}")
        raise _HTTPError(f"{r.status_code} for {full}: {msg_short}") from e

# --------------------------------------------------------------------------- #
# License acceptance (optional, benign if not required)
# --------------------------------------------------------------------------- #

def ensure_license_acceptance(timeout: int = 30) -> None:
    existing = os.environ.get("COVERAGE_LICENSE_TOKEN", "").strip()
    if existing:
        print("[note] CMS license agreement acknowledged (existing token).", flush=True)
        return
    try:
        j = _get("/metadata/license-agreement", None, timeout)
    except (RetryError, _HTTPError):
        print("[warn] license-agreement endpoint not reachable/ok; proceeding without token.", flush=True)
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

# --------------------------------------------------------------------------- #
# Reports discovery (lists)
# --------------------------------------------------------------------------- #

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

# --------------------------------------------------------------------------- #
# Param shaping
# --------------------------------------------------------------------------- #

def _ids_to_param_sets(row_ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """
    Build permutations of id params supported by the v1 endpoints.
    For Articles: article_id, article_display_id (+ document_version if provided).
    For LCDs:     lcd_id, lcd_display_id (+ document_version if provided).
    Fallbacks include document_id/document_display_id (from reports).
    """
    p: List[Dict[str, Any]] = []

    # raw values found on report rows
    article_id = row_ids.get("article_id") or row_ids.get("id") or row_ids.get("document_id")
    article_disp = row_ids.get("article_display_id") or row_ids.get("display_id") or row_ids.get("document_display_id")

    lcd_id = row_ids.get("lcd_id") or row_ids.get("id") or row_ids.get("document_id")
    lcd_disp = row_ids.get("lcd_display_id") or row_ids.get("display_id") or row_ids.get("document_display_id")

    version = row_ids.get("document_version")

    # Article param combos
    for key, val in (("article_id", article_id), ("article_display_id", article_disp),
                     ("document_id", row_ids.get("document_id")), ("document_display_id", row_ids.get("document_display_id"))):
        if val:
            d = {key: val}
            if version:
                d["document_version"] = version
            p.append(d)

    # LCD param combos
    for key, val in (("lcd_id", lcd_id), ("lcd_display_id", lcd_disp),
                     ("document_id", row_ids.get("document_id")), ("document_display_id", row_ids.get("document_display_id"))):
        if val:
            d = {key: val}
            if version:
                d["document_version"] = version
            p.append(d)

    # Always include an empty dict last (some endpoints may support no id for testing)
    p.append({})
    return p

def _try_many(paths: Iterable[str], ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    """
    Try each path with each id-key permutation. Return [] on errors or when the
    endpoint yields zero rows (we treat zero rows as a valid but empty result).
    """
    for path in paths:
        for params in _ids_to_param_sets(ids):
            pretty_keys = ",".join(params.keys()) if params else ""
            try:
                payload = _get(path, params or None, timeout)
            except (RetryError, _HTTPError) as e:
                _debug(f"[DEBUG]   -> {path} with {pretty_keys or '(no-params)'}: {e} (continue)")
                continue

            meta = payload.get("meta") or {}
            data = payload.get("data") or []
            if isinstance(data, list) and data:
                return data

            _debug(f"[DEBUG]   page meta: {list(meta.keys()) or []}")
            _debug(f"[DEBUG]   -> {path} with {pretty_keys or '(no-params)'}: {len(data)} rows")
    return []

# --------------------------------------------------------------------------- #
# Detail endpoints (sanity probes)
# --------------------------------------------------------------------------- #

def get_lcd_detail_any(ids: Mapping[str, Any], timeout: int) -> Dict[str, Any]:
    # v1 LCD detail lives at /data/lcd/
    for params in _ids_to_param_sets(ids):
        try:
            return _get("/data/lcd", params or None, timeout)
        except (RetryError, _HTTPError):
            continue
    return {}

def get_article_detail_any(ids: Mapping[str, Any], timeout: int) -> Dict[str, Any]:
    for params in _ids_to_param_sets(ids):
        try:
            return _get("/data/article", params or None, timeout)
        except (RetryError, _HTTPError):
            continue
    return {}

# --------------------------------------------------------------------------- #
# Harvest families — BACKWARD-COMPAT NAMES expected by run_once.py
# Each tries Article first, then LCD (per your earlier intent).
# --------------------------------------------------------------------------- #

def get_codes_table_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/code-table", "/data/lcd/code-table"],  # lcd code-table may be empty/non-existent in practice
        ids, timeout
    )

def get_icd10_covered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/icd10-covered", "/data/lcd/icd10-covered"],
        ids, timeout
    )

def get_icd10_noncovered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/icd10-noncovered", "/data/lcd/icd10-noncovered"],
        ids, timeout
    )

def get_hcpc_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/hcpc-code", "/data/lcd/hcpc-code"],
        ids, timeout
    )

def get_hcpc_modifiers_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/hcpc-modifier", "/data/lcd/hcpc-modifier"],
        ids, timeout
    )

def get_revenue_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/revenue-code", "/data/lcd/revenue-code"],
        ids, timeout
    )

def get_bill_types_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/bill-codes", "/data/lcd/bill-codes"],
        ids, timeout
    )
