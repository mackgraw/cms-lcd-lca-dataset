# scripts/coverage_api.py
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
    return {"User-Agent": "cms-lcd-lca-harvester/1.0"}

def _params_with_license(params: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    tok = os.environ.get("COVERAGE_LICENSE_TOKEN", "").strip()
    out = dict(params or {})
    if tok:
        out["license_token"] = tok
    return out

def _short_msg(j: Any) -> str:
    """
    Extract a concise error message, truncated to 120 chars.
    """
    s = ""
    if isinstance(j, dict):
        # Try typical keys
        for key in ("message", "error", "detail", "title"):
            if key in j:
                try:
                    s = str(j[key])
                    break
                except Exception:
                    pass
        # If not, flatten briefly
        if not s:
            try:
                s = str(j)[:120]
            except Exception:
                s = ""
    else:
        try:
            s = str(j)[:120]
        except Exception:
            s = ""
    return (s or "").strip()[:120]

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.6, min=0.6, max=3),
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
        # Try to capture short server message
        short = ""
        try:
            j = r.json()
            short = _short_msg(j)
            keys = list(j.keys())
        except Exception:
            keys = []
        if short:
            _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}; message='{short}'")
        else:
            _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}")
        raise _HTTPError(f"{r.status_code} for {full} — {short or 'HTTP error'}") from e
    try:
        j = r.json()
        keys = list(j.keys())
    except Exception:
        j = {}
        keys = []
    _debug(f"[DEBUG] GET {full} -> {r.status_code}; keys={keys}")
    return j

# ---------- license ----------

def ensure_license_acceptance(timeout: int = 30) -> None:
    tok = os.environ.get("COVERAGE_LICENSE_TOKEN", "").strip()
    if tok:
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

# ---------- discovery ----------

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

# ---------- parameter generation ----------

def _param_shapes_for_article(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """
    Try several shapes, with & without document_version.
    Order matters (more specific first).
    """
    did  = ids.get("document_id") or ids.get("article_id")
    disp = ids.get("document_display_id") or ids.get("article_display_id")
    ver  = ids.get("document_version")
    shapes: List[Dict[str, Any]] = []
    if did and ver:
        shapes.append({"article_id": did, "document_version": ver})
        shapes.append({"document_id": did, "document_version": ver})
    if disp and ver:
        shapes.append({"article_display_id": disp, "document_version": ver})
        shapes.append({"document_display_id": disp, "document_version": ver})
    if did:
        shapes.append({"article_id": did})
        shapes.append({"document_id": did})
    if disp:
        shapes.append({"article_display_id": disp})
        shapes.append({"document_display_id": disp})
    # final fallback: empty params (some endpoints may allow all; we will filter client-side)
    shapes.append({})
    return shapes

def _param_shapes_for_lcd(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    did  = ids.get("document_id") or ids.get("lcd_id")
    disp = ids.get("document_display_id") or ids.get("lcd_display_id")
    ver  = ids.get("document_version")
    shapes: List[Dict[str, Any]] = []
    if did and ver:
        shapes.append({"lcd_id": did, "document_version": ver})
        shapes.append({"document_id": did, "document_version": ver})
    if disp and ver:
        shapes.append({"lcd_display_id": disp, "document_version": ver})
        shapes.append({"document_display_id": disp, "document_version": ver})
    if did:
        shapes.append({"lcd_id": did})
        shapes.append({"document_id": did})
    if disp:
        shapes.append({"lcd_display_id": disp})
        shapes.append({"document_display_id": disp})
    shapes.append({})
    return shapes

def _try_many(paths: Iterable[str], param_shapes: Iterable[Mapping[str, Any]], timeout: int) -> List[Dict[str, Any]]:
    """
    Try each path with each param shape until we get a non-empty data list.
    """
    for path in paths:
        for params in param_shapes:
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

# ---------- fetching families ----------

def get_codes_table_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    # SAD Code Table (Article first; LCD has a 'code' table but different scope)
    return _try_many(
        ["/data/article/code-table", "/data/lcd/hcpc-code"],  # fallback LCD path
        _param_shapes_for_article(ids) + _param_shapes_for_lcd(ids),
        timeout
    )

def get_icd10_covered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/icd10-covered", "/data/lcd/hcpc-code"],  # LCD has hcpc/other groups; no direct icd10-covered in swagger
        _param_shapes_for_article(ids) + _param_shapes_for_lcd(ids),
        timeout
    )

def get_icd10_noncovered_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/icd10-noncovered", "/data/lcd/hcpc-code"],
        _param_shapes_for_article(ids) + _param_shapes_for_lcd(ids),
        timeout
    )

def get_hcpc_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/hcpc-code", "/data/lcd/hcpc-code"],
        _param_shapes_for_article(ids) + _param_shapes_for_lcd(ids),
        timeout
    )

def get_hcpc_modifiers_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/hcpc-modifier", "/data/lcd/hcpc-code-group"],  # nearest LCD analogue
        _param_shapes_for_article(ids) + _param_shapes_for_lcd(ids),
        timeout
    )

def get_revenue_codes_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/revenue-code"],
        _param_shapes_for_article(ids),
        timeout
    )

def get_bill_types_any(ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    return _try_many(
        ["/data/article/bill-codes"],
        _param_shapes_for_article(ids),
        timeout
    )
