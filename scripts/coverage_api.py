from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests


BASE_URL = "https://api.coverage.cms.gov/v1"
USER_AGENT = "cms-lcd-lca-dataset/harvester (+https://github.com/)"


def _none_if_blank(v: Any) -> Optional[Any]:
    """Return None if v is None, '', or all-whitespace; else v as-is."""
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v

def _csv_or_none(v: Optional[str]) -> Optional[str]:
    """Normalize a CSV string; drop if it ends up empty."""
    if v is None:
        return None
    items = [p.strip() for p in v.split(",")]
    items = [p for p in items if p]
    return ",".join(items) if items else None

def build_params(**kwargs: Any) -> Dict[str, Any]:
    """
    Build a query params dict, stripping out any None/empty/whitespace values.
    Pass ALL candidate params and let this drop the blanks.
    """
    out: Dict[str, Any] = {}
    for k, v in kwargs.items():
        if isinstance(v, str) and k.lower() in {"states", "state", "contractors", "contractor"}:
            v = _csv_or_none(v)  # normalize CSV filters
        v = _none_if_blank(v)
        if v is not None:
            out[k] = v
    return out

def _env(name: str, default: str = "") -> str:
    val = os.getenv(name, default)
    print(f"[PY-ENV] {name} = {val}")
    return val


def _debug(msg: str) -> None:
    print(f"[DEBUG] {msg}")


def _truncate_err_message(msg: Any, limit: int = 120) -> str:
    try:
        s = str(msg)
    except Exception:
        s = repr(msg)
    if len(s) > limit:
        return s[:limit]
    return s


def _session(timeout: int = 30) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    # don’t reuse proxies/token/etc. API is public
    s.trust_env = True
    s.request = s.request  # type: ignore[attr-defined]
    s.timeout = timeout  # just to carry a default
    return s


def _get_json(path: str, params: Optional[Mapping[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    _debug(f"GET {url} -> ...")
    try:
        r = requests.get(url, params=params or {}, timeout=timeout, headers={"User-Agent": USER_AGENT})
        if r.status_code >= 400:
            # Try to show the API’s short message if present
            msg = None
            try:
                j = r.json()
                msg = j.get("message") or j.get("error") or j
            except Exception:
                msg = r.text
            short = _truncate_err_message(msg)
            _debug(f"GET {url} -> {r.status_code}; keys={list(getattr(r, 'json', lambda: {})() or {}).keys() if r.headers.get('content-type','').startswith('application/json') else []}; message={short}")
            return {"meta": {"status": r.status_code, "notes": short}, "data": []}
        j = r.json()
        _debug(f"GET {url} -> {r.status_code}; keys={list(j.keys())}")
        return j
    except Exception as e:
        short = _truncate_err_message(e)
        return {"meta": {"status": 599, "notes": short}, "data": []}


# -------------------------
# Metadata / license
# -------------------------
def ensure_license_acceptance(timeout: int = 30) -> None:
    """
    Calls the license endpoint; prints what the server returns.
    Signature includes 'timeout' to match callers.
    """
    j = _get_json("/metadata/license-agreement", timeout=timeout)
    keys = list(j.keys())
    _debug(f"GET {BASE_URL}/metadata/license-agreement -> {j.get('meta', {}).get('status', 200)}; keys={keys}")
    print("[note] CMS license agreement acknowledged (no token provided).")


# -------------------------
# Reports (lists of docs)
# -------------------------
def list_final_lcds(states: str = "", status: str = "", contractors: str = "", timeout: int = 30) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if states:
        params["states"] = states
    if status:
        params["status"] = status
    if contractors:
        params["contractors"] = contractors
    j = _get_json("/reports/local-coverage-final-lcds", params=params, timeout=timeout)
    return j.get("data", []) or []


def list_articles(states: str = "", status: str = "", contractors: str = "", timeout: int = 30) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if states:
        params["states"] = states
    if status:
        params["status"] = status
    if contractors:
        params["contractors"] = contractors
    j = _get_json("/reports/local-coverage-articles", params=params, timeout=timeout)
    return j.get("data", []) or []


# -------------------------
# ID helpers
# -------------------------
def possible_article_param_sets(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Return parameter combos to try for article endpoints (best-first)."""
    combos: List[Dict[str, Any]] = []
    # Prefer explicit id + version combos first
    for a_id_key in ("article_id", "document_id", "article_display_id", "document_display_id"):
        if ids.get(a_id_key) and ids.get("document_version"):
            combos.append({a_id_key: ids[a_id_key], "document_version": ids["document_version"]})
    # Then single keys
    for a_id_key in ("article_id", "document_id", "article_display_id", "document_display_id"):
        if ids.get(a_id_key):
            combos.append({a_id_key: ids[a_id_key]})
    # Fallback empty (some endpoints allow it, usually returns empty)
    combos.append({})
    return combos


def possible_lcd_param_sets(ids: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Return parameter combos to try for LCD endpoints (best-first)."""
    combos: List[Dict[str, Any]] = []
    for k in ("lcd_id", "document_id", "lcd_display_id", "document_display_id"):
        if ids.get(k) and ids.get("document_version"):
            combos.append({k: ids[k], "document_version": ids["document_version"]})
    for k in ("lcd_id", "document_id", "lcd_display_id", "document_display_id"):
        if ids.get(k):
            combos.append({k: ids[k]})
    combos.append({})
    return combos


def _page(path: str, param_sets: Sequence[Mapping[str, Any]], timeout: int = 30) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Try each parameter set until we either get a non-empty data array or we run out.
    Return (rows, last_meta).
    """
    last_meta: Dict[str, Any] = {}
    for params in param_sets:
        keys = ",".join(params.keys()) or "(no-params)"
        _debug(f"  -> trying {path} with {keys}")
        j = _get_json(path, params=params, timeout=timeout)
        meta = j.get("meta", {})
        data = j.get("data", []) or []
        last_meta = meta
        if data:
            return data, meta
    return [], last_meta


# -------------------------
# Article-only endpoints
# -------------------------
def get_codes_table_any(ids: Mapping[str, Any], timeout: int = 30) -> List[Dict[str, Any]]:
    # NOTE: code-table exists only for articles (SAD Exclusion code table)
    rows, meta = _page("/data/article/code-table", possible_article_param_sets(ids), timeout=timeout)
    _debug(f"  -> /data/article/code-table with {','.join(possible_article_param_sets(ids)[0].keys()) or '(no-params)'}: {len(rows)} rows")
    return rows


def get_icd10_covered_any(ids: Mapping[str, Any], timeout: int = 30) -> List[Dict[str, Any]]:
    # Article-only
    rows, meta = _page("/data/article/icd10-covered", possible_article_param_sets(ids), timeout=timeout)
    _debug(f"  -> /data/article/icd10-covered with {','.join(possible_article_param_sets(ids)[0].keys()) or '(no-params)'}: {len(rows)} rows")
    return rows


def get_icd10_noncovered_any(ids: Mapping[str, Any], timeout: int = 30) -> List[Dict[str, Any]]:
    # Article-only
    rows, meta = _page("/data/article/icd10-noncovered", possible_article_param_sets(ids), timeout=timeout)
    _debug(f"  -> /data/article/icd10-noncovered with {','.join(possible_article_param_sets(ids)[0].keys()) or '(no-params)'}: {len(rows)} rows")
    return rows


def get_hcpc_modifiers_any(ids: Mapping[str, Any], timeout: int = 30) -> List[Dict[str, Any]]:
    # Article-only
    rows, meta = _page("/data/article/hcpc-modifier", possible_article_param_sets(ids), timeout=timeout)
    _debug(f"  -> /data/article/hcpc-modifier with {','.join(possible_article_param_sets(ids)[0].keys()) or '(no-params)'}: {len(rows)} rows")
    return rows


def get_revenue_codes_any(ids: Mapping[str, Any], timeout: int = 30) -> List[Dict[str, Any]]:
    # Article-only
    rows, meta = _page("/data/article/revenue-code", possible_article_param_sets(ids), timeout=timeout)
    _debug(f"  -> /data/article/revenue-code with {','.join(possible_article_param_sets(ids)[0].keys()) or '(no-params)'}: {len(rows)} rows")
    return rows


def get_bill_codes_any(ids: Mapping[str, Any], timeout: int = 30) -> List[Dict[str, Any]]:
    # Article-only
    rows, meta = _page("/data/article/bill-codes", possible_article_param_sets(ids), timeout=timeout)
    _debug(f"  -> /data/article/bill-codes with {','.join(possible_article_param_sets(ids)[0].keys()) or '(no-params)'}: {len(rows)} rows")
    return rows


# -------------------------
# Article & LCD endpoints
# -------------------------
def get_hcpc_codes_any(ids: Mapping[str, Any], timeout: int = 30) -> List[Dict[str, Any]]:
    # Try article first, then LCD
    a_rows, a_meta = _page("/data/article/hcpc-code", possible_article_param_sets(ids), timeout=timeout)
    if a_rows:
        _debug(f"  -> /data/article/hcpc-code yielded {len(a_rows)} rows")
        return a_rows
    l_rows, l_meta = _page("/data/lcd/hcpc-code", possible_lcd_param_sets(ids), timeout=timeout)
    _debug(f"  -> /data/lcd/hcpc-code yielded {len(l_rows)} rows")
    return l_rows


# -------------------------
# Utilities for callers
# -------------------------
def discover_ids_from_report_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    The reports provide different fields depending on document type.
    This function standardizes what we might pass into *any() helpers.
    """
    ids: Dict[str, Any] = {}
    # Common
    if row.get("document_id"):
        ids["document_id"] = row["document_id"]
    if row.get("document_display_id"):
        ids["document_display_id"] = row["document_display_id"]
    if row.get("document_version"):
        ids["document_version"] = row["document_version"]

    # Article
    if str(row.get("document_display_id", "")).startswith("A"):
        if row.get("document_id"):
            ids["article_id"] = row["document_id"]
        if row.get("document_display_id"):
            ids["article_display_id"] = row["document_display_id"]

    # LCD
    if str(row.get("document_display_id", "")).startswith("L"):
        if row.get("document_id"):
            ids["lcd_id"] = row["document_id"]
        if row.get("document_display_id"):
            ids["lcd_display_id"] = row["document_display_id"]

    return ids
