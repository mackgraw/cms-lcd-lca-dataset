# scripts/coverage_api.py
from __future__ import annotations

import json
import os
import textwrap
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


BASE_URL = "https://api.coverage.cms.gov"


def _debug(msg: str) -> None:
    print(f"[DEBUG] {msg}", flush=True)


def short_error_text(payload: Any, limit: int = 120) -> str:
    """
    Return the first `limit` chars of a server error message when present.
    """
    try:
        if isinstance(payload, dict):
            # common CMS error payload uses 'message'
            m = payload.get("message")
            if m:
                return str(m)[:limit]
            # sometimes nested
            data = payload.get("data")
            if isinstance(data, dict) and "message" in data:
                return str(data["message"])[:limit]
        return (json.dumps(payload) if not isinstance(payload, str) else payload)[:limit]
    except Exception:
        return "<unreadable error>"


def build_params(raw: Dict[str, Optional[str]]) -> Dict[str, str]:
    """
    Clean query params by removing None/empty/whitespace-only values.
    """
    out: Dict[str, str] = {}
    for k, v in raw.items():
        if v is None:
            continue
        s = str(v).strip()
        if not s or s == '""':
            # treat empty-string envs as unset (no filter)
            continue
        out[k] = s
    return out


def _get(session: requests.Session, path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> Tuple[int, Dict[str, Any]]:
    url = f"{BASE_URL}{path}"
    _debug(f"GET {url} -> ...")
    resp = session.get(url, params=params or {}, timeout=timeout)
    status = resp.status_code
    try:
        payload = resp.json()
    except Exception:
        payload = {"message": f"Non-JSON response, status={status}", "text": resp.text[:120]}
    if status != 200:
        _debug(f"GET {url} -> {status}; keys={list(payload.keys())}; message={short_error_text(payload)}")
    else:
        _debug(f"GET {url} -> {status}; keys={list(payload.keys())}")
    return status, payload


def ensure_license_acceptance(session: Optional[requests.Session] = None) -> None:
    """
    Swagger shows GET /v1/metadata/license-agreement/ – API accepts without trailing slash too.
    """
    close_after = False
    if session is None:
        session = requests.Session()
        close_after = True
    try:
        _debug(f"GET {BASE_URL}/v1/metadata/license-agreement -> ...")
        status, payload = _get(session, "/v1/metadata/license-agreement")
        if status == 200:
            print("[note] CMS license agreement acknowledged (no token provided).")
        else:
            msg = short_error_text(payload)
            print(f"[warn] License agreement call returned {status}: {msg}")
    finally:
        if close_after:
            session.close()


def fetch_local_reports(
    session: requests.Session,
    states: Optional[str] = None,
    status: Optional[str] = None,
    contractors: Optional[str] = None,
    timeout: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Pull local coverage 'final-lcds' and 'articles' report lists.
    All filters are optional; empty/whitespace strings are ignored.
    """
    # According to swagger:
    # /v1/reports/local-coverage-final-lcds/
    # /v1/reports/local-coverage-articles/
    params = build_params({
        # The reports endpoints generally accept 'states', 'status', 'contractors'
        # If an API ignores some keys, they will be harmlessly dropped.
        "states": states,
        "status": status,
        "contractors": contractors,
    })
    lcds: List[Dict[str, Any]] = []
    arts: List[Dict[str, Any]] = []

    for path, sink in (
        ("/v1/reports/local-coverage-final-lcds", lcds),
        ("/v1/reports/local-coverage-articles", arts),
    ):
        status_code, payload = _get(session, path, params=params, timeout=timeout)
        if status_code == 200 and isinstance(payload, dict):
            data = payload.get("data", [])
            if isinstance(data, list):
                sink.extend(data)
        else:
            # Log concise server message
            msg = short_error_text(payload)
            print(f"[warn] reports fetch {path} returned {status_code}: {msg}")

    return lcds, arts


# ----------------------------
# Data endpoint callers (Article and LCD)
# ----------------------------

def _fetch_table(
    session: requests.Session,
    path: str,
    ids: Dict[str, Any],
    id_sets: Iterable[Tuple[str, ...]],
    timeout: Optional[int] = None,
    label: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Try the endpoint with different identifier combinations until we get data or exhaust options.
    """
    label = label or path
    rows: List[Dict[str, Any]] = []

    # Try combinations of id params (order matters)
    for combo in id_sets:
        params = {k: ids.get(k) for k in combo if ids.get(k) is not None}
        q = build_params(params)
        # Log exactly which params were used
        if q:
            joined = ",".join(combo)
        else:
            joined = "(no-params)"
        status, payload = _get(session, path, params=q, timeout=timeout)
        page_meta = payload.get("meta", {}) if isinstance(payload, dict) else {}
        if isinstance(page_meta, dict):
            _debug(f"  page meta: {list(page_meta.keys())}")
        data_part = payload.get("data", []) if isinstance(payload, dict) else []
        n = len(data_part) if isinstance(data_part, list) else 0
        print(f"[DEBUG]   -> {path} with {joined}: {n} rows")

        if n > 0 and isinstance(data_part, list):
            rows.extend(data_part)
            break  # stop at first successful id combination

    return rows


# Valid Article data endpoints from swagger
ARTICLE_ENDPOINTS: Tuple[str, ...] = (
    "/v1/data/article/code-table",
    "/v1/data/article/icd10-covered",
    "/v1/data/article/icd10-noncovered",
    "/v1/data/article/hcpc-code",
    "/v1/data/article/hcpc-modifier",
    "/v1/data/article/revenue-code",
    "/v1/data/article/bill-codes",
)

# Valid LCD endpoints from swagger we actually need (avoid lcd/code-table which 400s)
LCD_ENDPOINTS: Tuple[str, ...] = (
    "/v1/data/lcd/hcpc-code",
    # add other lcd endpoints you need that exist in swagger:
    # "/v1/data/lcd/revision-history", etc.
)


def get_article_tables(
    session: requests.Session,
    ids: Dict[str, Any],
    timeout: Optional[int] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch rows from the supported article data endpoints using various id combos.
    """
    # try with (article/document) ids, with/without version when appropriate
    # Ordered to reduce call volume.
    id_combos = [
        ("article_id", "document_version"),
        ("article_display_id", "document_version"),
        ("document_id", "document_version"),
        ("document_display_id", "document_version"),
        ("article_id",),
        ("article_display_id",),
        ("document_id",),
        ("document_display_id",),
        # some article endpoints allow lcd ids (rare), include last
        ("lcd_id", "document_version"),
        ("lcd_display_id", "document_version"),
        ("lcd_id",),
        ("lcd_display_id",),
        tuple(),  # final "no params" probe
    ]

    results: Dict[str, List[Dict[str, Any]]] = {}
    for ep in ARTICLE_ENDPOINTS:
        rows = _fetch_table(session, ep, ids, id_combos, timeout=timeout, label="article")
        results[ep] = rows
    return results


def get_lcd_tables(
    session: requests.Session,
    ids: Dict[str, Any],
    timeout: Optional[int] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch rows from supported lcd data endpoints (skip undocumented ones).
    """
    id_combos = [
        ("lcd_id", "document_version"),
        ("lcd_display_id", "document_version"),
        ("document_id", "document_version"),
        ("document_display_id", "document_version"),
        ("lcd_id",),
        ("lcd_display_id",),
        ("document_id",),
        ("document_display_id",),
        tuple(),
    ]
    results: Dict[str, List[Dict[str, Any]]] = {}
    for ep in LCD_ENDPOINTS:
        rows = _fetch_table(session, ep, ids, id_combos, timeout=timeout, label="lcd")
        results[ep] = rows
    return results
