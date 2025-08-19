# scripts/coverage_api.py
from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, Optional, Tuple
import requests

BASE_URL = "https://api.coverage.cms.gov/v1"
DEFAULT_TIMEOUT = float(os.environ.get("COVERAGE_TIMEOUT", "30"))  # seconds

class CoverageApiError(RuntimeError):
    def __init__(self, status: int, url: str, payload: Optional[dict] = None, text: str = ""):
        self.status = status
        self.url = url
        self.payload = payload or {}
        self.text = text or ""
        # short message from server if available
        msg = self.payload.get("message") or self.text
        short = (msg or "").replace("\n", " ")[:120]
        super().__init__(f"{status} for {url}: {short}")

def _short_error(msg: str) -> str:
    return (msg or "").replace("\n", " ")[:120]

def _mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "cms-harvest/1.0"})
    return s

def api_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = DEFAULT_TIMEOUT,
    log: Any = print,
    allow_4xx: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    """
    GET {BASE_URL}{path}. Returns (status_code, json_dict).
    On 4xx/5xx: if allow_4xx is False, raises CoverageApiError (after logging a 120‑char excerpt).
                if allow_4xx is True, returns (status, json_dict) for the caller to handle.
    """
    if not path.startswith("/"):
        path = "/" + path
    url = BASE_URL + path

    sess = session or _mk_session()
    log(f"[DEBUG] GET {url} -> ...")
    try:
        r = sess.get(url, params=params or {}, timeout=timeout)
    except requests.RequestException as e:
        raise CoverageApiError(0, url, text=str(e)) from e

    status = r.status_code
    try:
        data = r.json()
    except ValueError:
        data = {}

    keys = list(data.keys()) if isinstance(data, dict) else []
    if status >= 400:
        msg = ""
        if isinstance(data, dict):
            msg = data.get("message") or data.get("error") or ""
        else:
            msg = r.text or ""
        short = _short_error(msg)
        log(f"[DEBUG] GET {url} -> {status}; keys={keys!r}; message={short}")
        if allow_4xx:
            return status, data if isinstance(data, dict) else {"raw": r.text}
        raise CoverageApiError(status, url, payload=data if isinstance(data, dict) else {}, text=r.text)

    log(f"[DEBUG] GET {url} -> {status}; keys={keys!r}")
    return status, data if isinstance(data, dict) else {"data": data}

def ensure_license_acceptance(
    *,
    session: Optional[requests.Session] = None,
    log: Any = print,
) -> Optional[str]:
    """
    Calls /metadata/license-agreement to acknowledge license. Returns token (if server provides one).
    """
    status, js = api_get("/metadata/license-agreement", session=session, log=log)
    token = None
    # CMS sometimes returns token under 'data' or 'meta'
    if isinstance(js, dict):
        if isinstance(js.get("data"), dict):
            token = js["data"].get("token") or js["data"].get("value")
        if not token and isinstance(js.get("meta"), dict):
            token = js["meta"].get("token") or js["meta"].get("value")
    if token:
        log(f"[note] CMS license agreement acknowledged (token present).")
    else:
        log(f"[note] CMS license agreement acknowledged (no token provided).")
    return token

# -------- Reports helpers --------

def get_report(report_path: str, params: Optional[Dict[str, Any]] = None, *, session: Optional[requests.Session] = None, log: Any = print) -> Dict[str, Any]:
    """
    Fetch a /reports/... endpoint. Example: '/reports/local-coverage-articles'
    """
    if not report_path.startswith("/"):
        report_path = "/" + report_path
    if not report_path.startswith("/reports/"):
        report_path = "/reports" + report_path
    return api_get(report_path, params=params, session=session, log=log)[1]

# -------- Data helpers (Article/LCD) --------

def get_article_page(endpoint: str, params: Optional[Dict[str, Any]] = None, *, session: Optional[requests.Session] = None, log: Any = print, allow_4xx: bool = False) -> Tuple[int, Dict[str, Any]]:
    """
    GET /data/article/<endpoint>
    """
    ep = endpoint.strip("/")
    return api_get(f"/data/article/{ep}", params=params, session=session, log=log, allow_4xx=allow_4xx)

def get_lcd_page(endpoint: str, params: Optional[Dict[str, Any]] = None, *, session: Optional[requests.Session] = None, log: Any = print, allow_4xx: bool = False) -> Tuple[int, Dict[str, Any]]:
    """
    GET /data/lcd/<endpoint>
    """
    ep = endpoint.strip("/")
    return api_get(f"/data/lcd/{ep}", params=params, session=session, log=log, allow_4xx=allow_4xx)

# -------- Specific helpers used by the harvester --------

VALID_ARTICLE_ENDPTS = {
    "code-table",               # SAD exclusion code table (Articles only)
    "icd10-covered",
    "icd10-covered-group",
    "icd10-noncovered",
    "icd10-noncovered-group",
    "icd10-pcs-code",
    "icd10-pcs-code-group",
    "hcpc-code",
    "hcpc-code-group",
    "hcpc-modifier",
    "hcpc-modifier-group",
    "revenue-code",
    "bill-codes",
    # ... add more article endpoints as needed
}

VALID_LCD_ENDPTS = {
    "hcpc-code",
    "hcpc-code-group",
    "revision-history",
    "related-documents",
    "related-ncd-documents",
    "future-retire",
    "primary-jurisdiction",
    "tracking-sheet",
    "urls",
    "attachments",
    "reason-change",
    "advisory-committee",
    "contractor",
    "synopsis-changes",
    # ... add more LCD endpoints as needed
}

def get_codes_table_any(ids: Dict[str, Any], *, session: Optional[requests.Session] = None, log: Any = print) -> Dict[str, Any]:
    """
    Try retrieving the Article SAD Exclusion 'code-table'.
    (LCDs do NOT have a 'code-table' endpoint; calling it will 400. We avoid that.)
    We attempt multiple parameter keys if available.
    """
    keys_sets = [
        ("article_id", "document_version"),
        ("article_display_id", "document_version"),
        ("document_id", "document_version"),
        ("document_display_id", "document_version"),
        ("article_id",),
        ("article_display_id",),
        ("document_id",),
        ("document_display_id",),
    ]

    for keys in keys_sets:
        params = {k: ids[k] for k in keys if k in ids and ids[k] is not None}
        status, js = get_article_page("code-table", params=params, session=session, log=log)
        meta = js.get("meta", {}) if isinstance(js, dict) else {}
        rows = js.get("data", []) if isinstance(js, dict) else []
        log(f"[DEBUG]   -> /data/article/code-table with {','.join(keys) if keys else '(no-params)'}: {len(rows) if isinstance(rows, list) else 0} rows")
    # Return the last response (your harvester aggregates rows itself)
    return js

def fetch_article_endpoint(endpoint: str, ids: Dict[str, Any], *, session: Optional[requests.Session] = None, log: Any = print) -> Dict[str, Any]:
    """
    Generic fetcher for /data/article/<endpoint> using a variety of ID params.
    """
    if endpoint not in VALID_ARTICLE_ENDPTS:
        # Allow but warn — still try since API evolves.
        log(f"[DEBUG] WARNING: endpoint '{endpoint}' not in known VALID_ARTICLE_ENDPTS; attempting anyway.")

    key_orders = [
        ("article_id", "document_version"),
        ("article_display_id", "document_version"),
        ("document_id", "document_version"),
        ("document_display_id", "document_version"),
        ("article_id",),
        ("article_display_id",),
        ("document_id",),
        ("document_display_id",),
        (),  # no params
    ]
    last = {}
    for keys in key_orders:
        params = {k: ids[k] for k in keys if k in ids and ids[k] is not None}
        status, js = get_article_page(endpoint, params=params, session=session, log=log)
        rows = js.get("data", []) if isinstance(js, dict) else []
        log(f"[DEBUG]   -> /data/article/{endpoint} with {','.join(keys) if keys else '(no-params)'}: {len(rows) if isinstance(rows, list) else 0} rows")
        last = js
    return last

def fetch_lcd_endpoint(endpoint: str, ids: Dict[str, Any], *, session: Optional[requests.Session] = None, log: Any = print) -> Dict[str, Any]:
    """
    Generic fetcher for /data/lcd/<endpoint> using a variety of ID params.
    NOTE: Will log (and *not* raise) 400s so the harvester can continue.
    """
    if endpoint not in VALID_LCD_ENDPTS:
        log(f"[DEBUG] WARNING: endpoint '{endpoint}' not in known VALID_LCD_ENDPTS; attempting anyway.")
    key_orders = [
        ("lcd_id", "document_version"),
        ("lcd_display_id", "document_version"),
        ("document_id", "document_version"),
        ("document_display_id", "document_version"),
        ("lcd_id",),
        ("lcd_display_id",),
        ("document_id",),
        ("document_display_id",),
        (),  # no params
    ]
    last = {}
    for keys in key_orders:
        params = {k: ids[k] for k in keys if k in ids and ids[k] is not None}
        status, js = get_lcd_page(endpoint, params=params, session=session, log=log, allow_4xx=True)
        if status >= 400:
            # Log with first 120 chars (api_get already prints it, but we echo a concise line here too)
            msg = ""
            if isinstance(js, dict):
                msg = js.get("message") or js.get("error") or ""
            short = _short_error(msg)
            log(f"[DEBUG]   -> /data/lcd/{endpoint} with {','.join(keys) if keys else '(no-params)'}: {status} for {BASE_URL}/data/lcd/{endpoint}: {short} (continue)")
        else:
            rows = js.get("data", []) if isinstance(js, dict) else []
            log(f"[DEBUG]   -> /data/lcd/{endpoint} with {','.join(keys) if keys else '(no-params)'}: {len(rows) if isinstance(rows, list) else 0} rows")
        last = js
    return last
