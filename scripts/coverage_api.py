# scripts/coverage_api.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.coverage.cms.gov/v1"
_HELLO_MSG = "Hello MCIM API Users!"

def _build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=40)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"Accept": "application/json"})
    return s

_session = _build_session()

# ---------- license token ----------
_TOKEN: Optional[str] = None
_TOKEN_EXP: float = 0.0

def _full_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return BASE_URL + path

def ensure_license_acceptance(timeout: Optional[float] = None) -> None:
    global _TOKEN, _TOKEN_EXP
    url = _full_url("/metadata/license-agreement")
    print(f"[DEBUG] GET {url} -> ...")
    r = _session.get(url, timeout=timeout)
    try:
        j = r.json()
    except Exception:
        j = None
    if r.status_code != 200:
        raise RuntimeError(f"license-agreement {r.status_code}: {getattr(j, 'text', j)}")
    print(f"[DEBUG] GET {url} -> {r.status_code}; keys={list((j or {}).keys())}")
    if isinstance(j, dict):
        data = j.get("data", [])
        if isinstance(data, list) and data and isinstance(data[0], dict) and "Token" in data[0]:
            _TOKEN = data[0]["Token"]
            _TOKEN_EXP = time.time() + 55 * 60
            print("[note] CMS license agreement accepted; session token cached.")
        else:
            _TOKEN = None
            _TOKEN_EXP = 0.0
            print("[note] CMS license agreement acknowledged (no token provided).")

def _maybe_attach_token(headers: Dict[str, str]) -> Dict[str, str]:
    global _TOKEN, _TOKEN_EXP
    if _TOKEN and time.time() >= _TOKEN_EXP:
        try:
            ensure_license_acceptance(timeout=float(os.getenv("COVERAGE_TIMEOUT") or 30))
        except Exception as e:
            print(f"[warn] token refresh failed; proceeding without token: {e}")
    if _TOKEN:
        h = dict(headers)
        h["Authorization"] = f"Bearer {_TOKEN}"
        return h
    return headers

def _get_json(method: str, path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Dict[str, Any]:
    url = _full_url(path)
    headers = _maybe_attach_token({})
    r = _session.request(method=method.upper(), url=url, params=params or {}, headers=headers, timeout=timeout)
    if r.status_code in (401, 403) and _TOKEN is not None:
        try:
            ensure_license_acceptance(timeout=timeout)
            headers = _maybe_attach_token({})
            r = _session.request(method=method.upper(), url=url, params=params or {}, headers=headers, timeout=timeout)
        except Exception:
            pass
    try:
        j = r.json()
    except Exception:
        j = {"text": r.text}
    if r.status_code >= 400:
        msg = (j.get("message") if isinstance(j, dict) else None) or r.text
        raise RuntimeError(f"{url} {r.status_code}: {msg}")
    return j if isinstance(j, dict) else {}

# ---------- reports ----------
def fetch_local_reports(timeout: Optional[float] = None) -> Tuple[List[dict], List[dict]]:
    lcds_url = "/reports/local-coverage-final-lcds"
    arts_url = "/reports/local-coverage-articles"

    print(f"[DEBUG] GET {_full_url(lcds_url)} -> ...")
    j1 = _get_json("GET", lcds_url, timeout=timeout)
    print(f"[DEBUG] GET {_full_url(arts_url)} -> ...")
    j2 = _get_json("GET", arts_url, timeout=timeout)

    lcds = j1.get("data", []) if isinstance(j1, dict) else []
    arts = j2.get("data", []) if isinstance(j2, dict) else []
    return (lcds if isinstance(lcds, list) else []), (arts if isinstance(arts, list) else [])

# ---------- Article harvesting ----------
ARTICLE_ENDPOINTS = [
    "/data/article/code-table",
    "/data/article/icd10-covered",
    "/data/article/icd10-noncovered",
    "/data/article/hcpc-code",
    "/data/article/hcpc-modifier",
    "/data/article/revenue-code",
    "/data/article/bill-codes",
]

def _collect_endpoint_rows(path: str, primary_params: Dict[str, Any], timeout: Optional[float]) -> List[dict]:
    short = _full_url(path)[len(BASE_URL):]
    try:
        j = _get_json("GET", path, params=primary_params, timeout=timeout)
        meta = (j or {}).get("meta", {})
        data = (j or {}).get("data", [])
        print("[DEBUG]   page meta:", list(meta.keys()))
        shown = ",".join(f"{k}={v}" for k, v in primary_params.items()) or "(no-params)"
        print(f"[DEBUG]   -> {short} with {shown}: {len(data) if isinstance(data, list) else 0} rows")
        return data if isinstance(data, list) else []
    except RuntimeError as e:
        shown = ",".join(f"{k}={v}" for k, v in primary_params.items()) or "(no-params)"
        print(f"[DEBUG]   -> {path} with {shown}: {e}")
        if "ver" in primary_params and " 400:" in str(e):
            # retry unversioned only if we had a version and the error was HTTP
            fallback = {k: v for k, v in primary_params.items() if k != "ver"}
            try:
                j = _get_json("GET", path, params=fallback, timeout=timeout)
                meta = (j or {}).get("meta", {})
                data = (j or {}).get("data", [])
                print("[DEBUG]   page meta:", list(meta.keys()))
                shown = ",".join(f"{k}={v}" for k, v in fallback.items()) or "(no-params)"
                print(f"[DEBUG]   -> {short} with {shown}: {len(data) if isinstance(data, list) else 0} rows")
                return data if isinstance(data, list) else []
            except RuntimeError as e2:
                print(f"[DEBUG]   -> {path} with {shown}: {e2}")
        return []

def harvest_article_endpoints(article_row: dict, timeout: Optional[float]) -> Tuple[dict, dict]:
    def pick(row: dict, *names: str) -> Any:
        for n in names:
            if n in row:
                return row[n]
        return None

    article_id = pick(article_row, "article_id", "articleId", "document_id", "documentId")
    article_display_id = pick(article_row, "article_display_id", "articleDisplayId", "document_display_id", "documentDisplayId")
    document_version = pick(article_row, "document_version", "documentVersion", "ver")

    print(f"[DEBUG] [Article] {article_display_id or article_id}")

    aid = int(article_id) if (article_id is not None and str(article_id).isdigit()) else None
    ver = int(document_version) if (document_version is not None and str(document_version).isdigit()) else None
    if not aid:
        return {}, {"article_id": None, "article_display_id": article_display_id, "document_version": ver}

    results: Dict[str, List[dict]] = {}
    params = {"articleid": aid, **({"ver": ver} if ver else {})}
    for ep in ARTICLE_ENDPOINTS:
        rows = _collect_endpoint_rows(ep, params, timeout=timeout)
        results[ep] = rows

    meta = {
        "article_id": aid,
        "article_display_id": article_display_id,
        "document_version": ver,
    }
    return results, meta

# ---------- LCD harvesting ----------
# We start by assuming all endpoints *might* be available; we will disable them
# globally after we see the API’s "Hello MCIM API Users!" 400 for any one.
_LCD_EP_SUPPORTED: Dict[str, Optional[bool]] = {}

LCD_ENDPOINTS = [
    "/data/lcd/code-table",
    "/data/lcd/icd10-covered",
    "/data/lcd/icd10-noncovered",
    "/data/lcd/hcpc-code",
    "/data/lcd/hcpc-modifier",
    "/data/lcd/revenue-code",
    "/data/lcd/bill-codes",
]

def _is_hello_400(err: Exception) -> bool:
    s = str(err)
    return " 400:" in s and _HELLO_MSG in s

def _collect_lcd_rows_with_probe(path: str, primary_params: Dict[str, Any], timeout: Optional[float]) -> List[dict]:
    # If we’ve already determined this endpoint is unsupported, skip fast.
    if _LCD_EP_SUPPORTED.get(path) is False:
        return []

    short = _full_url(path)[len(BASE_URL):]
    try:
        j = _get_json("GET", path, params=primary_params, timeout=timeout)
        meta = (j or {}).get("meta", {})
        data = (j or {}).get("data", [])
        if _LCD_EP_SUPPORTED.get(path) is None:
            _LCD_EP_SUPPORTED[path] = True
            print(f"[note] enabling LCD endpoint {short}")
        print("[DEBUG]   page meta:", list(meta.keys()))
        shown = ",".join(f"{k}={v}" for k, v in primary_params.items()) or "(no-params)"
        print(f"[DEBUG]   -> {short} with {shown}: {len(data) if isinstance(data, list) else 0} rows")
        return data if isinstance(data, list) else []
    except RuntimeError as e:
        if _is_hello_400(e):
            if _LCD_EP_SUPPORTED.get(path) is not False:
                _LCD_EP_SUPPORTED[path] = False
                print(f"[note] disabling unsupported LCD endpoint {short} (API returned {_HELLO_MSG!r})")
            return []
        # If we provided a version, try unversioned before giving up.
        if "ver" in primary_params:
            fallback = {k: v for k, v in primary_params.items() if k != "ver"}
            try:
                j = _get_json("GET", path, params=fallback, timeout=timeout)
                meta = (j or {}).get("meta", {})
                data = (j or {}).get("data", [])
                if _LCD_EP_SUPPORTED.get(path) is None:
                    _LCD_EP_SUPPORTED[path] = True
                    print(f"[note] enabling LCD endpoint {short}")
                print("[DEBUG]   page meta:", list(meta.keys()))
                shown = ",".join(f"{k}={v}" for k, v in fallback.items()) or "(no-params)"
                print(f"[DEBUG]   -> {short} with {shown}: {len(data) if isinstance(data, list) else 0} rows")
                return data if isinstance(data, list) else []
            except RuntimeError as e2:
                if _is_hello_400(e2):
                    if _LCD_EP_SUPPORTED.get(path) is not False:
                        _LCD_EP_SUPPORTED[path] = False
                        print(f"[note] disabling unsupported LCD endpoint {short} (API returned {_HELLO_MSG!r})")
                    return []
                # other errors -> just print once for visibility
                print(f"[DEBUG]   -> {path} error after fallback: {e2}")
                return []
        # other non-hello errors (e.g., 404/422/etc.)
        print(f"[DEBUG]   -> {path} error: {e}")
        return []

def harvest_lcd_endpoints(lcd_row: dict, timeout: Optional[float]) -> Tuple[dict, dict]:
    def pick(row: dict, *names: str) -> Any:
        for n in names:
            if n in row:
                return row[n]
        return None

    lcd_id = pick(lcd_row, "lcd_id", "lcdId", "document_id", "documentId")
    lcd_display_id = pick(lcd_row, "lcd_display_id", "lcdDisplayId", "document_display_id", "documentDisplayId")
    document_version = pick(lcd_row, "document_version", "documentVersion", "ver")

    print(f"[DEBUG] [LCD] {lcd_display_id or lcd_id}")

    lid = int(lcd_id) if (lcd_id is not None and str(lcd_id).isdigit()) else None
    ver = int(document_version) if (document_version is not None and str(document_version).isdigit()) else None
    if not lid:
        return {}, {"lcd_id": None, "lcd_display_id": lcd_display_id, "document_version": ver}

    params = {"lcdid": lid, **({"ver": ver} if ver else {})}
    results: Dict[str, List[dict]] = {}

    for ep in LCD_ENDPOINTS:
        rows = _collect_lcd_rows_with_probe(ep, params, timeout)
        results[ep] = rows

    meta = {
        "lcd_id": lid,
        "lcd_display_id": lcd_display_id,
        "document_version": ver,
    }
    return results, meta
