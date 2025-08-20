# scripts/coverage_api.py

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import requests


_BASE = "https://api.coverage.cms.gov"
_SESSION: Optional[requests.Session] = None
_BEARER: Optional[str] = None
_TOKEN_TS: Optional[float] = None

# The API says tokens last ~1 hour. Refresh a bit early to be safe.
_TOKEN_TTL_SECONDS = 60 * 60
_REFRESH_SKEW_SECONDS = 5 * 60  # refresh ~5 minutes early


# -----------------------------
# Session & token management
# -----------------------------
def get_session() -> requests.Session:
    """Return a memoized requests.Session with standard headers."""
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "cms-lcd-lca-dataset/harvester",
            }
        )
        _SESSION = s
    return _SESSION


def _token_is_stale() -> bool:
    """True if we should refresh the token due to age."""
    if _TOKEN_TS is None:
        return True
    age = time.time() - _TOKEN_TS
    return age >= (_TOKEN_TTL_SECONDS - _REFRESH_SKEW_SECONDS)


def ensure_license_acceptance(timeout: Optional[int] = None) -> Optional[str]:
    """
    Call /v1/metadata/license-agreement to acknowledge the license
    and install the returned bearer token on the session.
    Returns the token, or None if the endpoint didn’t return one.
    """
    global _BEARER, _TOKEN_TS

    s = get_session()
    url = f"{_BASE}/v1/metadata/license-agreement"
    print(f"[DEBUG] GET {url} -> ...")
    r = s.get(url, timeout=timeout or 30)

    # Try to parse JSON even on error; log useful bits.
    try:
        payload = r.json()
    except Exception:
        payload = {"meta": {"status": {"id": r.status_code, "message": r.reason}}, "data": []}

    status = payload.get("meta", {}).get("status", {})
    code = status.get("id", r.status_code)
    print(f"[DEBUG] GET {url} -> {code}; keys={list(payload.keys())}")
    # full payload can be noisy, but printing here has helped debugging:
    print(f"[DEBUG] GET {url} -> {payload}; keys={list(payload.keys())}")

    # Extract the token (if provided)
    token = None
    try:
        data = payload.get("data") or []
        if data and isinstance(data, list) and isinstance(data[0], dict):
            token = data[0].get("Token")
    except Exception:
        token = None

    if token:
        _BEARER = token
        _TOKEN_TS = time.time()
        s.headers["Authorization"] = f"Bearer {token}"
        masked = token[:8] + "..." + token[-4:] if len(token) > 12 else "***"
        print(f"[note] CMS license token acquired and installed (Bearer {masked}).")
    else:
        print("[note] CMS license agreement acknowledged (no token provided).")

    return token


# -----------------------------
# Low-level GET helpers
# -----------------------------
def _remap_short_params(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Optional convenience: map short names used in some call sites to API names.
    Example: articleid -> article_id, ver -> document_version
    """
    if not p:
        return p
    p = dict(p)  # copy
    # Short -> API
    if "articleid" in p:
        p["article_id"] = p.pop("articleid")
    if "lcdid" in p:
        p["lcd_id"] = p.pop("lcdid")
    if "docid" in p:
        p["document_id"] = p.pop("docid")
    if "docdisplay" in p:
        p["document_display_id"] = p.pop("docdisplay")
    if "ver" in p:
        p["document_version"] = p.pop("ver")
    return p


def _maybe_log_server_message(payload: Dict[str, Any]) -> None:
    # Print the first 120 chars of an API-level "message", when present
    msg = payload.get("message")
    if msg is not None:
        print(f"[DEBUG]   server message: {str(msg)[:120]}")


def _request_with_optional_refresh(
    method: str, url: str, *, params: Optional[Dict[str, Any]], timeout: Optional[int]
) -> requests.Response:
    """
    Make one request. If we get 401/403, refresh token and retry once.
    Also refresh token if it's stale before we make the call.
    """
    s = get_session()

    # Pre-emptive refresh if our token is stale/absent
    if _token_is_stale():
        try:
            ensure_license_acceptance(timeout=timeout)
        except Exception as e:
            print(f"[warn] token refresh failed pre-request: {e!r}")

    # First attempt
    r = s.request(method, url, params=params or {}, timeout=timeout or 30)

    if r.status_code in (401, 403):
        # Try a refresh and retry once
        print(f"[DEBUG] {r.status_code} received; attempting token refresh and retry once.")
        try:
            ensure_license_acceptance(timeout=timeout)
        except Exception as e:
            print(f"[warn] token refresh failed after {r.status_code}: {e!r}")
        r = s.request(method, url, params=params or {}, timeout=timeout or 30)

    return r


def api_get(path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> Dict[str, Any]:
    """
    GET wrapper that uses the shared session, includes Bearer auth,
    logs useful bits, and returns the parsed JSON.
    Raises for non-2xx after logging message (if present).
    """
    url = f"{_BASE}{path}" if not path.startswith("http") else path
    p = _remap_short_params(params or {})

    print(f"[DEBUG] GET {url} -> ...")
    r = _request_with_optional_refresh("GET", url, params=p, timeout=timeout)

    # Try JSON; if not JSON, raise on HTTP error then return empty dict.
    try:
        payload = r.json()
    except Exception:
        r.raise_for_status()
        return {}

    # Log basics
    meta = payload.get("meta", {})
    status = meta.get("status", {})
    code = status.get("id", r.status_code)
    print(f"[DEBUG] GET {url} -> {code}; keys={list(payload.keys())}")
    _maybe_log_server_message(payload)

    # Raise if server says it's an error (e.g., 400) — caller may catch.
    r.raise_for_status()
    return payload


def api_paginate(path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Simple paginator for endpoints that just return all rows in `data`.
    (Most of the Coverage API's /v1/data/* endpoints behave this way.)
    """
    payload = api_get(path, params=params, timeout=timeout)
    return list(payload.get("data") or [])
