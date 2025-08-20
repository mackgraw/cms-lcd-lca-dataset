# scripts/coverage_api.py
from __future__ import annotations

import os
import time
import typing as t
from dataclasses import dataclass, field

import requests
from requests import Response

BASE_URL = "https://api.coverage.cms.gov"
SESSION = requests.Session()

# In-memory token cache (simple and CI-safe)
@dataclass
class LicenseToken:
    token: t.Optional[str] = None
    obtained_at: float = 0.0
    ttl_seconds: int = 3600  # CMS says ~1 hour

    def is_fresh(self) -> bool:
        if not self.token:
            return False
        # refresh proactively at 55 minutes
        return (time.time() - self.obtained_at) < (self.ttl_seconds - 300)

TOKEN_CACHE = LicenseToken()

# ---------- Utilities ----------

def _short_err(msg: str, limit: int = 120) -> str:
    """Trim server error messages for logs."""
    msg = (msg or "").strip().replace("\n", " ")
    return (msg[:limit] + ("…" if len(msg) > limit else ""))

def _full_url(path: str) -> str:
    if path.startswith("http"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return BASE_URL + path

def _headers() -> dict:
    # Some endpoints may not require the token at all,
    # but if we have one, include it in a conservative, non-breaking way.
    # The API docs' exact header name is redacted in the notes text,
    # so we *only* include it as Authorization if present.
    if TOKEN_CACHE.token:
        return {"Authorization": f"Bearer {TOKEN_CACHE.token}"}
    return {}

def _request(method: str, path: str, *, params: dict | None = None, timeout: int | float | None = None) -> Response:
    url = _full_url(path)
    resp = SESSION.request(method=method, url=url, params=params or {}, headers=_headers(), timeout=timeout)
    # If auth/token is required and we got rejected, try to refresh once and retry
    if resp.status_code in (401, 403):
        # refresh token then retry once
        ensure_license_acceptance()
        resp = SESSION.request(method=method, url=url, params=params or {}, headers=_headers(), timeout=timeout)
    return resp

def _get_json(method: str, path: str, *, params: dict | None = None, timeout: int | float | None = None) -> dict:
    resp = _request(method, path, params=params, timeout=timeout)
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        # If not JSON and not an HTTP error, raise anyway
        raise

    # If the server embeds errors as JSON with 'message'
    if resp.status_code >= 400:
        message = data.get("message") if isinstance(data, dict) else None
        raise RuntimeError(f"{resp.status_code} for {path}: {_short_err(str(message) if message else resp.text)}")

    return data

# ---------- Public API ----------

def ensure_license_acceptance(timeout: int | float | None = None) -> None:
    """
    Call the metadata license endpoint. Cache the token (if one is returned)
    and refresh it when stale. Safe to call as often as you like.
    """
    if TOKEN_CACHE.is_fresh():
        return

    j = _get_json("GET", "/v1/metadata/license-agreement", timeout=timeout)

    # Common success shape from CMS:
    # {
    #   "meta": { "status": {"id": 200, "message": "OK"}, "notes": "...Please use this token as a ***..." , ... },
    #   "data": [{"Token": "<uuid>"}]
    # }
    token_val = None
    try:
        data = j.get("data") or []
        if data and isinstance(data, list) and isinstance(data[0], dict):
            token_val = data[0].get("Token")
    except Exception:
        token_val = None

    TOKEN_CACHE.token = token_val or TOKEN_CACHE.token
    TOKEN_CACHE.obtained_at = time.time()

def fetch_local_reports(
    states: list[str] | None = None,
    status: str | None = None,
    contractors: list[str] | None = None,
    timeout: int | float | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Return (final_lcds, articles) from the 'reports' endpoints.
    Filters are applied client-side (CMS reports endpoints don’t support all filters).
    """
    ensure_license_acceptance(timeout=timeout)

    # Pull both lists
    lcds_json = _get_json("GET", "/v1/reports/local-coverage-final-lcds", timeout=timeout)
    arts_json = _get_json("GET", "/v1/reports/local-coverage-articles", timeout=timeout)

    lcds = lcds_json.get("data", []) if isinstance(lcds_json, dict) else []
    arts = arts_json.get("data", []) if isinstance(arts_json, dict) else []

    # Optional client-side filters
    def _match_states(row: dict) -> bool:
        if not states:
            return True
        # common fields: 'contractor_name_type' often includes state info (e.g., "First Coast Service Options, Inc. (FL)")
        s = (row.get("contractor_name_type") or "") + " " + (row.get("title") or "")
        return any(st.strip().upper() in s.upper() for st in states)

    def _match_status(row: dict) -> bool:
        if not status or status.lower() == "all":
            return True
        # reports endpoints typically don't include an explicit status for "final" lists;
        # leave as True unless you later supplement with more metadata.
        return True

    def _match_contractors(row: dict) -> bool:
        if not contractors:
            return True
        s = (row.get("contractor_name_type") or "")
        return any(c.lower() in s.lower() for c in contractors)

    lcds_f = [r for r in lcds if _match_states(r) and _match_status(r) and _match_contractors(r)]
    arts_f = [r for r in arts if _match_states(r) and _match_status(r) and _match_contractors(r)]
    return (lcds_f, arts_f)

def _try_article_param_sets(article_id: t.Optional[int], article_display_id: t.Optional[str], document_version: t.Optional[int]) -> list[dict]:
    """
    The article 'data' endpoints accept different parameter names depending on the table.
    From your logs, 'articleid' and 'ver' worked (but returned 0 rows on those examples).
    We’ll try a small set of permutations.
    """
    params_sets: list[dict] = []

    aid = int(article_id) if (article_id is not None and str(article_id).isdigit()) else None
    ver = int(document_version) if (document_version is not None and str(document_version).isdigit()) else None
    disp = (article_display_id or "").strip() or None

    # Highest-confidence combos first:
    if aid and ver:
        params_sets.append({"articleid": aid, "ver": ver})
    if disp and ver:
        params_sets.append({"articledisplayid": disp, "ver": ver})

    # Looser (ID-only) fallbacks:
    if aid:
        params_sets.append({"articleid": aid})
    if disp:
        params_sets.append({"articledisplayid": disp})

    # Final fallback: nothing (some endpoints allow paging everything, but we expect 0)
    params_sets.append({})
    return params_sets

def _collect_article_endpoint_rows(path: str, param_sets: list[dict], timeout: int | float | None) -> list[dict]:
    rows: list[dict] = []
    for p in param_sets:
        try:
            j = _get_json("GET", path, params=p, timeout=timeout)
        except RuntimeError as e:
            # Print concise server msg and continue
            print(f"[DEBUG]   -> {path} with {','.join(f'{k}={v}' for k,v in p.items()) or '(no-params)'}: {e}")
            continue
        meta = (j or {}).get("meta", {})
        data = (j or {}).get("data", [])
        status = ((meta or {}).get("status") or {}).get("id")
        notes = (meta or {}).get("notes")
        fields = (meta or {}).get("fields")
        children = (meta or {}).get("children")
        print("[DEBUG]   page meta:", list(k for k in meta.keys()))
        print(f"[DEBUG]   -> {_full_url(path)[len(BASE_URL):]} with "
              f"{','.join(f'{k}={v}' for k,v in p.items()) or '(no-params)'}: {len(data)} rows")
        if isinstance(data, list) and data:
            rows.extend(data)
        # stop early if we found something
        if rows:
            break
    return rows

def get_article_codes(
    *,
    article_id: int | None = None,
    article_display_id: str | None = None,
    document_version: int | None = None,
    timeout: int | float | None = None,
) -> dict[str, list[dict]]:
    """
    Pull code tables for a single Article across the relevant endpoints.
    Returns a dict { endpoint_name -> [rows] }
    """
    ensure_license_acceptance(timeout=timeout)

    param_sets = _try_article_param_sets(article_id, article_display_id, document_version)

    endpoints = {
        "code-table": "/v1/data/article/code-table",
        "icd10-covered": "/v1/data/article/icd10-covered",
        "icd10-noncovered": "/v1/data/article/icd10-noncovered",
        "hcpc-code": "/v1/data/article/hcpc-code",
        "hcpc-modifier": "/v1/data/article/hcpc-modifier",
        "revenue-code": "/v1/data/article/revenue-code",
        "bill-codes": "/v1/data/article/bill-codes",
    }

    out: dict[str, list[dict]] = {}
    for name, path in endpoints.items():
        print(f"[DEBUG] GET {BASE_URL}{path} -> ...")
        rows = _collect_article_endpoint_rows(path, param_sets, timeout)
        out[name] = rows

    return out

# (Optional) Stub for LCD code pulls, if you wire it later.
def get_lcd_codes(
    *,
    lcd_id: int | None = None,
    lcd_display_id: str | None = None,
    document_version: int | None = None,
    timeout: int | float | None = None,
) -> dict[str, list[dict]]:
    """
    Placeholder for LCD code pulls — add real LCD endpoints you plan to query.
    Keeping the shape consistent with get_article_codes for drop-in use.
    """
    ensure_license_acceptance(timeout=timeout)

    # Add the LCD endpoints you need, similar to the article ones.
    endpoints = {
        # Example (uncomment when you confirm):
        # "hcpc-code": "/v1/data/lcd/hcpc-code",
        # "icd10-covered": "/v1/data/lcd/icd10-covered",
        # "icd10-noncovered": "/v1/data/lcd/icd10-noncovered",
        # ...
    }
    out: dict[str, list[dict]] = {}
    for name, path in endpoints.items():
        print(f"[DEBUG] GET {BASE_URL}{path} -> ...")
        # implement a _try_lcd_param_sets() if needed; for now, empty
        try:
            j = _get_json("GET", path, params={}, timeout=timeout)
            out[name] = (j or {}).get("data", []) or []
        except RuntimeError as e:
            print(f"[DEBUG]   -> {path} error: {e}")
            out[name] = []
    return out
