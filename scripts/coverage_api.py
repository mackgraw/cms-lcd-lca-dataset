# scripts/coverage_api.py
from __future__ import annotations
import os, time, typing as t
from dataclasses import dataclass
import requests
from requests import Response

BASE_URL = "https://api.coverage.cms.gov"
SESSION = requests.Session()

@dataclass
class LicenseToken:
    token: t.Optional[str] = None
    obtained_at: float = 0.0
    ttl_seconds: int = 3600  # ~1 hour

    def is_fresh(self) -> bool:
        if not self.token:
            return False
        return (time.time() - self.obtained_at) < (self.ttl_seconds - 300)  # refresh ~55 min

TOKEN_CACHE = LicenseToken()

def _short_err(msg: str, limit: int = 120) -> str:
    msg = (msg or "").strip().replace("\n", " ")
    return (msg[:limit] + ("…" if len(msg) > limit else ""))

def _full_url(path: str) -> str:
    if path.startswith("http"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return BASE_URL + path

def _headers() -> dict:
    return {"Authorization": f"Bearer {TOKEN_CACHE.token}"} if TOKEN_CACHE.token else {}

def ensure_license_acceptance(timeout: int | float | None = None) -> None:
    if TOKEN_CACHE.is_fresh():
        return
    j = _get_json("GET", "/v1/metadata/license-agreement", timeout=timeout)
    token_val = None
    try:
        data = j.get("data") or []
        if data and isinstance(data, list) and isinstance(data[0], dict):
            token_val = data[0].get("Token")
    except Exception:
        token_val = None
    if token_val:
        TOKEN_CACHE.token = token_val
        TOKEN_CACHE.obtained_at = time.time()
    else:
        # no token returned is still OK for public endpoints; mark time to avoid hammering
        TOKEN_CACHE.obtained_at = time.time()

def _request(method: str, path: str, *, params: dict | None = None, timeout: int | float | None = None) -> Response:
    url = _full_url(path)
    resp = SESSION.request(method=method, url=url, params=params or {}, headers=_headers(), timeout=timeout)
    if resp.status_code in (401, 403):
        ensure_license_acceptance(timeout=timeout)
        resp = SESSION.request(method=method, url=url, params=params or {}, headers=_headers(), timeout=timeout)
    return resp

def _get_json(method: str, path: str, *, params: dict | None = None, timeout: int | float | None = None) -> dict:
    resp = _request(method, path, params=params, timeout=timeout)
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise
    if resp.status_code >= 400:
        message = data.get("message") if isinstance(data, dict) else None
        raise RuntimeError(f"{resp.status_code} for {path}: {_short_err(str(message) if message else resp.text)}")
    return data

def fetch_local_reports(
    states: list[str] | None = None,
    status: str | None = None,
    contractors: list[str] | None = None,
    timeout: int | float | None = None,
) -> tuple[list[dict], list[dict]]:
    ensure_license_acceptance(timeout=timeout)

    lcds_json = _get_json("GET", "/v1/reports/local-coverage-final-lcds", timeout=timeout)
    arts_json = _get_json("GET", "/v1/reports/local-coverage-articles", timeout=timeout)

    lcds = lcds_json.get("data", []) if isinstance(lcds_json, dict) else []
    arts = arts_json.get("data", []) if isinstance(arts_json, dict) else []

    def _match_states(row: dict) -> bool:
        if not states:
            return True
        s = (row.get("contractor_name_type") or "") + " " + (row.get("title") or "")
        return any(st.strip().upper() in s.upper() for st in states)

    def _match_status(row: dict) -> bool:
        if not status or status.lower() == "all":
            return True
        return True

    def _match_contractors(row: dict) -> bool:
        if not contractors:
            return True
        s = (row.get("contractor_name_type") or "")
        return any(c.lower() in s.lower() for c in contractors)

    lcds_f = [r for r in lcds if _match_states(r) and _match_status(r) and _match_contractors(r)]
    arts_f = [r for r in arts if _match_states(r) and _match_status(r) and _match_contractors(r)]
    return (lcds_f, arts_f)

def _try_article_param_sets(article_id: int | None, document_version: int | None) -> list[dict]:
    """Only use articleid (+ optional ver). Using articledisplayid causes 400s."""
    params_sets: list[dict] = []
    aid = int(article_id) if (article_id is not None and str(article_id).isdigit()) else None
    ver = int(document_version) if (document_version is not None and str(document_version).isdigit()) else None

    if aid and ver:
        params_sets.append({"articleid": aid, "ver": ver})
    if aid:
        params_sets.append({"articleid": aid})
    # no (no-params) fallback anymore for article data endpoints (they demand articleid)
    return params_sets

def _collect_article_endpoint_rows(path: str, param_sets: list[dict], timeout: int | float | None) -> list[dict]:
    rows: list[dict] = []
    for p in param_sets:
        try:
            j = _get_json("GET", path, params=p, timeout=timeout)
        except RuntimeError as e:
            print(f"[DEBUG]   -> {path} with {','.join(f'{k}={v}' for k,v in p.items()) or '(no-params)'}: {e}")
            # if server says invalid articleid, no point trying other shapes—just continue to next article/endpoint
            if "invalid articleid" in str(e).lower() or "you must include an articleid" in str(e).lower():
                break
            continue
        meta = (j or {}).get("meta", {})
        data = (j or {}).get("data", [])
        print("[DEBUG]   page meta:", list(meta.keys()))
        print(f"[DEBUG]   -> {_full_url(path)[len(BASE_URL):]} with "
              f"{','.join(f'{k}={v}' for k,v in p.items()) or '(no-params)'}: {len(data) if isinstance(data, list) else 0} rows")
        if isinstance(data, list) and data:
            rows.extend(data)
            break  # found rows for this endpoint; stop trying fallback param shapes
    return rows

def get_article_codes(
    *,
    article_id: int | None = None,
    article_display_id: str | None = None,  # kept for signature compatibility; not sent
    document_version: int | None = None,
    timeout: int | float | None = None,
) -> dict[str, list[dict]]:
    ensure_license_acceptance(timeout=timeout)

    param_sets = _try_article_param_sets(article_id, document_version)

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
        out[name] = _collect_article_endpoint_rows(path, param_sets, timeout)
    return out

# Placeholder for LCDs if you decide to wire them up later.
def get_lcd_codes(*, lcd_id: int | None = None, lcd_display_id: str | None = None,
                  document_version: int | None = None, timeout: int | float | None = None) -> dict[str, list[dict]]:
    ensure_license_acceptance(timeout=timeout)
    return {}
