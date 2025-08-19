# scripts/coverage_api.py
from __future__ import annotations

import os
import time
import typing as t
import urllib.parse
import requests

BASE_URL = "https://api.coverage.cms.gov/v1"

# Optional bearer token from the license-agreement endpoint.
# We tolerate missing token and proceed (public endpoints).
LICENSE_BEARER = os.environ.get("COVERAGE_LICENSE_BEARER", "").strip()

# ---- Utilities --------------------------------------------------------------

class ApiError(RuntimeError):
    pass


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if LICENSE_BEARER:
        h["Authorization"] = f"Bearer {LICENSE_BEARER}"
    return h


def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(1.5 * attempt, 6.0))


def _is_known_fake_400(payload: t.Any) -> bool:
    """
    The API returns 400 with a friendly message when you hit a non-existent route
    family like /data/lcd/code-table. Treat those as 'endpoint not available'.
    """
    if not isinstance(payload, dict):
        return False
    msg = str(payload.get("message", "")).lower()
    return "please reference the documentation" in msg or "swagger" in msg


def _get(path: str, params: dict | None = None, tolerate_404=False) -> dict:
    url = urllib.parse.urljoin(BASE_URL + "/", path.lstrip("/"))
    for attempt in range(1, 5):
        r = requests.get(url, params=params or {}, headers=_headers(), timeout=60)
        ct = r.headers.get("content-type", "")
        is_json = "json" in ct
        data = r.json() if is_json else {"status_code": r.status_code, "text": r.text}

        if r.status_code == 200:
            return data

        if r.status_code in (404,) and tolerate_404:
            return {"meta": {"status": {"id": 404, "message": "not found"}}, "data": []}

        # Known "friendly 400" when endpoint doesn’t exist
        if r.status_code == 400 and _is_known_fake_400(data):
            return {"meta": {"status": {"id": 400, "message": "endpoint not available"}}, "data": []}

        # Retry on 5xx and transient 4xx
        if r.status_code >= 500 or r.status_code in (408, 429):
            _sleep_backoff(attempt)
            continue

        # Hard error
        raise ApiError(f"GET {url} -> {r.status_code}: {data!r}")

    raise ApiError(f"GET {url} failed after retries.")

# ---- Article endpoints ------------------------------------------------------

def get_article(article_id: str, ver: str | None = None) -> dict:
    params = {"articleid": article_id}
    if ver:
        params["ver"] = ver
    return _get("/data/article/", params)

def get_article_code_table(article_id: str, ver: str | None = None) -> dict:
    params = {"articleid": article_id}
    if ver:
        params["ver"] = ver
    return _get("/data/article/code-table", params)

def get_article_icd10_covered(article_id: str, ver: str | None = None) -> dict:
    params = {"articleid": article_id}
    if ver:
        params["ver"] = ver
    return _get("/data/article/icd10-covered", params)

def get_article_icd10_noncovered(article_id: str, ver: str | None = None) -> dict:
    params = {"articleid": article_id}
    if ver:
        params["ver"] = ver
    return _get("/data/article/icd10-noncovered", params)

def get_article_icd10_pcs(article_id: str, ver: str | None = None) -> dict:
    params = {"articleid": article_id}
    if ver:
        params["ver"] = ver
    return _get("/data/article/icd10-pcs-code", params)

def get_article_hcpc_code(article_id: str, ver: str | None = None) -> dict:
    params = {"articleid": article_id}
    if ver:
        params["ver"] = ver
    return _get("/data/article/hcpc-code", params)

# ---- LCD endpoints (VALID ones only) ---------------------------------------

def get_lcd(lcdid: str, ver: str | None = None) -> dict:
    params = {"lcdid": lcdid}
    if ver:
        params["ver"] = ver
    return _get("/data/lcd/", params)

def get_lcd_hcpc_code(lcdid: str, ver: str | None = None) -> dict:
    params = {"lcdid": lcdid}
    if ver:
        params["ver"] = ver
    return _get("/data/lcd/hcpc-code", params)

def get_lcd_hcpc_code_group(lcdid: str, ver: str | None = None) -> dict:
    params = {"lcdid": lcdid}
    if ver:
        params["ver"] = ver
    return _get("/data/lcd/hcpc-code-group", params)

def get_lcd_contractor(lcdid: str, ver: str | None = None) -> dict:
    params = {"lcdid": lcdid}
    if ver:
        params["ver"] = ver
    return _get("/data/lcd/contractor", params)

def get_lcd_revision_history(lcdid: str, ver: str | None = None) -> dict:
    params = {"lcdid": lcdid}
    if ver:
        params["ver"] = ver
    return _get("/data/lcd/revision-history", params)

def get_lcd_primary_jurisdiction(lcdid: str, ver: str | None = None) -> dict:
    params = {"lcdid": lcdid}
    if ver:
        params["ver"] = ver
    return _get("/data/lcd/primary-jurisdiction", params)

# ---- Helpers used by run_once ----------------------------------------------

def get_codes_table_any(document_type: str, identifier: str, ver: str | None = None) -> dict:
    """
    Unified helper used by the pipeline:
      - For ARTICLES: returns SAD exclusion code table (/data/article/code-table)
      - For LCDs: there is NO equivalent 'code-table'; return an empty shape.
    """
    dt = document_type.lower().strip()
    if dt == "article":
        return get_article_code_table(identifier, ver)
    if dt == "lcd":
        # No such endpoint for LCDs; return empty structure but valid meta.
        return {"meta": {"status": {"id": 200, "message": "ok"}}, "data": []}
    raise ValueError(f"Unsupported document_type: {document_type}")

def get_icd10_any(document_type: str, identifier: str, ver: str | None = None) -> dict:
    """
    Returns ICD-10 data:
      - For ARTICLES: use icd10-covered + icd10-noncovered.
      - For LCDs: NO /data/lcd/icd10-* endpoints exist; return empty.
    """
    dt = document_type.lower().strip()
    if dt == "article":
        covered = get_article_icd10_covered(identifier, ver)
        noncov = get_article_icd10_noncovered(identifier, ver)
        pcs = get_article_icd10_pcs(identifier, ver)
        # Merge into a single payload for convenience
        return {
            "meta": {"status": {"id": 200, "message": "ok"}},
            "data": {
                "covered": covered.get("data", []),
                "noncovered": noncov.get("data", []),
                "pcs": pcs.get("data", []),
            },
        }
    if dt == "lcd":
        return {"meta": {"status": {"id": 200, "message": "ok"}}, "data": {"covered": [], "noncovered": [], "pcs": []}}
    raise ValueError(f"Unsupported document_type: {document_type}")
