from __future__ import annotations

import json
from typing import List, Optional, Dict, Any
from urllib.parse import urlencode
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

BASE_URL = "https://api.coverage.cms.gov"
ARTIFACTS_DIR = Path(__file__).resolve().parent.parent / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# Status mapping for Reports
# -------------------------
def _status_code(status: str | None) -> str:
    if not status:
        return "all"
    s = str(status).strip().lower()
    return {
        "active": "A",
        "retired": "R",
        "future": "F",
        "future effective": "F",
        "all": "all",
        "": "all",
    }.get(s, "all")


# -------------------------
# Robust list extractor
# -------------------------
_PREFERRED_KEYS = (
    # common
    "items", "results", "rows", "records",
    # CMS Coverage shapes
    "data", "codes", "codeTable", "table", "icd10Covered", "icd10Noncovered",
)

def _pick_list(d: Any) -> List[dict]:
    """
    Return a list of dicts from common API shapes. Recurses into nested dicts,
    so it works for {"meta":..., "data": {"items":[...]}} and similar.
    """
    # direct list
    if isinstance(d, list):
        # ensure list of dicts (sometimes it's list[str], which we still serialize)
        return d  # type: ignore[return-value]

    if not isinstance(d, dict):
        return []

    # try preferred keys (some may themselves be dicts)
    for k in _PREFERRED_KEYS:
        if k in d:
            v = d[k]
            if isinstance(v, list):
                return v  # type: ignore[return-value]
            if isinstance(v, dict):
                out = _pick_list(v)
                if out:
                    return out

    # fallback: search all dict values
    for v in d.values():
        if isinstance(v, list):
            return v  # type: ignore[return-value]
        if isinstance(v, dict):
            out = _pick_list(v)
            if out:
                return out

    return []


# -------------------------
# HTTP with debug dumps
# -------------------------
@retry(stop=stop_after_attempt(4), wait=wait_exponential_jitter(initial=1, max=10))
def _get(path: str, params: Optional[Dict[str, str]] = None, timeout: int = 30) -> Any:
    params = params or {}
    url = f"{BASE_URL}{path}"
    full_url = f"{url}{'?' + urlencode(params) if params else ''}"

    r = requests.get(url, params=params, timeout=timeout)
    status = r.status_code
    try:
        payload: Any = r.json()
    except Exception:
        payload = {"_non_json_text": r.text}

    # Write debug dump for inspection in Actions artifacts
    try:
        safe = path.strip("/").replace("/", "_")
        dump_name = f"debug_{safe}.json"
        with open(ARTIFACTS_DIR / dump_name, "w", encoding="utf-8") as f:
            json.dump({"full_url": full_url, "status": status, "data": payload}, f, ensure_ascii=False, indent=2)
        if isinstance(payload, dict):
            print(f"[DEBUG] GET {full_url} -> {status}; keys={list(payload.keys())[:6]}")
        elif isinstance(payload, list):
            print(f"[DEBUG] GET {full_url} -> {status}; list_len={len(payload)}")
        else:
            print(f"[DEBUG] GET {full_url} -> {status}; type={type(payload).__name__}")
    except Exception as e:
        print(f"[DEBUG] failed to write debug dump for {full_url}: {e}")

    r.raise_for_status()
    return payload


# -------------------------
# Reports (discovery)
# -------------------------
def list_final_lcds(
    states: Optional[List[str]] = None,
    status: Optional[str] = "all",
    contractors: Optional[List[str]] = None,
    timeout: int = 30,
) -> List[dict]:
    params: Dict[str, str] = {}
    if states:
        params["state"] = ",".join(states)
    if contractors:
        params["contractor"] = ",".join(contractors)
    sc = _status_code(status)
    if sc != "all":
        params["lcdStatus"] = sc  # CORRECT param

    data = _get("/v1/reports/local-coverage-final-lcds", params, timeout)
    return _pick_list(data)


def list_articles(
    states: Optional[List[str]] = None,
    status: Optional[str] = "all",
    contractors: Optional[List[str]] = None,
    timeout: int = 30,
) -> List[dict]:
    params: Dict[str, str] = {}
    if states:
        params["state"] = ",".join(states)
    if contractors:
        params["contractor"] = ",".join(contractors)
    sc = _status_code(status)
    if sc != "all":
        params["articleStatus"] = sc  # CORRECT param

    data = _get("/v1/reports/local-coverage-articles", params, timeout)
    return _pick_list(data)


# -------------------------
# Document detail (LCD)
# -------------------------
def get_lcd(lcd_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/lcd", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)


def get_lcd_revision_history(lcd_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/lcd/revision-history", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)
    return _pick_list(data)


# -------------------------
# Document detail (Article)
# -------------------------
# Pass BOTH snake_case and camelCase to be safe
def get_article(article_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/article", {"article_id": article_id, "articleId": article_id}, timeout)


def get_article_revision_history(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/revision-history", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_article_codes_table(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/code-table", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_article_icd10_covered(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/icd10-covered", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_article_icd10_noncovered(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/icd10-noncovered", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_article_hcpc_codes(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/hcpc-code", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_article_hcpc_modifiers(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/hcpc-modifier", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_article_revenue_codes(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/revenue-code", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_article_bill_types(article_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/article/bill-codes", {"article_id": article_id, "articleId": article_id}, timeout)
    return _pick_list(data)


def get_update_period(timeout: int = 30) -> dict:
    return _get("/v1/metadata/update-period/", None, timeout)
