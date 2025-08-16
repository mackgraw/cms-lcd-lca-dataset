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

def _pick_list(data: Any) -> List[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "data", "results", "rows"):
            v = data.get(key)
            if isinstance(v, list):
                return v
    return []

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

    # Write debug dump so we can inspect exact shapes from Actions artifacts
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

# -------- Reports (discovery) --------
def list_final_lcds(states: Optional[List[str]] = None,
                    status: Optional[str] = "all",
                    contractors: Optional[List[str]] = None,
                    timeout: int = 30) -> List[dict]:
    params: Dict[str, str] = {}
    if states:
        params["state"] = ",".join(states)
    if contractors:
        params["contractor"] = ",".join(contractors)
    sc = _status_code(status)
    if sc != "all":
        params["lcdStatus"] = sc
    data = _get("/v1/reports/local-coverage-final-lcds", params, timeout)
    return _pick_list(data)

def list_articles(states: Optional[List[str]] = None,
                  status: Optional[str] = "all",
                  contractors: Optional[List[str]] = None,
                  timeout: int = 30) -> List[dict]:
    params: Dict[str, str] = {}
    if states:
        params["state"] = ",".join(states)
    if contractors:
        params["contractor"] = ",".join(contractors)
    sc = _status_code(status)
    if sc != "all":
        params["articleStatus"] = sc
    data = _get("/v1/reports/local-coverage-articles", params, timeout)
    return _pick_list(data)

# -------- Document detail (LCD) --------
def get_lcd(lcd_id: str, timeout: int = 30) -> dict:
    return _get("/v1/data/lcd", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)

def get_lcd_revision_history(lcd_id: str, timeout: int = 30) -> List[dict]:
    data = _get("/v1/data/lcd/revision-history", {"lcd_id": lcd_id, "lcdId": lcd_id}, timeout)
    return _pick_list(data)

# -------- Document detail (Article) --------
# NOTE: pass BOTH snake_case and camelCase param names to be safe.
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
