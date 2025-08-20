# scripts/run_once.py
from __future__ import annotations

import csv
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from scripts.coverage_api import (
    ensure_license_acceptance,
    get_article_codes,
    get_article_modifiers,
    get_article_icd10_covered,
    get_article_icd10_noncovered,
    get_article_revenue_codes,
    get_article_bill_codes,
    get_article_code_table,
    get_lcd_codes,
    get_lcd_icd10_covered,
    get_lcd_icd10_noncovered,
)

BASE = "https://api.coverage.cms.gov"


# -----------------------------
# Utilities & logging
# -----------------------------
def _debug(msg: str) -> None:
    print(f"[DEBUG] {msg}")


def _note(msg: str) -> None:
    print(f"[note] {msg}")


def _get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None) -> Dict[str, Any]:
    _debug(f"GET {url} -> ...")
    r = requests.get(url, params=params or {}, timeout=timeout)
    try:
        j = r.json()
    except Exception:
        j = {}
    # compact log
    if isinstance(j, dict):
        keys = list(j.keys())
        if "message" in j:
            _debug(f"GET {url} -> {r.status_code}; keys={keys}; message={j['message']}")
        else:
            _debug(f"GET {url} -> {r.status_code}; keys={keys}")
    else:
        _debug(f"GET {url} -> {r.status_code}")
    return j or {}


def _env_raw(name: str) -> Optional[str]:
    """Return env var, treating None, '', whitespace-only as None."""
    v = os.environ.get(name)
    if v is None:
        return None
    if v.strip() == "":
        return None
    return v


def _env_list(name: str, sep: str = ",") -> List[str]:
    v = _env_raw(name)
    if not v:
        return []
    return [s.strip() for s in v.split(sep) if s.strip()]


def _env_int(name: str) -> Optional[int]:
    v = _env_raw(name)
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


# -----------------------------
# Report fetching & filtering
# -----------------------------
def fetch_local_report(slug: str, timeout: Optional[int]) -> List[Dict[str, Any]]:
    """
    slug examples:
      'local-coverage-final-lcds'
      'local-coverage-articles'
    """
    url = f"{BASE}/v1/reports/{slug}"
    obj = _get_json(url, timeout=timeout)
    return obj.get("data", []) or []


def filter_rows(
    rows: Iterable[Dict[str, Any]],
    states: List[str],
    contractors: List[str],
) -> List[Dict[str, Any]]:
    """
    Filters on simple substrings within 'contractor_name_type' for states or contractor names.
    If filters are empty, returns rows unchanged.
    """
    out: List[Dict[str, Any]] = []
    want_states = [s.upper() for s in states]
    want_contractors = [c.lower() for c in contractors]

    for r in rows:
        cval = (r.get("contractor_name_type") or "")
        cval_l = cval.lower()
        cval_u = cval.upper()

        ok_state = True
        ok_contractor = True

        if want_states:
            ok_state = any(s in cval_u for s in want_states)

        if want_contractors:
            ok_contractor = any(k in cval_l for k in want_contractors)

        if ok_state and ok_contractor:
            out.append(r)

    return out


# -----------------------------
# Row -> IDs map
# -----------------------------
def ids_for_article(row: Dict[str, Any]) -> Dict[str, Any]:
    # The API wants articleid + ver
    return {
        "article_id": row.get("document_id"),
        "document_version": row.get("document_version"),
    }


def ids_for_lcd(row: Dict[str, Any]) -> Dict[str, Any]:
    # The API wants lcdid + ver
    return {
        "lcd_id": row.get("document_id"),
        "document_version": row.get("document_version"),
    }


# -----------------------------
# Code harvesting
# -----------------------------
ARTICLE_ENDPOINTS = [
    ("code-table", get_article_code_table),
    ("icd10-covered", get_article_icd10_covered),
    ("icd10-noncovered", get_article_icd10_noncovered),
    ("hcpc-code", get_article_codes),
    ("hcpc-modifier", get_article_modifiers),
    ("revenue-code", get_article_revenue_codes),
    ("bill-codes", get_article_bill_codes),
]

LCD_ENDPOINTS = [
    ("hcpc-code", get_lcd_codes),
    ("icd10-covered", get_lcd_icd10_covered),
    ("icd10-noncovered", get_lcd_icd10_noncovered),
]


def harvest_article_codes(row: Dict[str, Any], timeout: Optional[int]) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Return (code_rows, had_any_codes)
    """
    ids = ids_for_article(row)
    display = row.get("document_display_id") or row.get("title") or ""
    print(f"[DEBUG] [Article] {display}")

    had_any = False
    rows_out: List[Dict[str, Any]] = []

    for label, func in ARTICLE_ENDPOINTS:
        data = func(ids, timeout=timeout)
        # logging in coverage_api already prints page meta + counts
        if data:
            had_any = True
            for d in data:
                rows_out.append(
                    {
                        "document_type": "Article",
                        "document_id": row.get("document_id"),
                        "document_display_id": row.get("document_display_id"),
                        "document_version": row.get("document_version"),
                        "endpoint": label,
                        "payload": json.dumps(d, ensure_ascii=False),
                    }
                )

    return rows_out, had_any


def harvest_lcd_codes(row: Dict[str, Any], timeout: Optional[int]) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Return (code_rows, had_any_codes)
    """
    ids = ids_for_lcd(row)
    display = row.get("document_display_id") or row.get("title") or ""
    print(f"[DEBUG] [LCD] {display}")

    had_any = False
    rows_out: List[Dict[str, Any]] = []

    for label, func in LCD_ENDPOINTS:
        data = func(ids, timeout=timeout)
        if data:
            had_any = True
            for d in data:
                rows_out.append(
                    {
                        "document_type": "LCD",
                        "document_id": row.get("document_id"),
                        "document_display_id": row.get("document_display_id"),
                        "document_version": row.get("document_version"),
                        "endpoint": label,
                        "payload": json.dumps(d, ensure_ascii=False),
                    }
                )

    return rows_out, had_any


# -----------------------------
# CSV I/O
# -----------------------------
def write_codes_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        "document_type",
        "document_id",
        "document_display_id",
        "document_version",
        "endpoint",
        "payload",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_nocodes_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        "document_type",
        "document_id",
        "document_display_id",
        "document_version",
        "note",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    # Env parsing (treat blanks as unset)
    states = _env_list("COVERAGE_STATES")
    status = (_env_raw("COVERAGE_STATUS") or "").lower()  # currently unused; reports are per type
    contractors = _env_list("COVERAGE_CONTRACTORS")
    max_docs = _env_int("COVERAGE_MAX_DOCS")
    timeout = _env_int("COVERAGE_TIMEOUT")

    # Print the environment as the logs show
    print(f"[PY-ENV] COVERAGE_STATES = {','.join(states) if states else ''}")
    print(f"[PY-ENV] COVERAGE_STATUS = {status or ''}")
    print(f"[PY-ENV] COVERAGE_CONTRACTORS = {' '.join(contractors) if contractors else ''} ")
    print(f"[PY-ENV] COVERAGE_MAX_DOCS = {str(max_docs) if max_docs is not None else ''}")
    print(f"[PY-ENV] COVERAGE_TIMEOUT = {str(timeout) if timeout is not None else ''}")

    # License
    ensure_license_acceptance()

    # Reports
    _debug("trying /reports/local-coverage-final-lcds (no status)")
    lcd_rows = fetch_local_report("local-coverage-final-lcds", timeout)
    _debug("trying /reports/local-coverage-articles (no status)")
    art_rows = fetch_local_report("local-coverage-articles", timeout)

    # Simple filtering by state/contractor if those envs are supplied
    if states or contractors:
        lcd_rows = filter_rows(lcd_rows, states, contractors)
        art_rows = filter_rows(art_rows, states, contractors)

    n_lcd = len(lcd_rows)
    n_art = len(art_rows)
    _debug(f"discovered {n_lcd} LCDs, {n_art} Articles (total {n_lcd + n_art})")

    # Respect max docs if provided
    if max_docs is not None and max_docs >= 0:
        # We'll allocate roughly half to articles, half to LCDs, but keep it simple:
        half = max_docs // 2
        art_rows = art_rows[:half] if half > 0 else []
        lcd_rows = lcd_rows[: (max_docs - len(art_rows))]
        n_lcd = len(lcd_rows)
        n_art = len(art_rows)

    total = n_lcd + n_art
    _debug(f"processing {total} documents")

    code_rows: List[Dict[str, Any]] = []
    nocode_rows: List[Dict[str, Any]] = []

    # Articles first
    for idx, row in enumerate(art_rows, start=1):
        display = row.get("document_display_id") or row.get("title") or ""
        print(f"[DEBUG] [Article {idx}/{len(art_rows)}] {display}")
        rows_out, had_any = harvest_article_codes(row, timeout)
        if rows_out:
            code_rows.extend(rows_out)
        if not had_any:
            nocode_rows.append(
                {
                    "document_type": "Article",
                    "document_id": row.get("document_id"),
                    "document_display_id": row.get("document_display_id"),
                    "document_version": row.get("document_version"),
                    "note": "No codes returned from article endpoints",
                }
            )

    # LCDs
    for idx, row in enumerate(lcd_rows, start=1):
        display = row.get("document_display_id") or row.get("title") or ""
        print(f"[DEBUG] [LCD {idx}/{len(lcd_rows)}] {display}")
        rows_out, had_any = harvest_lcd_codes(row, timeout)
        if rows_out:
            code_rows.extend(rows_out)
        if not had_any:
            nocode_rows.append(
                {
                    "document_type": "LCD",
                    "document_id": row.get("document_id"),
                    "document_display_id": row.get("document_display_id"),
                    "document_version": row.get("document_version"),
                    "note": "No codes returned from lcd endpoints",
                }
            )

    # Write outputs
    os.makedirs("dataset", exist_ok=True)
    codes_path = "dataset/document_codes_latest.csv"
    nocodes_path = "dataset/document_nocodes_latest.csv"

    write_codes_csv(codes_path, code_rows)
    write_nocodes_csv(nocodes_path, nocode_rows)
    _note(f"Wrote {len(code_rows)} code rows to {codes_path}")
    _note(f"Wrote {len(nocode_rows)} no-code rows to {nocodes_path}")


if __name__ == "__main__":
    # Allow a no-op run for CI sanity checks when *no* example IDs or reports are intended
    # (kept simple — we always try to run; if environment is empty, it still works).
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
