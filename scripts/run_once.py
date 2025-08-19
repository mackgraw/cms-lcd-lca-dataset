# scripts/run_once.py
from __future__ import annotations

import csv
import os
from typing import Any, Dict, List, Tuple

import requests

from scripts.coverage_api import (
    ensure_license_acceptance,
    fetch_local_reports,
    get_article_tables,
    get_lcd_tables,
    build_params,  # might be useful later
)


def _env(name: str, default: str = "") -> str:
    # Accept empty/whitespace strings as "unset"
    val = os.getenv(name, default)
    if val is None:
        return ""
    s = str(val)
    return s if s.strip() else ""


def _echo_env():
    print(f"[PY-ENV] COVERAGE_STATES = {os.getenv('COVERAGE_STATES','')}")
    print(f"[PY-ENV] COVERAGE_STATUS = {os.getenv('COVERAGE_STATUS','')}")
    print(f"[PY-ENV] COVERAGE_CONTRACTORS = {os.getenv('COVERAGE_CONTRACTORS','')}")
    print(f"[PY-ENV] COVERAGE_MAX_DOCS = {os.getenv('COVERAGE_MAX_DOCS','')}")
    print(f"[PY-ENV] COVERAGE_TIMEOUT = {os.getenv('COVERAGE_TIMEOUT','')}")


def _mk_dirs():
    os.makedirs(".harvest_logs", exist_ok=True)
    os.makedirs("dataset", exist_ok=True)


def _doc_ids_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize IDs from report rows. Reports usually include:
    - document_id / document_display_id
    - document_type ('Article' or 'LCD')
    - document_version (int/str)
    """
    ids: Dict[str, Any] = {}
    # Most reports use these names:
    ids["document_id"] = row.get("document_id")
    ids["document_display_id"] = row.get("document_display_id")
    ids["document_type"] = row.get("document_type")
    ids["document_version"] = row.get("document_version")

    # Helpful aliases for data endpoints:
    if ids.get("document_type", "").lower() == "article":
        ids["article_id"] = ids.get("document_id")
        ids["article_display_id"] = ids.get("document_display_id")
    elif ids.get("document_type", "").lower() == "lcd":
        ids["lcd_id"] = ids.get("document_id")
        ids["lcd_display_id"] = ids.get("document_display_id")
    return ids


def _write_csv(path: str, headers: List[str], rows: List[List[Any]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(r)


def main() -> None:
    _mk_dirs()
    _echo_env()

    STATES = _env("COVERAGE_STATES")
    STATUS = _env("COVERAGE_STATUS")
    CONTRACTORS = _env("COVERAGE_CONTRACTORS")
    MAX_DOCS = _env("COVERAGE_MAX_DOCS")
    TIMEOUT = _env("COVERAGE_TIMEOUT")

    max_docs: int = int(MAX_DOCS) if MAX_DOCS.isdigit() else 0
    timeout: int = int(TIMEOUT) if TIMEOUT.isdigit() else 30

    sess = requests.Session()

    # License call (no timeout argument in function signature)
    ensure_license_acceptance(sess)

    # Fetch local reports with optional filters (empty strings are ignored)
    lcds, articles = fetch_local_reports(
        sess,
        states=STATES or None,
        status=STATUS or None,
        contractors=CONTRACTORS or None,
        timeout=timeout,
    )

    n_lcd = len(lcds)
    n_art = len(articles)
    print(f"[DEBUG] discovered {n_lcd} LCDs, {n_art} Articles (total {n_lcd + n_art})")

    # Limit number of docs if MAX_DOCS given
    if max_docs > 0:
        articles = articles[:max_docs]
        lcds = lcds[: max(0, max_docs - len(articles))]

    print(f"[DEBUG] processing {len(articles) + len(lcds)} documents")

    # We’ll aggregate “codes” vs “no-codes” rows simply based on whether any endpoint returned rows
    code_rows: List[List[Any]] = []
    nocode_rows: List[List[Any]] = []

    # Process articles
    for idx, row in enumerate(articles, start=1):
        doc_id = row.get("document_display_id") or row.get("document_id") or "?"
        print(f"[DEBUG] [Article {idx}/{len(articles)}] {doc_id}")
        ids = _doc_ids_from_row(row)
        tables = get_article_tables(sess, ids, timeout=timeout)

        # If any endpoint had rows, we’ll consider this a “code row present”
        total = sum(len(v) for v in tables.values())
        if total > 0:
            code_rows.append([ids.get("article_display_id") or ids.get("document_display_id") or "",
                              ids.get("article_id") or ids.get("document_id") or "",
                              ids.get("document_version") or ""])
        else:
            nocode_rows.append([ids.get("article_display_id") or ids.get("document_display_id") or "",
                                ids.get("article_id") or ids.get("document_id") or "",
                                ids.get("document_version") or ""])

    # Process LCDs (only endpoints we know are valid in swagger)
    for idx, row in enumerate(lcds, start=1):
        doc_id = row.get("document_display_id") or row.get("document_id") or "?"
        print(f"[DEBUG] [LCD {idx}/{len(lcds)}] {doc_id}")
        ids = _doc_ids_from_row(row)
        tables = get_lcd_tables(sess, ids, timeout=timeout)
        total = sum(len(v) for v in tables.values())
        if total > 0:
            code_rows.append([ids.get("lcd_display_id") or ids.get("document_display_id") or "",
                              ids.get("lcd_id") or ids.get("document_id") or "",
                              ids.get("document_version") or ""])
        else:
            nocode_rows.append([ids.get("lcd_display_id") or ids.get("document_display_id") or "",
                                ids.get("lcd_id") or ids.get("document_id") or "",
                                ids.get("document_version") or ""])

    # Always write both CSVs so artifacts step doesn’t fail
    codes_path = "dataset/document_codes_latest.csv"
    nocodes_path = "dataset/document_nocodes_latest.csv"
    _write_csv(codes_path, ["display_id", "id", "version"], code_rows)
    _write_csv(nocodes_path, ["display_id", "id", "version"], nocode_rows)
    print(f"[note] Wrote {len(code_rows)} code rows to {codes_path}")
    print(f"[note] Wrote {len(nocode_rows)} no-code rows to {nocodes_path}")


if __name__ == "__main__":
    main()
