#!/usr/bin/env python3
"""
Harvest a single pass of Coverage API data with optional filters from env vars.
Empty or missing env vars are treated as "no filter".
"""

from __future__ import annotations
import os
import sys
from typing import Optional, Dict, Any, Iterable, Tuple

from scripts.coverage_api import (
    ensure_license_acceptance,
    fetch_local_reports,
    fetch_article_subresource_rows,
    fetch_lcd_subresource_rows,
)

# ----------------------------
# Env handling (empty-safe)
# ----------------------------

def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return None if env var is missing or empty/whitespace; else the stripped value."""
    val = os.getenv(name, None)
    if val is None:
        return default
    val = val.strip()
    if val == "":
        return None
    return val

def _get_env_int(name: str, default: int = 0) -> int:
    val = _get_env(name)
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        return default

COVERAGE_STATES      = _get_env("COVERAGE_STATES")        # e.g. "FL,PA" or None
COVERAGE_STATUS      = _get_env("COVERAGE_STATUS")        # e.g. "final|proposed|all" or None
COVERAGE_CONTRACTORS = _get_env("COVERAGE_CONTRACTORS")   # e.g. "First Coast,Novitas" or None
COVERAGE_MAX_DOCS    = _get_env_int("COVERAGE_MAX_DOCS", 0)     # 0 = unlimited
COVERAGE_TIMEOUT     = _get_env_int("COVERAGE_TIMEOUT", 0)      # 0 = default

def _env_echo():
    print(f"[PY-ENV] COVERAGE_STATES = {COVERAGE_STATES or 'ALL'}")
    print(f"[PY-ENV] COVERAGE_STATUS = {COVERAGE_STATUS or 'ALL'}")
    print(f"[PY-ENV] COVERAGE_CONTRACTORS = {COVERAGE_CONTRACTORS or 'ALL'}")
    print(f"[PY-ENV] COVERAGE_MAX_DOCS = {COVERAGE_MAX_DOCS or '∞'}")
    print(f"[PY-ENV] COVERAGE_TIMEOUT = {COVERAGE_TIMEOUT or 'default'}")

# ----------------------------
# Small helpers
# ----------------------------

def _mkdirs():
    os.makedirs(".harvest_logs", exist_ok=True)
    os.makedirs("dataset", exist_ok=True)

def _maybe_split_csv(s: Optional[str]) -> Optional[Iterable[str]]:
    """Return None (no filter) or an iterable of cleaned tokens."""
    if not s:
        return None
    parts = [p.strip() for p in s.split(",")]
    parts = [p for p in parts if p]
    return parts or None

# ----------------------------
# Main
# ----------------------------

def main() -> None:
    _mkdirs()
    _env_echo()

    # Treat empty envs as "no filter"
    states      = _maybe_split_csv(COVERAGE_STATES)
    contractors = _maybe_split_csv(COVERAGE_CONTRACTORS)
    status      = COVERAGE_STATUS or None
    max_docs    = COVERAGE_MAX_DOCS if COVERAGE_MAX_DOCS > 0 else None
    timeout     = COVERAGE_TIMEOUT if COVERAGE_TIMEOUT > 0 else None

    # 1) Accept license (no-op if not needed)
    ensure_license_acceptance(timeout=timeout)

    # 2) Get local-coverage reports (articles + final LCDs) honoring filters if provided
    reports = fetch_local_reports(
        states=states,
        status=status,
        contractors=contractors,
        timeout=timeout,
    )

    total_lcds = len(reports.get("lcds", []))
    total_articles = len(reports.get("articles", []))
    print(f"[DEBUG] discovered {total_lcds} LCDs, {total_articles} Articles (total {total_lcds + total_articles})")

    # 3) Limit if requested
    articles = reports.get("articles", [])
    lcds     = reports.get("lcds", [])

    if max_docs:
        articles = articles[:max_docs]
        lcds     = lcds[:max_docs]

    # 4) Iterate and pull subresources
    code_rows: list[Dict[str, Any]] = []
    nocode_rows: list[Dict[str, Any]] = []

    def _row_id_debug(row: Dict[str, Any]) -> Tuple[Optional[int], Optional[str], Optional[int], Optional[str], Optional[int]]:
        # Normalize common identifiers from report rows
        return (
            row.get("document_id"),
            row.get("document_display_id"),
            row.get("article_id") or row.get("lcd_id"),
            row.get("article_display_id") or row.get("lcd_display_id"),
            row.get("document_version"),
        )

    # Articles
    for idx, row in enumerate(articles, start=1):
        doc_id, doc_disp, art_or_lcd_id, art_or_lcd_disp, version = _row_id_debug(row)
        print(f"[DEBUG] [Article {idx}/{len(articles)}] {row.get('title','')}  id={art_or_lcd_id}  display={art_or_lcd_disp}")
        rows = fetch_article_subresource_rows(
            # Pass all we know; API client will auto-drop Nones/empties.
            article_id=row.get("article_id"),
            article_display_id=row.get("article_display_id"),
            document_id=row.get("document_id"),
            document_display_id=row.get("document_display_id"),
            document_version=row.get("document_version"),
            timeout=timeout,
        )
        if rows:
            code_rows.extend(rows)
        else:
            nocode_rows.append(row)

    # LCDs
    for idx, row in enumerate(lcds, start=1):
        print(f"[DEBUG] [LCD {idx}/{len(lcds)}] {row.get('title','')}  id={row.get('lcd_id')}  display={row.get('lcd_display_id')}")
        rows = fetch_lcd_subresource_rows(
            lcd_id=row.get("lcd_id"),
            lcd_display_id=row.get("lcd_display_id"),
            document_id=row.get("document_id"),
            document_display_id=row.get("document_display_id"),
            document_version=row.get("document_version"),
            timeout=timeout,
        )
        if rows:
            code_rows.extend(rows)
        else:
            nocode_rows.append(row)

    # 5) Write outputs
    import csv

    codes_csv = "dataset/document_codes_latest.csv"
    nocodes_csv = "dataset/document_nocodes_latest.csv"

    # Write codes
    with open(codes_csv, "w", newline="", encoding="utf-8") as f:
        if code_rows:
            fieldnames = sorted({k for r in code_rows for k in r.keys()})
        else:
            fieldnames = ["document_id", "document_display_id", "note"]  # minimal header
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in code_rows:
            writer.writerow(r)
    print(f"[note] Wrote {len(code_rows)} code rows to {codes_csv}")

    # Write no-codes
    with open(nocodes_csv, "w", newline="", encoding="utf-8") as f:
        if nocode_rows:
            fieldnames = sorted({k for r in nocode_rows for k in r.keys()})
        else:
            fieldnames = ["document_id", "document_display_id", "note"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in nocode_rows:
            writer.writerow(r)
    print(f"[note] Wrote {len(nocode_rows)} no-code rows to {nocodes_csv}")


if __name__ == "__main__":
    main()
