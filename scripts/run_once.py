# scripts/run_once.py
from __future__ import annotations

import csv
import io
import os
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

# NOTE: these functions are expected in scripts.coverage_api per your repo
from scripts.coverage_api import (
    ensure_license_acceptance,
    list_final_lcds,
    list_articles,
    get_codes_table_any,
    get_icd10_covered_any,
    get_icd10_noncovered_any,
    get_hcpc_codes_any,
    get_hcpc_modifiers_any,
    get_revenue_codes_any,
    get_bill_types_any,
)

OUT_DIR = Path("dataset")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CODES_ZIP = OUT_DIR / "document_codes_latest.zip"
CODES_CSV_NAME_IN_ZIP = "document_codes_latest.csv"
NOCODES_CSV = OUT_DIR / "document_nocodes_latest.csv"

# ---------- util logging ----------

def _env(name: str, default: str = "") -> str:
    val = os.environ.get(name, "")
    print(f"[PY-ENV] {name} = {val}", flush=True)
    return val or default

def _debug(msg: str) -> None:
    print(msg, flush=True)

def _ids_from_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Normalize the id fields we’ve seen in the report rows to parameters
    our coverage_api helpers understand.
    """
    ids: Dict[str, Any] = {}
    # Articles
    if "document_type" in row and str(row.get("document_type", "")).lower() == "article":
        if "document_id" in row and row["document_id"]:
            ids["article_id"] = row["document_id"]
            ids["document_id"] = row["document_id"]
        if "document_display_id" in row and row["document_display_id"]:
            ids["article_display_id"] = row["document_display_id"]
            ids["document_display_id"] = row["document_display_id"]
    # LCDs
    if "document_type" in row and "lcd" in str(row.get("document_type", "")).lower():
        if "document_id" in row and row["document_id"]:
            ids["lcd_id"] = row["document_id"]
            ids["document_id"] = row["document_id"]
        if "document_display_id" in row and row["document_display_id"]:
            ids["lcd_display_id"] = row["document_display_id"]
            ids["document_display_id"] = row["document_display_id"]

    # Also carry version if present
    if "document_version" in row and row["document_version"] not in (None, ""):
        ids["document_version"] = row["document_version"]

    return ids

def _ids_from_env() -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    # Article
    if os.environ.get("EXAMPLE_ARTICLE_ID"):
        ids["article_id"] = os.environ["EXAMPLE_ARTICLE_ID"]
        ids["document_id"] = os.environ["EXAMPLE_ARTICLE_ID"]
    if os.environ.get("EXAMPLE_ARTICLE_DISPLAY_ID"):
        ids["article_display_id"] = os.environ["EXAMPLE_ARTICLE_DISPLAY_ID"]
        ids["document_display_id"] = os.environ["EXAMPLE_ARTICLE_DISPLAY_ID"]
    # LCD
    if os.environ.get("EXAMPLE_LCD_ID"):
        ids["lcd_id"] = os.environ["EXAMPLE_LCD_ID"]
        ids["document_id"] = os.environ["EXAMPLE_LCD_ID"]
    if os.environ.get("EXAMPLE_LCD_DISPLAY_ID"):
        ids["lcd_display_id"] = os.environ["EXAMPLE_LCD_DISPLAY_ID"]
        ids["document_display_id"] = os.environ["EXAMPLE_LCD_DISPLAY_ID"]
    # version (optional)
    if os.environ.get("EXAMPLE_DOCUMENT_VERSION"):
        ids["document_version"] = os.environ["EXAMPLE_DOCUMENT_VERSION"]
    return ids

def _write_zip_with_csv(rows: List[Dict[str, Any]], zip_path: Path, csv_name_in_zip: str) -> None:
    # Collect all keys to make a stable header
    header: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                header.append(k)

    # Write CSV into an in-memory buffer, then zip it
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=header or ["document_type", "document_display_id", "document_id"])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name_in_zip, buf.getvalue())

def _write_nocodes_csv_if_missing_or_empty(path: Path, header: Optional[List[str]] = None) -> None:
    """
    Always ensure there is a nocodes CSV. If the caller later writes
    a non-empty one, that will overwrite this placeholder.
    """
    if header is None:
        header = ["document_type", "document_display_id", "document_id", "reason"]
    # Create/overwrite with just headers
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)

# ---------- main ----------

def main() -> None:
    # Read “filters”, mainly for logs and parity with previous scripts
    states = _env("COVERAGE_STATES")
    status = _env("COVERAGE_STATUS")
    contractors = _env("COVERAGE_CONTRACTORS")
    max_docs = _env("COVERAGE_MAX_DOCS")
    timeout_env = _env("COVERAGE_TIMEOUT")
    try:
        timeout = int(timeout_env) if timeout_env.strip() else 30
    except Exception:
        timeout = 30

    # License acceptance (token is optional, script should proceed either way)
    ensure_license_acceptance(timeout=timeout)

    # Discover lists
    _debug("[DEBUG] trying /reports/local-coverage-final-lcds (no status)" if not status else "[DEBUG] trying /reports/local-coverage-final-lcds")
    lcd_rows = list_final_lcds(states=states or "", status=status or "", contractors=contractors or "", timeout=timeout)

    _debug("[DEBUG] trying /reports/local-coverage-articles (no status)" if not status else "[DEBUG] trying /reports/local-coverage-articles")
    art_rows = list_articles(states=states or "", status=status or "", contractors=contractors or "", timeout=timeout)

    _debug(f"[DEBUG] discovered {len(lcd_rows)} LCDs, {len(art_rows)} Articles (total {len(lcd_rows)+len(art_rows)})")

    # Decide which doc(s) to probe
    ids = _ids_from_env()
    picked: List[Tuple[str, Dict[str, Any]]] = []

    if ids:
        # If env provided, we’ll just use those
        # Prefer Article if article_* present, else LCD if lcd_* present
        if any(k in ids for k in ("article_id", "article_display_id")):
            picked.append(("Article", ids))
        elif any(k in ids for k in ("lcd_id", "lcd_display_id")):
            picked.append(("LCD", ids))
        else:
            # Raw document_* only → treat it as unknown; still probe via both families
            picked.append(("Unknown", ids))
        _debug("INFO EXAMPLE_* ids provided; using those for the sanity probe.")
    else:
        # Auto-pick: first Article and first LCD (if available)
        if art_rows:
            a_ids = _ids_from_row({**art_rows[0], "document_type": "Article"})
            picked.append(("Article", a_ids))
        if lcd_rows:
            l_ids = _ids_from_row({**lcd_rows[0], "document_type": "LCD"})
            picked.append(("LCD", l_ids))

        if not picked:
            print("INFO No EXAMPLE_* ids set and discovery returned no rows; run_once completed with no-ops.", flush=True)
            # Still emit empty nocodes file so artifact step never fails.
            _write_nocodes_csv_if_missing_or_empty(NOCODES_CSV)
            _write_zip_with_csv([], CODES_ZIP, CODES_CSV_NAME_IN_ZIP)
            return

    # Probe the endpoints for each picked doc
    aggregated_rows: List[Dict[str, Any]] = []
    nocodes_rows: List[Dict[str, Any]] = []

    families = [
        ("code-table", get_codes_table_any),
        ("icd10-covered", get_icd10_covered_any),
        ("icd10-noncovered", get_icd10_noncovered_any),
        ("hcpc-code", get_hcpc_codes_any),
        ("hcpc-modifier", get_hcpc_modifiers_any),
        ("revenue-code", get_revenue_codes_any),
        ("bill-codes", get_bill_types_any),
    ]

    for (doc_type, doc_ids) in picked:
        disp = doc_ids.get("article_display_id") or doc_ids.get("lcd_display_id") or doc_ids.get("document_display_id") or ""
        num = doc_ids.get("article_id") or doc_ids.get("lcd_id") or doc_ids.get("document_id") or ""
        ver = doc_ids.get("document_version") or ""
        pretty_ids = " ".join(
            f"{k}={v}" for k, v in doc_ids.items() if v not in (None, "")
        )
        _debug(f"[DEBUG] [{doc_type}] ids={{{pretty_ids}}}")

        for fname, fn in families:
            rows = []
            try:
                rows = list(fn(doc_ids, timeout))
            except Exception as e:
                _debug(f"[DEBUG]   -> /data/{doc_type.lower()}/{fname} exception: {e!r}")

            # Basic debug around each call
            if rows:
                _debug(f"[DEBUG]   -> /data/{'article' if 'article' in doc_ids else 'lcd'}/{fname}: {len(rows)} rows")
            else:
                _debug(f"[DEBUG]   -> /data/{'article' if 'article' in doc_ids else 'lcd'}/{fname}: 0 rows")

            # Tag and collect
            for r in rows:
                rr = dict(r)
                rr["_document_type"] = doc_type
                rr["_document_display_id"] = disp
                rr["_document_id"] = num
                rr["_document_version"] = ver
                rr["_section"] = fname
                aggregated_rows.append(rr)

        # If we saw no rows for this doc at all, note that in nocodes output
        if not any(r for r in aggregated_rows if r.get("_document_id") == num or r.get("_document_display_id") == disp):
            nocodes_rows.append({
                "document_type": doc_type,
                "document_display_id": disp,
                "document_id": num,
                "reason": "No rows returned by any probed sections"
            })

    # Always emit nocodes CSV (headers only is fine)
    if nocodes_rows:
        with NOCODES_CSV.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["document_type", "document_display_id", "document_id", "reason"])
            w.writeheader()
            for r in nocodes_rows:
                w.writerow(r)
    else:
        _write_nocodes_csv_if_missing_or_empty(NOCODES_CSV)

    # Emit codes zip (with CSV inside)
    _write_zip_with_csv(aggregated_rows, CODES_ZIP, CODES_CSV_NAME_IN_ZIP)

    # Friendly summary
    _debug(f"        -> aggregated rows: {len(aggregated_rows)}")
    if nocodes_rows:
        _debug(f"        -> nocodes rows: {len(nocodes_rows)}")
    print("Run completed.", flush=True)


if __name__ == "__main__":
    main()
