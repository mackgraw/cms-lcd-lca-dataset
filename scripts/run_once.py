# scripts/run_once.py
from __future__ import annotations

import argparse
import csv
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from scripts.coverage_api import (
    _debug,
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

# Output locations
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
OUT_DIR  = os.path.join(ROOT_DIR, "dataset")
OUT_CSV_CODES   = os.path.join(OUT_DIR, "document_codes_latest.csv")
OUT_CSV_NOCODE  = os.path.join(OUT_DIR, "document_nocodes_latest.csv")  # NEW

def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name, "")
    print(f"[PY-ENV] {name:<16}= {v}", flush=True)
    return v or default

def _mk_ids(kind: str, row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Normalize identifiers for the current item.
    Articles: id -> article_id, display_id -> article_display_id
    LCDs:     id -> document_id, display_id -> document_display_id
    """
    if kind == "Article":
        return {
            "article_id": row.get("article_id") or row.get("id"),
            "article_display_id": row.get("article_display_id") or row.get("display_id"),
            # keep lcd keys empty for clarity
            "document_id": None,
            "document_display_id": None,
        }
    else:  # "LCD"
        return {
            "document_id": row.get("document_id") or row.get("id"),
            "document_display_id": row.get("document_display_id") or row.get("display_id"),
            "article_id": None,
            "article_display_id": None,
        }

def _mk_label(row: Mapping[str, Any]) -> str:
    title = row.get("title") or row.get("name") or row.get("display_name") or ""
    disp  = row.get("display_id") or row.get("document_display_id") or ""
    rid   = row.get("article_id") or row.get("id") or row.get("document_id") or ""
    return f"{title or '?'}  id={rid}  display={disp}"

def _collect_for_ids(kind: str, ids: Mapping[str, Any], timeout: int) -> List[Dict[str, Any]]:
    """
    Collects rows only from endpoints that match the document *kind*.
    kind: "Article" or "LCD"
    """
    rows: List[Dict[str, Any]] = []

    def tag_and_extend(endpoint: str, getter):
        try:
            got = getter(ids, timeout)
        except Exception as e:
            _debug(f"[DEBUG]   -> {endpoint} error: {e} (continue)")
            got = []
        for r in got:
            r = dict(r)
            r["_endpoint"] = endpoint
            rows.append(r)

    # Using helpers that choose Article-first, then LCD, but we call them once per family
    tag_and_extend("code-table",       get_codes_table_any)
    tag_and_extend("icd10-covered",    get_icd10_covered_any)
    tag_and_extend("icd10-noncovered", get_icd10_noncovered_any)
    tag_and_extend("hcpc-code",        get_hcpc_codes_any)
    tag_and_extend("hcpc-modifier",    get_hcpc_modifiers_any)
    tag_and_extend("revenue-code",     get_revenue_codes_any)
    tag_and_extend("bill-codes",       get_bill_types_any)

    return rows

def _write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    header: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                header.append(k)
    if not header:
        header = ["_endpoint"]  # minimal header to keep a valid CSV

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

def _write_nocode_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    """
    rows is a list of simple dicts with the identifying info for each doc that yielded 0 rows.
    """
    os.makedirs(OUT_DIR, exist_ok=True)
    header = ["_kind", "_document_id", "_document_display_id", "_title"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def _parse_args():
    ap = argparse.ArgumentParser(description="Coverage harvest runner")
    ap.add_argument("--file", help="Process a single document identifier (document_id, display_id, or article_id)")
    ap.add_argument("--manifest", help="Path to a newline-delimited list of document identifiers to process")
    # keep compatibility with other unknown args your code may pass around
    return ap.parse_known_args()

def _load_manifest(path: str) -> List[str]:
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Manifest not found: {p}")
    return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]

def _id_keys(kind: str, row: Mapping[str, Any]) -> List[str]:
    """All comparable identifiers for a row, coerced to strings."""
    ids = _mk_ids(kind, row)
    # compare against both article_* and document_* for convenience
    vals = [
        ids.get("document_id"),
        ids.get("document_display_id"),
        ids.get("article_id"),
        ids.get("article_display_id"),
    ]
    return [str(v) for v in vals if v not in (None, "")]

def _select_work(work: List[Tuple[str, Dict[str, Any]]], allow_ids: Optional[set[str]]) -> List[Tuple[str, Dict[str, Any]]]:
    if not allow_ids:
        return work
    selected = []
    for item in work:
        kind, row = item
        keys = _id_keys(kind, row)
        if any(k in allow_ids for k in keys):
            selected.append(item)
    return selected

def _shard_filter(work: List[Tuple[str, Dict[str, Any]]], shard_index: int, shard_total: int) -> List[Tuple[str, Dict[str, Any]]]:
    """Deterministically select a slice of work by hashing a stable identifier."""
    import hashlib
    if shard_total <= 1:
        return work
    out: List[Tuple[str, Dict[str, Any]]] = []
    for item in work:
        kind, row = item
        keys = _id_keys(kind, row)
        # prefer display id -> document id -> article id
        key = (keys[0] if keys else "")
        h = hashlib.md5(key.encode("utf-8")).hexdigest()
        if int(h, 16) % shard_total == shard_index:
            out.append(item)
    return out

def main() -> None:
    # Args (manifest / file) and ENV (for sharding)
    args, unknown = _parse_args()

    SHARD_INDEX = int(os.environ.get("SHARD_INDEX", "0") or "0")
    SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", "1") or "1")

    # Read env (existing behavior)
    STATES      = _env("COVERAGE_STATES")
    STATUS      = _env("COVERAGE_STATUS")
    CONTRACTORS = _env("COVERAGE_CONTRACTORS")
    MAX_DOCS    = int(_env("COVERAGE_MAX_DOCS") or "0")
    TIMEOUT     = int(_env("COVERAGE_TIMEOUT")  or "30")

    # Make sure licensed endpoints are okay to call
    try:
        ensure_license_acceptance(timeout=TIMEOUT)
    except Exception as e:
        print(f"[WARN] ensure_license_acceptance() failed: {e}. Continuing anyway.", flush=True)

    # Discover
    lcds = list_final_lcds(STATES, STATUS, CONTRACTORS, timeout=TIMEOUT)
    arts = list_articles(STATES, STATUS, CONTRACTORS, timeout=TIMEOUT)
    _debug(f"[DEBUG] discovered {len(lcds)} LCDs, {len(arts)} Articles (total {len(lcds)+len(arts)})")

    # Prefer Articles first (LCD data endpoints often 400 or empty)
    work: List[Tuple[str, Dict[str, Any]]] = [("Article", a) for a in arts] + [("LCD", l) for l in lcds]

    # Apply allowlist from CLI (manifest / file)
    allow_ids: Optional[set[str]] = None
    if args.file and args.manifest:
        raise SystemExit("--file and --manifest are mutually exclusive")
    if args.file:
        allow_ids = {args.file.strip()}
    elif args.manifest:
        allow_ids = set(_load_manifest(args.manifest))
    if allow_ids:
        work = _select_work(work, allow_ids)
        _debug(f"[DEBUG] after allowlist: {len(work)} items")

    # If no allowlist, apply shard envs
    if not allow_ids:
        before = len(work)
        work = _shard_filter(work, SHARD_INDEX, SHARD_TOTAL)
        _debug(f"[DEBUG] shard {SHARD_INDEX}/{SHARD_TOTAL} -> {len(work)} of {before} items")

    # Truncate if requested
    if MAX_DOCS and len(work) > MAX_DOCS:
        work = work[:MAX_DOCS]

    all_rows: List[Dict[str, Any]] = []
    no_code_rows: List[Dict[str, Any]] = []

    processed = 0
    for idx, (kind, row) in enumerate(work, start=1):
        label = _mk_label(row)
        _debug(f"[DEBUG] [{kind} {idx}/{len(work)}] {label}")
        ids = _mk_ids(kind, row)

        try:
            got = _collect_for_ids(kind, ids, TIMEOUT)
        except Exception as e:
            _debug(f"[DEBUG]   -> collect errored: {e} (continue)")
            got = []

        if got:
            for r in got:
                # Populate _document_* columns with whichever ID set we have for easy downstream use
                r["_document_display_id"] = ids.get("document_display_id") or ids.get("article_display_id")
                r["_document_id"]        = ids.get("document_id") or ids.get("article_id")
                r["_kind"]               = kind
                r["_title"]              = row.get("title") or row.get("name") or ""
            _debug(f"        -> aggregated rows: {len(got)}")
            all_rows.extend(got)
        else:
            _debug(f"        -> aggregated rows: 0")
            no_code_rows.append({
                "_kind": kind,
                "_document_id": ids.get("document_id") or ids.get("article_id") or "",
                "_document_display_id": (ids.get("document_display_id") or ids.get("article_display_id") or ""),
                "_title": row.get("title") or row.get("name") or "",
            })

        processed += 1

    # Write outputs
    _write_csv(OUT_CSV_CODES, all_rows)
    _write_nocode_csv(OUT_CSV_NOCODE, no_code_rows)

    print(f"[SUMMARY] processed items: {processed}, total rows written: {len(all_rows)}")
    print(f"[SUMMARY] CSV (codes): {OUT_CSV_CODES}")
    print(f"[SUMMARY] CSV (no-code): {OUT_CSV_NOCODE}  (documents that yielded 0 rows)")

if __name__ == "__main__":
    main()
