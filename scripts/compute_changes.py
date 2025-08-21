#!/usr/bin/env python3
"""
Compute normalized codes and day-over-day changes.

Usage:
  python -m scripts.compute_changes <current_document_codes.csv> [<prev_codes_normalized.csv>]

Outputs (in ./dataset):
  - codes_normalized.csv
  - codes_changes.csv
"""
from __future__ import annotations
import csv
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

OUT_DIR = Path("dataset")
OUT_DIR.mkdir(parents=True, exist_ok=True)

NORM_HEADER = ["doc_type", "doc_id", "code_system", "code", "description", "coverage_flag"]
CHG_HEADER  = ["change_type", "doc_type", "doc_id", "code_system", "code", "prev_flag", "curr_flag"]

def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def write_csv_rows(path: Path, header: List[str], rows: Iterable[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow(row)

def normalize_current(document_codes_csv: Path) -> List[Dict[str, str]]:
    """Normalize merged document_codes.csv -> codes_normalized.csv (Article + LCD)."""
    src = read_csv_rows(document_codes_csv)
    out: List[Dict[str, str]] = []

    for row in src:
        # Accept common column variants
        doc_type = (row.get("document_type") or row.get("doc_type") or "").strip()
        doc_id = (row.get("document_id") or row.get("doc_id") or row.get("article_id") or "").strip()
        code_system = (row.get("code_system") or "").strip()
        code = (row.get("code") or "").strip()
        description = (row.get("description") or "").strip()
        coverage_flag = (row.get("coverage_flag") or "").strip()

        if not doc_id or not code:
            continue

        out.append({
            "doc_type": doc_type,
            "doc_id": doc_id,
            "code_system": code_system,
            "code": code,
            "description": description,
            "coverage_flag": coverage_flag,
        })

    return out

def compute_changes(prev_rows: List[Dict[str, str]], curr_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Return Added/Removed/FlagChanged based on coverage_flag keyed by (doc_type, doc_id, code_system, code)."""
    def key(r: Dict[str, str]) -> Tuple[str, str, str, str]:
        return (
            (r.get("doc_type") or "").strip(),
            (r.get("doc_id") or "").strip(),
            (r.get("code_system") or "").strip(),
            (r.get("code") or "").strip(),
        )

    prev_map: Dict[Tuple[str, str, str, str], str] = {}
    curr_map: Dict[Tuple[str, str, str, str], str] = {}

    for r in prev_rows:
        prev_map[key(r)] = (r.get("coverage_flag") or "").strip()
    for r in curr_rows:
        curr_map[key(r)] = (r.get("coverage_flag") or "").strip()

    changes: List[Dict[str, str]] = []

    all_keys = set(prev_map.keys()) | set(curr_map.keys())
    for k in sorted(all_keys):
        pf = prev_map.get(k)
        cf = curr_map.get(k)
        if pf is None and cf is not None:
            ct = "Added"
        elif pf is not None and cf is None:
            ct = "Removed"
        elif pf != cf:
            ct = "FlagChanged"
        else:
            continue

        dt, di, cs, c = k
        changes.append({
            "change_type": ct,
            "doc_type": dt,
            "doc_id": di,
            "code_system": cs,
            "code": c,
            "prev_flag": "" if pf is None else pf,
            "curr_flag": "" if cf is None else cf,
        })

    return changes

def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python -m scripts.compute_changes <current_document_codes.csv> [prev_codes_normalized.csv]", file=sys.stderr)
        return 2

    current_csv = Path(argv[1]).resolve()
    prev_norm_csv = Path(argv[2]).resolve() if len(argv) >= 3 else None

    # Normalize
    current_norm = normalize_current(current_csv)
    norm_path = OUT_DIR / "codes_normalized.csv"
    write_csv_rows(norm_path, NORM_HEADER, current_norm)
    print(f"[ok] wrote {norm_path} ({len(current_norm)} rows)")

    # Changes
    changes_path = OUT_DIR / "codes_changes.csv"
    if prev_norm_csv and prev_norm_csv.exists() and prev_norm_csv.stat().st_size > 0:
        prev_rows = read_csv_rows(prev_norm_csv)
        changes = compute_changes(prev_rows, current_norm)
        write_csv_rows(changes_path, CHG_HEADER, changes)
        print(f"[ok] wrote {changes_path} ({len(changes)} rows)")
    else:
        write_csv_rows(changes_path, CHG_HEADER, [])
        print(f"[ok] no previous baseline; created empty {changes_path}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
