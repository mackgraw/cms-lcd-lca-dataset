from __future__ import annotations
import csv, json, sys
from pathlib import Path
from typing import Dict, Iterable, List

# Local imports
from scripts.diff_changes import compute_code_changes
from scripts.normalize import norm_article_code_row

"""
Usage:
  python -m scripts.compute_changes <current_document_codes.csv> [prev_article_code_table_normalized.csv]

Outputs:
  dataset/article_code_table_normalized.csv
  dataset/article_code_table_changes.csv  (empty if no prev provided or first run)
"""

OUT_DIR = Path("dataset")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _read_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            yield row

def _write_csv_rows(path: Path, header: List[str], rows: Iterable[Dict[str, str]]):
    newfile = not path.exists()
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow(row)

def _normalize_current(document_codes_csv: Path) -> List[Dict[str, str]]:
    # Expect columns: document_type, document_id, endpoint, row_json,...
    out: List[Dict[str, str]] = []
    for row in _read_csv_rows(document_codes_csv):
        if row.get("document_type") != "Article":
            continue  # diff focuses on article code table for now
        if row.get("endpoint") != "/data/article/code-table":
            continue
        aid = str(row.get("document_id") or "").strip()
        if not aid:
            continue
        try:
            payload = json.loads(row.get("row_json") or "{}")
        except Exception:
            payload = {}
        norm = norm_article_code_row(aid, {
            "code": payload.get("code") or payload.get("Code"),
            "description": payload.get("description") or payload.get("Description"),
            "coverage_flag": payload.get("coverage_flag") or payload.get("CoverageFlag") or "",
            "code_system": payload.get("code_system") or payload.get("CodeSystem") or "",
        })
        out.append(norm)
    return out

def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.compute_changes <current_document_codes.csv> [prev_normalized.csv]", file=sys.stderr)
        return 2

    current_csv = Path(sys.argv[1]).resolve()
    prev_norm_csv = Path(sys.argv[2]).resolve() if len(sys.argv) >= 3 else None

    current_norm = _normalize_current(current_csv)
    norm_path = OUT_DIR / "article_code_table_normalized.csv"
    _write_csv_rows(norm_path, ["article_id", "code", "description", "coverage_flag", "code_system"], current_norm)
    print(f"[ok] wrote normalized: {norm_path} ({len(current_norm)} rows)")

    changes_path = OUT_DIR / "article_code_table_changes.csv"
    if prev_norm_csv and prev_norm_csv.exists():
        prev_norm = list(_read_csv_rows(prev_norm_csv))
        changes = compute_code_changes(prev_norm, current_norm)
        _write_csv_rows(
            changes_path,
            ["change_type", "article_id", "code_system", "code", "prev_flag", "curr_flag"],
            changes,
        )
        print(f"[ok] wrote changes: {changes_path} ({len(changes)} rows)")
    else:
        # first run: create empty changes file with header
        _write_csv_rows(
            changes_path,
            ["change_type", "article_id", "code_system", "code", "prev_flag", "curr_flag"],
            [],
        )
        print(f"[ok] no previous normalized file; created empty changes file: {changes_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
