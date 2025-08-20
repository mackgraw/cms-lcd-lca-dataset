# scripts/run_once.py
from __future__ import annotations
import csv, json, os
from pathlib import Path
from typing import Dict, List, Tuple

from scripts.coverage_api import (
    ensure_license_acceptance,
    fetch_local_reports,
    harvest_article_endpoints,
    harvest_lcd_endpoints,
)

OUT_DIR = Path("dataset")
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = Path(".harvest_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else v.strip()

def _env_int(name: str, default: int | None) -> int | None:
    v = _env(name, None)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def _print_env():
    for n in ["COVERAGE_STATES","COVERAGE_STATUS","COVERAGE_CONTRACTORS","COVERAGE_MAX_DOCS","COVERAGE_TIMEOUT"]:
        print(f"[PY-ENV] {n} = {_env(n, '')}")

def _open_csv(path: Path, header: List[str]):
    new = not path.exists()
    f = path.open("a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=header)
    if new:
        w.writeheader()
    return f, w

def _summarize(by_ep: Dict[str, List[dict]]) -> Tuple[Dict[str, int], int]:
    counts = {ep: len(rows) for ep, rows in by_ep.items()}
    total = sum(counts.values())
    return counts, total

def _write_rows(writer, doc_type: str, meta: dict, ep: str, rows: List[dict], disp: str):
    for r in rows:
        writer.writerow({
            "document_type": doc_type,
            "document_id": meta.get("lcd_id") if doc_type == "LCD" else meta.get("article_id"),
            "document_display_id": (meta.get("lcd_display_id") if doc_type == "LCD" else meta.get("article_display_id")) or disp,
            "document_version": meta.get("document_version"),
            "endpoint": ep,
            "row_json": json.dumps(r, ensure_ascii=False),
        })

def main():
    _print_env()
    timeout = float(_env("COVERAGE_TIMEOUT", "30"))
    max_docs = _env_int("COVERAGE_MAX_DOCS", None)

    # Accept license + pull lists
    ensure_license_acceptance(timeout=timeout)
    lcds, arts = fetch_local_reports(timeout=timeout)

    # Optional cap (blank env = no cap)
    if max_docs:
        lcds = lcds[:max_docs]
        arts = arts[:max_docs]

    print(f"[DEBUG] discovered {len(lcds)} LCDs, {len(arts)} Articles (total {len(lcds)+len(arts)})")
    print(f"[DEBUG] processing {len(lcds)} LCDs and {len(arts)} Articles")

    codes_csv = OUT_DIR / "document_codes_latest.csv"
    nocodes_csv = OUT_DIR / "document_nocodes_latest.csv"

    codes_f, codes_w = _open_csv(
        codes_csv, ["document_type","document_id","document_display_id","document_version","endpoint","row_json"]
    )
    nocodes_f, nocodes_w = _open_csv(
        nocodes_csv, ["document_type","document_id","document_display_id","document_version","reason"]
    )

    wrote = 0
    try:
        # LCDs
        for i, lcd in enumerate(lcds, 1):
            disp = lcd.get("lcd_display_id") or lcd.get("document_display_id")
            print(f"[DEBUG] [LCD {i}/{len(lcds)}] {disp}")
            by_ep, meta = harvest_lcd_endpoints(lcd, timeout=timeout)
            _, total = _summarize(by_ep)
            print(f"[sum] {disp}: total={total}")
            if total:
                for ep, rows in by_ep.items():
                    if rows:
                        _write_rows(codes_w, "LCD", meta, ep, rows, disp)
                        wrote += len(rows)
            else:
                nocodes_w.writerow({
                    "document_type": "LCD",
                    "document_id": meta.get("lcd_id"),
                    "document_display_id": disp,
                    "document_version": meta.get("document_version"),
                    "reason": "no rows",
                })

        # Articles
        for i, art in enumerate(arts, 1):
            disp = art.get("article_display_id") or art.get("document_display_id")
            print(f"[DEBUG] [Article {i}/{len(arts)}] {disp}")
            by_ep, meta = harvest_article_endpoints(art, timeout=timeout)
            _, total = _summarize(by_ep)
            print(f"[sum] {disp}: total={total}")
            if total:
                for ep, rows in by_ep.items():
                    if rows:
                        _write_rows(codes_w, "Article", meta, ep, rows, disp)
                        wrote += len(rows)
            else:
                nocodes_w.writerow({
                    "document_type": "Article",
                    "document_id": meta.get("article_id"),
                    "document_display_id": disp,
                    "document_version": meta.get("document_version"),
                    "reason": "no rows",
                })
    finally:
        codes_f.close()
        nocodes_f.close()

    print(f"[note] Wrote {wrote} code rows to {codes_csv}")
    print(f"[note] Wrote no‑row docs to {nocodes_csv}")

if __name__ == "__main__":
    main()
