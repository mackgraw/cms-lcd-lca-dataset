# scripts/run_once.py
from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from scripts.coverage_api import (
    ensure_license_acceptance,
    fetch_local_reports,
    harvest_article_endpoints,
    harvest_lcd_endpoints,
)

OUT_DIR = Path("dataset")
LOG_DIR = Path(".harvest_logs")
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

CODES_CSV = OUT_DIR / "document_codes_latest.csv"
NOCODES_CSV = OUT_DIR / "document_nocodes_latest.csv"

FLUSH_EVERY = 250  # flush every N documents processed across both types

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    return val.strip()

def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    v = _env(name, None)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def _print_env():
    print(f"[PY-ENV] COVERAGE_STATES = {_env('COVERAGE_STATES', '')}")
    print(f"[PY-ENV] COVERAGE_STATUS = {_env('COVERAGE_STATUS', '')}")
    print(f"[PY-ENV] COVERAGE_CONTRACTORS = {_env('COVERAGE_CONTRACTORS', '')} ")
    print(f"[PY-ENV] COVERAGE_MAX_DOCS = {_env('COVERAGE_MAX_DOCS', '')}")
    print(f"[PY-ENV] COVERAGE_TIMEOUT = {_env('COVERAGE_TIMEOUT', '30')}")

def _open_csv_with_header(path: Path, header: List[str]):
    new_file = not path.exists()
    f = path.open("a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=header)
    if new_file:
        w.writeheader()
    return f, w

def _summarize_counts(by_endpoint: Dict[str, List[dict]]) -> Tuple[Dict[str, int], int]:
    counts = {ep: (len(rows) if isinstance(rows, list) else 0) for ep, rows in by_endpoint.items()}
    total = sum(counts.values())
    return counts, total

def _write_code_rows(writer: csv.DictWriter, doc_type: str, meta: dict, endpoint: str, rows: List[dict], display_id_fallback: str):
    for r in rows:
        writer.writerow(
            {
                "document_type": doc_type,
                "document_id": meta.get("lcd_id") if doc_type == "LCD" else meta.get("article_id"),
                "document_display_id": (
                    (meta.get("lcd_display_id") if doc_type == "LCD" else meta.get("article_display_id"))
                    or display_id_fallback
                ),
                "document_version": meta.get("document_version"),
                "endpoint": endpoint,
                "row_json": json.dumps(r, ensure_ascii=False),
            }
        )

def main() -> None:
    _print_env()

    timeout = float(_env("COVERAGE_TIMEOUT", "30"))
    max_docs = _env_int("COVERAGE_MAX_DOCS", None)

    # license/token (auto-refresh lives inside coverage_api)
    ensure_license_acceptance(timeout=timeout)

    # fetch reports
    lcds, articles = fetch_local_reports(timeout=timeout)
    total_lcds = len(lcds if isinstance(lcds, list) else [])
    total_articles = len(articles if isinstance(articles, list) else [])
    print(f"[DEBUG] discovered {total_lcds} LCDs, {total_articles} Articles (total {total_lcds + total_articles})")

    if max_docs is not None and max_docs > 0:
        lcds = lcds[: max(0, min(max_docs, len(lcds)))]
        articles = articles[: max(0, min(max_docs, len(articles)))]

    print(f"[DEBUG] processing {len(lcds)} LCDs and {len(articles)} Articles")

    codes_f, codes_w = _open_csv_with_header(
        CODES_CSV,
        [
            "document_type",
            "document_id",
            "document_display_id",
            "document_version",
            "endpoint",
            "row_json",
        ],
    )
    nocodes_f, nocodes_w = _open_csv_with_header(
        NOCODES_CSV,
        ["document_type", "document_id", "document_display_id", "document_version", "reason"],
    )

    processed = 0
    wrote_rows = 0

    try:
        # -------- LCDs --------
        for idx, lcd in enumerate(lcds, start=1):
            disp = lcd.get("lcd_display_id") or lcd.get("lcdDisplayId") or lcd.get("document_display_id") or lcd.get("documentDisplayId")
            print(f"[DEBUG] [LCD {idx}/{len(lcds)}] {disp}")
            by_ep, meta = harvest_lcd_endpoints(lcd, timeout=timeout)

            counts, total = _summarize_counts(by_ep)
            nonzero = ", ".join(f"{k.split('/')[-1]}={v}" for k, v in counts.items() if v)
            print(f"[sum] {disp}: total={total}" + (f" ({nonzero})" if nonzero else ""))

            if total > 0:
                for ep, rows in by_ep.items():
                    if not rows:
                        continue
                    _write_code_rows(codes_w, "LCD", meta, ep, rows, disp or "")
                    wrote_rows += len(rows)
            else:
                nocodes_w.writerow(
                    {
                        "document_type": "LCD",
                        "document_id": meta.get("lcd_id"),
                        "document_display_id": meta.get("lcd_display_id") or disp,
                        "document_version": meta.get("document_version"),
                        "reason": "no rows from any data endpoint",
                    }
                )

            processed += 1
            if processed % FLUSH_EVERY == 0:
                codes_f.flush(); nocodes_f.flush()
                os.fsync(codes_f.fileno()); os.fsync(nocodes_f.fileno())
                print(f"[note] flushed CSVs at {processed} documents (rows so far: {wrote_rows})")

        # -------- Articles --------
        for idx, art in enumerate(articles, start=1):
            disp = art.get("article_display_id") or art.get("articleDisplayId") or art.get("document_display_id") or art.get("documentDisplayId")
            print(f"[DEBUG] [Article {idx}/{len(articles)}] {disp}")
            by_ep, meta = harvest_article_endpoints(art, timeout=timeout)

            counts, total = _summarize_counts(by_ep)
            nonzero = ", ".join(f"{k.split('/')[-1]}={v}" for k, v in counts.items() if v)
            print(f"[sum] {disp}: total={total}" + (f" ({nonzero})" if nonzero else ""))

            if total > 0:
                for ep, rows in by_ep.items():
                    if not rows:
                        continue
                    _write_code_rows(codes_w, "Article", meta, ep, rows, disp or "")
                    wrote_rows += len(rows)
            else:
                nocodes_w.writerow(
                    {
                        "document_type": "Article",
                        "document_id": meta.get("article_id"),
                        "document_display_id": meta.get("article_display_id") or disp,
                        "document_version": meta.get("document_version"),
                        "reason": "no rows from any data endpoint",
                    }
                )

            processed += 1
            if processed % FLUSH_EVERY == 0:
                codes_f.flush(); nocodes_f.flush()
                os.fsync(codes_f.fileno()); os.fsync(nocodes_f.fileno())
                print(f"[note] flushed CSVs at {processed} documents (rows so far: {wrote_rows})")

    finally:
        codes_f.flush(); nocodes_f.flush()
        try:
            os.fsync(codes_f.fileno()); os.fsync(nocodes_f.fileno())
        except Exception:
            pass
        codes_f.close(); nocodes_f.close()

    print(f"[note] Wrote {wrote_rows} code rows to {CODES_CSV}")
    print(f"[note] Completed. See {NOCODES_CSV} for documents with zero rows.")
    # (We no longer print a derived "no-code count" since some documents may produce both types across runs.)

if __name__ == "__main__":
    main()
