from __future__ import annotations
import csv, json, os, sys
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


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return default if v is None or v.strip() == "" else v.strip()


def _env_int(name: str, default: Optional[int]) -> Optional[int]:
    v = _env(name, None)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def _print_env():
    for n in [
        "COVERAGE_STATES",
        "COVERAGE_STATUS",
        "COVERAGE_CONTRACTORS",
        "COVERAGE_MAX_DOCS",
        "COVERAGE_TIMEOUT",
        "SHARD_INDEX",
        "SHARD_TOTAL",
    ]:
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
        writer.writerow(
            {
                "document_type": doc_type,
                "document_id": meta.get("lcd_id") if doc_type == "LCD" else meta.get("article_id"),
                "document_display_id": (
                    meta.get("lcd_display_id") if doc_type == "LCD" else meta.get("article_display_id") or disp
                ),
                "document_version": meta.get("document_version"),
                "endpoint": ep,
                "row_json": json.dumps(r, ensure_ascii=False),
            }
        )


def _slice_for_shard(items: List[dict], shard_index: int, shard_total: int) -> List[dict]:
    """
    Stable, deterministic split by original order (no hashing).
    Shard k processes items where (position % shard_total) == shard_index.

    Adds a _pos key to each selected item for debug visibility.
    """
    selected = []
    for pos, row in enumerate(items):
        if (pos % shard_total) == shard_index:
            r = dict(row)
            r["_pos"] = pos  # keep original global index for debugging
            selected.append(r)
    return selected


def main():
    _print_env()
    timeout = float(_env("COVERAGE_TIMEOUT", "30"))
    max_docs = _env_int("COVERAGE_MAX_DOCS", None)

    shard_index = _env_int("SHARD_INDEX", 0) or 0
    shard_total = _env_int("SHARD_TOTAL", 1) or 1
    if shard_total < 1:
        shard_total = 1
    if shard_index < 0 or shard_index >= shard_total:
        print(f"[error] Invalid shard config SHARD_INDEX={shard_index}, SHARD_TOTAL={shard_total}")
        sys.exit(2)

    # File names include shard info to avoid collisions across parallel jobs
    codes_csv = OUT_DIR / f"document_codes_shard_{shard_index}_of_{shard_total}.csv"
    nocodes_csv = OUT_DIR / f"document_nocodes_shard_{shard_index}_of_{shard_total}.csv"

    # Start
    ensure_license_acceptance(timeout=timeout)
    lcds, arts = fetch_local_reports(timeout=timeout)

    # Optional global limit (applied BEFORE sharding for fairness)
    if max_docs:
        lcds = lcds[:max_docs]
        arts = arts[:max_docs]

    # Deterministic slice for this shard
    lcds_shard = _slice_for_shard(lcds, shard_index, shard_total)
    arts_shard = _slice_for_shard(arts, shard_index, shard_total)

    print(
        f"[DEBUG] sharding => total_lcds={len(lcds)} total_articles={len(arts)} | "
        f"shard {shard_index+1}/{shard_total}: lcds={len(lcds_shard)} articles={len(arts_shard)}"
    )

    codes_f, codes_w = _open_csv(
        codes_csv,
        ["document_type", "document_id", "document_display_id", "document_version", "endpoint", "row_json"],
    )
    nocodes_f, nocodes_w = _open_csv(
        nocodes_csv,
        ["document_type", "document_id", "document_display_id", "document_version", "reason"],
    )

    wrote = 0

    try:
        # LCDs
        for idx, lcd in enumerate(lcds_shard, 1):
            disp = lcd.get("lcd_display_id") or lcd.get("document_display_id")
            src_pos = lcd.get("_pos")
            print(f"[DEBUG] [LCD {idx}/{len(lcds_shard)} | src_pos={src_pos}] {disp}")
            by_ep, meta = harvest_lcd_endpoints(lcd, timeout=timeout)
            _, total = _summarize(by_ep)
            print(f"[sum] {disp}: total={total}")
            if total:
                for ep, rows in by_ep.items():
                    if not rows:
                        continue
                    _write_rows(codes_w, "LCD", meta, ep, rows, disp)
                    wrote += len(rows)
            else:
                nocodes_w.writerow(
                    {
                        "document_type": "LCD",
                        "document_id": meta.get("lcd_id"),
                        "document_display_id": disp,
                        "document_version": meta.get("document_version"),
                        "reason": "no rows",
                    }
                )

        # Articles
        for idx, art in enumerate(arts_shard, 1):
            disp = art.get("article_display_id") or art.get("document_display_id")
            src_pos = art.get("_pos")
            print(f"[DEBUG] [Article {idx}/{len(arts_shard)} | src_pos={src_pos}] {disp}")
            by_ep, meta = harvest_article_endpoints(art, timeout=timeout)
            _, total = _summarize(by_ep)
            print(f"[sum] {disp}: total={total}")
            if total:
                for ep, rows in by_ep.items():
                    if not rows:
                        continue
                    _write_rows(codes_w, "Article", meta, ep, rows, disp)
                    wrote += len(rows)
            else:
                nocodes_w.writerow(
                    {
                        "document_type": "Article",
                        "document_id": meta.get("article_id"),
                        "document_display_id": disp,
                        "document_version": meta.get("document_version"),
                        "reason": "no rows",
                    }
                )
    finally:
        codes_f.flush()
        nocodes_f.flush()
        codes_f.close()
        nocodes_f.close()

    print(f"[note] Shard {shard_index+1}/{shard_total} wrote {wrote} code rows to {codes_csv}")
    print(f"[note] Shard {shard_index+1}/{shard_total} completed. See {nocodes_csv} for zero‑row documents.")


if __name__ == "__main__":
    main()
