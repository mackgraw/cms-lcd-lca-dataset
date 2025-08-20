# scripts/run_once.py
from __future__ import annotations

import csv
import os
import sys
import typing as t

from scripts.coverage_api import (
    ensure_license_acceptance,
    fetch_local_reports,
    get_article_codes,
)

# ---------- Helpers ----------

def _read_env_list(name: str) -> list[str]:
    """
    Read a CSV-style env var into a list. Treat None, empty string, or a string of only
    whitespace as “no filter”. This lets blank GitHub env/vars mean “do all”.
    """
    raw = os.getenv(name, "")
    print(f"[PY-ENV] {name} = {raw}")
    if raw is None:
        return []
    s = raw.strip()
    if not s:
        return []
    # split on comma or whitespace
    parts = [p.strip() for p in s.replace(";", ",").replace("|", ",").split(",")]
    return [p for p in parts if p]

def _read_env_int(name: str, default: int | None = None) -> int | None:
    raw = os.getenv(name, "")
    print(f"[PY-ENV] {name} = {raw}")
    raw = (raw or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default

def _read_env_str(name: str, default: str | None = None) -> str | None:
    raw = os.getenv(name, "")
    print(f"[PY-ENV] {name} = {raw}")
    raw = (raw or "").strip()
    return raw if raw else default

def _safe_int(x: t.Any) -> int | None:
    try:
        i = int(x)
        return i
    except Exception:
        return None

def _write_rows_csv(path: str, headers: list[str], rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ---------- Main ----------

def main() -> None:
    # Read envs with robust defaults
    states = _read_env_list("COVERAGE_STATES")
    status = _read_env_str("COVERAGE_STATUS", default="all")
    contractors = _read_env_list("COVERAGE_CONTRACTORS")
    max_docs = _read_env_int("COVERAGE_MAX_DOCS", default=None)
    timeout = _read_env_int("COVERAGE_TIMEOUT", default=30)

    # Always ensure the license agreement is acknowledged & token cached.
    ensure_license_acceptance(timeout=timeout)

    # Discover documents
    final_lcds, articles = fetch_local_reports(
        states=states or None,
        status=status,
        contractors=contractors or None,
        timeout=timeout,
    )
    print(f"[DEBUG] discovered {len(final_lcds)} LCDs, {len(articles)} Articles (total {len(final_lcds)+len(articles)})")

    # Down-select if max_docs specified
    if max_docs is not None and max_docs > 0:
        # Pull evenly from both pools if possible
        half = max_docs // 2
        articles = articles[:max(half, 1)]
        final_lcds = final_lcds[:max(max_docs - len(articles), 0)]
    print(f"[DEBUG] processing {len(final_lcds)+len(articles)} documents")

    code_rows: list[dict] = []
    nocode_rows: list[dict] = []

    # Process Articles
    for idx, a in enumerate(articles, start=1):
        disp = (a.get("document_display_id") or a.get("document_id") or "").strip()
        print(f"[DEBUG] [Article {idx}/{len(articles)}] {disp}")
        print(f"[DEBUG] [Article] {disp}")

        article_id = _safe_int(a.get("document_id"))
        article_display_id = a.get("document_display_id")
        document_version = _safe_int(a.get("document_version"))

        tables = get_article_codes(
            article_id=article_id,
            article_display_id=article_display_id,
            document_version=document_version,
            timeout=timeout,
        )

        found_any = False
        for table_name, rows in tables.items():
            if rows:
                found_any = True
                for r in rows:
                    out = {
                        "document_type": "Article",
                        "document_id": article_id,
                        "document_display_id": article_display_id,
                        "document_version": document_version,
                        "table": table_name,
                    }
                    # merge the row fields
                    out.update(r if isinstance(r, dict) else {})
                    code_rows.append(out)
        if not found_any:
            nocode_rows.append({
                "document_type": "Article",
                "document_id": article_id,
                "document_display_id": article_display_id,
                "document_version": document_version,
                "reason": "No code rows found across known endpoints."
            })

    # (Optional) Process LCDs (left as placeholder; wire up when ready)
    # for idx, l in enumerate(final_lcds, start=1):
    #     disp = (l.get("document_display_id") or l.get("document_id") or "").strip()
    #     print(f"[DEBUG] [LCD {idx}/{len(final_lcds)}] {disp}")
    #     lcd_id = _safe_int(l.get("document_id"))
    #     lcd_display_id = l.get("document_display_id")
    #     document_version = _safe_int(l.get("document_version"))
    #     tables = get_lcd_codes(
    #         lcd_id=lcd_id,
    #         lcd_display_id=lcd_display_id,
    #         document_version=document_version,
    #         timeout=timeout,
    #     )
    #     found_any = any(bool(rows) for rows in tables.values())
    #     if not found_any:
    #         nocode_rows.append({
    #             "document_type": "LCD",
    #             "document_id": lcd_id,
    #             "document_display_id": lcd_display_id,
    #             "document_version": document_version,
    #             "reason": "No code rows found across known endpoints."
    #         })
    #     else:
    #         for table_name, rows in tables.items():
    #             for r in rows:
    #                 out = {
    #                     "document_type": "LCD",
    #                     "document_id": lcd_id,
    #                     "document_display_id": lcd_display_id,
    #                     "document_version": document_version,
    #                     "table": table_name,
    #                 }
    #                 out.update(r if isinstance(r, dict) else {})
    #                 code_rows.append(out)

    # Write outputs
    os.makedirs("dataset", exist_ok=True)
    code_path = "dataset/document_codes_latest.csv"
    nocode_path = "dataset/document_nocodes_latest.csv"

    # Write code rows
    if code_rows:
        # collect union of keys for header
        headers = sorted(set().union(*[r.keys() for r in code_rows]))
        _write_rows_csv(code_path, headers, code_rows)
        print(f"[note] Wrote {len(code_rows)} code rows to {code_path}")
    else:
        # even if empty, write an empty file with a tiny header for artifacts
        _write_rows_csv(code_path, ["document_type", "document_id", "document_display_id", "document_version", "table"], [])
        print(f"[note] Wrote 0 code rows to {code_path}")

    # Write no-code rows
    if nocode_rows:
        headers = sorted(set().union(*[r.keys() for r in nocode_rows]))
        _write_rows_csv(nocode_path, headers, nocode_rows)
        print(f"[note] Wrote {len(nocode_rows)} no-code rows to {nocode_path}")
    else:
        _write_rows_csv(nocode_path, ["document_type", "document_id", "document_display_id", "document_version", "reason"], [])
        print(f"[note] Wrote 0 no-code rows to {nocode_path}")

if __name__ == "__main__":
    main()
