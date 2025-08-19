# scripts/run_once.py
from __future__ import annotations

import csv
import os
import sys
from typing import Dict, Iterable, List, Tuple

# Local imports
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

# ---------- helpers ----------

def _env(name: str, default: str = "") -> str:
    val = os.environ.get(name, "").strip()
    print(f"[PY-ENV] {name} = {val}", flush=True)
    return val or default

def _shard_slice(n: int, shard_index: int, shard_total: int) -> Tuple[int, int]:
    # even split
    size = (n + shard_total - 1) // shard_total
    start = shard_index * size
    end = min(n, start + size)
    return start, end

def _write_rows(csv_path: str, rows: Iterable[Dict[str, str]], header: List[str]) -> int:
    wrote = 0
    need_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        if need_header:
            w.writeheader()
        for r in rows:
            w.writerow(r)
            wrote += 1
    return wrote

def _agg_rows(doc_kind: str, doc_ids: Dict[str, str], timeout: int) -> List[Dict[str, str]]:
    """
    Pull all “code-ish” families for a single document (Article or LCD).
    Returns a list of normalized rows with a `family` column and basic document keys.
    """
    families = [
        ("code-table", get_codes_table_any),
        ("icd10-covered", get_icd10_covered_any),
        ("icd10-noncovered", get_icd10_noncovered_any),
        ("hcpc-code", get_hcpc_codes_any),
        ("hcpc-modifier", get_hcpc_modifiers_any),
        ("revenue-code", get_revenue_codes_any),
        ("bill-codes", get_bill_types_any),
    ]
    out: List[Dict[str, str]] = []
    for fam_name, fetch in families:
        try:
            data = fetch(doc_ids, timeout)
        except Exception as e:
            print(f"[WARN] {doc_kind} fetch {fam_name} error: {e}", flush=True)
            data = []
        if not data:
            continue
        for row in data:
            norm = {"family": fam_name, "document_type": doc_kind}
            # carry through some common fields if present
            for k in ("document_id", "document_display_id", "article_id", "article_display_id",
                      "lcd_id", "lcd_display_id", "document_version"):
                if k in doc_ids and doc_ids[k]:
                    norm[k] = str(doc_ids[k])
            # flatten row keys into strings
            for k, v in (row or {}).items():
                norm[str(k)] = "" if v is None else str(v)
            out.append(norm)
    return out

# ---------- main ----------

def main() -> int:
    states = _env("COVERAGE_STATES")
    status = _env("COVERAGE_STATUS")
    contractors = _env("COVERAGE_CONTRACTORS")
    max_docs_str = _env("COVERAGE_MAX_DOCS")
    timeout_str = _env("COVERAGE_TIMEOUT")

    try:
        max_docs = int(max_docs_str) if max_docs_str else 0
    except ValueError:
        max_docs = 0

    try:
        timeout = int(timeout_str) if timeout_str else 30
    except ValueError:
        timeout = 30

    # License token (if any)
    ensure_license_acceptance(timeout=timeout)

    # Discover documents
    lcds = list_final_lcds(states=states, status=status, contractors=contractors, timeout=timeout)
    arts = list_articles(states=states, status=status, contractors=contractors, timeout=timeout)
    print(f"[DEBUG] discovered {len(lcds)} LCDs, {len(arts)} Articles (total {len(lcds)+len(arts)})", flush=True)

    # Combine (keep Articles first to match your logs)
    docs: List[Tuple[str, Dict[str, str]]] = []
    # Articles
    for r in arts:
        # r keys per your logs:
        # ['contractor_name_type','document_display_id','document_id','document_type',
        #  'document_version','effective_date','note','retirement_date','title','updated_on','updated_on_sort','url']
        ids = {
            "article_id": str(r.get("document_id") or "").strip(),
            "article_display_id": str(r.get("document_display_id") or "").strip(),
            "document_id": str(r.get("document_id") or "").strip(),
            "document_display_id": str(r.get("document_display_id") or "").strip(),
            "document_version": str(r.get("document_version") or "").strip(),
        }
        docs.append(("Article", ids))
    # LCDs
    for r in lcds:
        ids = {
            "lcd_id": str(r.get("document_id") or "").strip(),
            "lcd_display_id": str(r.get("document_display_id") or "").strip(),
            "document_id": str(r.get("document_id") or "").strip(),
            "document_display_id": str(r.get("document_display_id") or "").strip(),
            "document_version": str(r.get("document_version") or "").strip(),
        }
        docs.append(("LCD", ids))

    # Shard
    shard_index = int(os.environ.get("SHARD_INDEX", "0"))
    shard_total = int(os.environ.get("SHARD_TOTAL", "1"))
    start, end = _shard_slice(len(docs), shard_index, shard_total)
    docs = docs[start:end]
    print(f"[DEBUG] shard {shard_index}/{shard_total} -> {len(docs)} of {len(lcds)+len(arts)} items", flush=True)

    # Output CSVs
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataset_dir = os.path.join(base_dir, "dataset")
    os.makedirs(dataset_dir, exist_ok=True)
    codes_csv = os.path.join(dataset_dir, "document_codes_latest.csv")
    nocodes_csv = os.path.join(dataset_dir, "document_nocodes_latest.csv")

    # Process
    processed = 0
    total_rows = 0
    for idx, (kind, ids) in enumerate(docs, 1):
        # A little pretty banner
        disp = ids.get("article_display_id") or ids.get("lcd_display_id") or ids.get("document_display_id") or ""
        did  = ids.get("article_id") or ids.get("lcd_id") or ids.get("document_id") or ""
        ver  = ids.get("document_version") or ""
        print(f"[DEBUG] [{kind} {idx}/{len(docs)}] {disp or did} ids={{{' '.join([f'{k}={v}' for k,v in ids.items() if v])}}}", flush=True)

        rows = _agg_rows(kind, ids, timeout=timeout)
        if rows:
            # Determine header as union of keys (stable order)
            header = sorted({k for r in rows for k in r.keys()})
            wrote = _write_rows(codes_csv, rows, header)
            total_rows += wrote
        else:
            # Record that this document yielded no rows
            _write_rows(nocodes_csv, [{"document_type": kind, **ids}], header=["document_type","document_id","document_display_id","article_id","article_display_id","lcd_id","lcd_display_id","document_version"])
        processed += 1
        if max_docs and processed >= max_docs:
            break

    print(f"[SUMMARY] processed items: {processed}, total rows written: {total_rows}", flush=True)
    print(f"[SUMMARY] CSV (codes): {codes_csv}", flush=True)
    print(f"[SUMMARY] CSV (no-code): {nocodes_csv}  (documents that yielded 0 rows)", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
