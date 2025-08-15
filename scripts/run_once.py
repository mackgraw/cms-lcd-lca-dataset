from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
from dateutil import tz
import yaml

from scripts.coverage_api import (
    list_final_lcds, list_articles,
    get_article_icd10_covered, get_article_icd10_noncovered,
    get_article_hcpc_codes, get_article_hcpc_modifiers,
    get_article_revenue_codes, get_article_bill_types,
)

from scripts.normalize import norm_doc_stub, norm_article_code_row
from scripts.diff_changes import compute_code_changes

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "dataset"
ARTIFACTS_DIR = ROOT / "artifacts"
DATA_DIR = ROOT / "data"

def load_config():
    with open(ROOT / "config" / "config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Environment overrides
    if os.getenv("COVERAGE_STATES"):
        cfg["states"] = [s.strip() for s in os.getenv("COVERAGE_STATES").split(",") if s.strip()]
    if os.getenv("COVERAGE_STATUS"):
        cfg["status"] = os.getenv("COVERAGE_STATUS")
    if os.getenv("COVERAGE_CONTRACTORS"):
        cfg["contractors"] = [c.strip() for c in os.getenv("COVERAGE_CONTRACTORS").split(",") if c.strip()]
    if os.getenv("COVERAGE_MAX_DOCS"):
        cfg["max_docs_per_run"] = int(os.getenv("COVERAGE_MAX_DOCS"))
    if os.getenv("COVERAGE_TIMEOUT"):
        cfg["request_timeout_sec"] = int(os.getenv("COVERAGE_TIMEOUT"))
    return cfg

def now_et():
    return datetime.now(timezone.utc).astimezone(tz.gettz("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S %Z")

def main():
    cfg = load_config()
    states = cfg.get("states") or None
    status = cfg.get("status") or "Active"
    contractors = cfg.get("contractors") or None
    limit = int(cfg.get("max_docs_per_run", 250))

    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Discover docs
    lcds = list_final_lcds(states, status, contractors)[:limit]
    arts = list_articles(states, status, contractors)[:limit]

    docs_norm = [norm_doc_stub(x) for x in lcds] + [norm_doc_stub(x) for x in arts]
    docs_df = pd.DataFrame(docs_norm).drop_duplicates(subset=["doc_id"]).reset_index(drop=True)

    # 2) Pull codes from Articles
    rows = []
    for stub in arts:
        aid = stub.get("article_id") or stub.get("id")
        if not aid:
            continue
        for row in get_article_icd10_covered(aid):
            r = norm_article_code_row(aid, row); r["coverage_flag"]="covered"; r["code_system"]="ICD10-CM"; rows.append(r)
        for row in get_article_icd10_noncovered(aid):
            r = norm_article_code_row(aid, row); r["coverage_flag"]="noncovered"; r["code_system"]="ICD10-CM"; rows.append(r)
        for row in get_article_hcpc_codes(aid):
            r = norm_article_code_row(aid, row); r["code_system"]="HCPCS/CPT"; rows.append(r)
        for row in get_article_revenue_codes(aid):
            r = norm_article_code_row(aid, row); r["code_system"]="Revenue"; rows.append(r)
        for row in get_article_bill_types(aid):
            r = norm_article_code_row(aid, row); r["code_system"]="Bill Type"; rows.append(r)
        for row in get_article_hcpc_modifiers(aid):
            r = norm_article_code_row(aid, row); r["code_system"]="HCPCS Modifier"; rows.append(r)

    codes_df = pd.DataFrame(rows).drop_duplicates(subset=["doc_id","code_system","code"]).reset_index(drop=True)

    # 3) Diff vs previous snapshot (if any)
    prev_path = DATA_DIR / "document_codes_latest.csv"
    if prev_path.exists():
        prev_df = pd.read_csv(prev_path)
        changes_df = compute_code_changes(prev_df, codes_df)
    else:
        changes_df = pd.DataFrame(columns=["doc_id","code_system","code","change_type","prev_flag","curr_flag","change_date"])

    # 4) Save
    docs_out = DATASET_DIR / "documents_latest.csv"
    codes_out = DATASET_DIR / "document_codes_latest.csv"
    monthly_out = DATASET_DIR / f"changes_{datetime.utcnow().strftime('%Y-%m')}.csv"

    docs_df.to_csv(docs_out, index=False)
    codes_df.to_csv(codes_out, index=False)
    changes_df.to_csv(monthly_out, index=False)

    # persist snapshot for next diff
    docs_df.to_csv(DATA_DIR / "documents_latest.csv", index=False)
    codes_df.to_csv(DATA_DIR / "document_codes_latest.csv", index=False)

    print({
        "run_time": now_et(),
        "docs": len(docs_df),
        "articles": len(arts),
        "codes": len(codes_df),
        "changes": len(changes_df)
    })

if __name__ == "__main__":
    main()
