# scripts/sanity_probe.py
"""
Quick, noisy probe for a single token (A… article or L… LCD).
Examples:
  .\.venv\Scripts\python.exe -m scripts.sanity_probe A59636
  .\.venv\Scripts\python.exe -m scripts.sanity_probe L36668
"""
from __future__ import annotations

import sys
from typing import Any, Dict, List

from scripts.coverage_api import (
    _debug,
    get_article_any,
    get_final_lcd_any,
    get_article_codes_table_any,
    get_final_lcd_codes_table,
    get_icd10_covered_any,
    get_icd10_noncovered_any,
    get_hcpc_codes_any,
    get_hcpc_modifiers_any,
    get_revenue_codes_any,
    get_bill_types_any,
    _HTTPError,
)

TIMEOUT = 30

def _rows(label: str, rows: List[Dict[str, Any]]) -> None:
    print(f"=== {label} ===")
    print(f"rows: {len(rows)}\n")

def probe_article(token: str) -> None:
    print(f"\n=== Article detail for {token} ===")
    detail = get_article_any({"document_display_id": token}, TIMEOUT)
    print(detail)

    _rows("Article code-table",
          get_article_codes_table_any({"document_display_id": token}, TIMEOUT))
    _rows("Article icd10-covered",
          get_icd10_covered_any({"document_display_id": token}, TIMEOUT))
    _rows("Article icd10-noncovered",
          get_icd10_noncovered_any({"document_display_id": token}, TIMEOUT))
    _rows("Article hcpc-code",
          get_hcpc_codes_any({"document_display_id": token}, TIMEOUT))
    _rows("Article hcpc-modifier",
          get_hcpc_modifiers_any({"document_display_id": token}, TIMEOUT))
    _rows("Article revenue-code",
          get_revenue_codes_any({"document_display_id": token}, TIMEOUT))
    _rows("Article bill-codes",
          get_bill_types_any({"document_display_id": token}, TIMEOUT))

def probe_lcd(token: str) -> None:
    print(f"\n=== LCD detail for {token} ===")
    try:
        detail = get_final_lcd_any({"document_display_id": token}, TIMEOUT)
        print(detail)
    except _HTTPError as e:
        print(f"[note] LCD detail endpoint not available: {e}\n")

    print("=== Harvesting codes via article-then-lcd endpoints (safe) ===")
    # Try both article- and lcd- flavored tables in safe wrappers
    _rows("code-table (article→lcd)",
          get_article_codes_table_any({"document_display_id": token}, TIMEOUT) or
          get_final_lcd_codes_table({"document_display_id": token}, TIMEOUT))
    _rows("icd10-covered (article→lcd)",
          get_icd10_covered_any({"document_display_id": token}, TIMEOUT))
    _rows("icd10-noncovered (article→lcd)",
          get_icd10_noncovered_any({"document_display_id": token}, TIMEOUT))
    _rows("hcpc-code (article→lcd)",
          get_hcpc_codes_any({"document_display_id": token}, TIMEOUT))
    _rows("hcpc-modifier (article→lcd)",
          get_hcpc_modifiers_any({"document_display_id": token}, TIMEOUT))
    _rows("revenue-code (article→lcd)",
          get_revenue_codes_any({"document_display_id": token}, TIMEOUT))
    _rows("bill-codes (article→lcd)",
          get_bill_types_any({"document_display_id": token}, TIMEOUT))

def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.sanity_probe <A… or L… or numeric id>")
        sys.exit(2)
    token = sys.argv[1].strip()
    if token.upper().startswith("A"):
        probe_article(token)
    elif token.upper().startswith("L"):
        probe_lcd(token)
    else:
        # Default to article probe for numeric id; it’s the only one we’ve seen working
        probe_article(token)

if __name__ == "__main__":
    main()
