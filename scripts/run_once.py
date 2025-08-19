# scripts/run_once.py
from __future__ import annotations

import os
import sys
import logging
from scripts.coverage_api import (
    get_article,
    get_article_hcpc_code,
    get_codes_table_any,
    get_icd10_any,
    get_lcd,
    get_lcd_hcpc_code,
    get_lcd_hcpc_code_group,
    get_lcd_contractor,
    get_lcd_revision_history,
    get_lcd_primary_jurisdiction,
    ApiError,
)

logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG") else logging.INFO,
    format="%(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def process_article(article_id: str, ver: str | None = None) -> None:
    log.debug(f"[Article] {article_id} ver={ver or 'latest'}")
    core = get_article(article_id, ver)
    log.debug(f"  article core: {len(core.get('data', []))} rows")

    hcpcs = get_article_hcpc_code(article_id, ver)
    log.debug(f"  article hcpc-code: {len(hcpcs.get('data', []))} rows")

    codes_tbl = get_codes_table_any("article", article_id, ver)
    log.debug(f"  article code-table: {len(codes_tbl.get('data', []))} rows")

    icd10 = get_icd10_any("article", article_id, ver)
    log.debug(
        "  article icd10: covered=%d noncovered=%d pcs=%d",
        len(icd10["data"]["covered"]),
        len(icd10["data"]["noncovered"]),
        len(icd10["data"]["pcs"]),
    )


def process_lcd(lcdid: str, ver: str | None = None) -> None:
    log.debug(f"[LCD] {lcdid} ver={ver or 'latest'}")
    core = get_lcd(lcdid, ver)
    log.debug(f"  lcd core: {len(core.get('data', []))} rows")

    # VALID LCD coding endpoints: hcpc-code & hcpc-code-group
    hcpcs = get_lcd_hcpc_code(lcdid, ver)
    log.debug(f"  lcd hcpc-code: {len(hcpcs.get('data', []))} rows")

    hcpcs_group = get_lcd_hcpc_code_group(lcdid, ver)
    log.debug(f"  lcd hcpc-code-group: {len(hcpcs_group.get('data', []))} rows")

    contractor = get_lcd_contractor(lcdid, ver)
    log.debug(f"  lcd contractor: {len(contractor.get('data', []))} rows")

    revhist = get_lcd_revision_history(lcdid, ver)
    log.debug(f"  lcd revision-history: {len(revhist.get('data', []))} rows")

    primjur = get_lcd_primary_jurisdiction(lcdid, ver)
    log.debug(f"  lcd primary-jurisdiction: {len(primjur.get('data', []))} rows")

    # IMPORTANT: do NOT call non-existent LCD endpoints like:
    #   /data/lcd/code-table
    #   /data/lcd/icd10-covered
    # Those calls previously caused 400s in the logs.


def main(argv: list[str]) -> int:
    # Minimal smoke run that exercises both branches with a single record each.
    # The surrounding workflow should shard & iterate like your existing code.
    try:
        # Example IDs are placeholders; your runner provides real ones.
        example_article_id = os.environ.get("EXAMPLE_ARTICLE_ID")
        example_lcd_id = os.environ.get("EXAMPLE_LCD_ID")

        if example_article_id:
            process_article(example_article_id)

        if example_lcd_id:
            process_lcd(example_lcd_id)

        if not example_article_id and not example_lcd_id:
            log.info("No EXAMPLE_* ids provided; run_once completed with no-ops.")
        return 0
    except ApiError as e:
        log.error(str(e))
        return 2
    except Exception:
        log.exception("Unexpected failure")
        return 3


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
