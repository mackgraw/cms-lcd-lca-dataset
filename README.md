# CMS LCD/LCA Dataset (Daily Auto-Updated)

**What you get**
- `documents_latest.csv` — LCDs & Articles with contractor, state, status, **source_url**
- `document_codes_latest.csv` — per-article code rows (ICD-10, HCPCS/CPT, Revenue, Bill Type, HCPCS Modifiers), with `coverage_flag`
- `changes_YYYY-MM.csv` — Added / Removed / FlagChanged vs prior run

**Download the latest ZIP**
➡️ https://github.com/mackgraw/cms-lcd-lca-dataset/releases/latest/download/cms_lcd_lca_dataset_latest.zip

**How it updates**
- GitHub Actions runs daily at 07:05 ET
- Commits CSVs to `/dataset/` and publishes a Release ZIP

**Notes**
- Data derived from CMS Coverage API. CPT is AMA IP; this redistributes code identifiers only, not proprietary CPT text. Not legal/clinical advice.
