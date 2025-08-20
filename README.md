# CMS LCD/LCA Dataset (Daily 6 AM ET, Sharded)

**Release assets (latest run)**  
- `document_codes.csv` — merged per-document code rows (ICD-10, HCPCS/CPT, Revenue, Bill Type, HCPCS Modifier) with `coverage_flag`.  
- `document_nocodes.csv` — documents that returned no code rows during this run.  
- `codes_normalized.csv` — normalized codes across **Articles and LCDs** with a stable schema.  
- `codes_changes.csv` — day-over-day diff vs the previous release (`Added`, `Removed`, `FlagChanged`).  
- `dataset-YYYYMMDD-HHMMSS.zip` — zipped CSV bundle.

**Download the latest release**  
➡️ https://github.com/mackgraw/cms-lcd-lca-dataset/releases/latest

**How it updates**  
- GitHub Actions workflow shards the workload across N workers.  
- Runs **daily at 6:00 AM America/New_York** (cron-guarded).  
- Merges shard outputs, computes normalized + changes CSVs, zips, and creates a **new Release** every day (marked *latest*).

**Notes**  
- Data derived from CMS Coverage API. CPT is AMA IP; this redistributes identifiers only, not proprietary CPT text.  
- Not legal or clinical advice.

## File Schemas

### document_codes.csv
- `document_type` — `LCD` | `Article`  
- `document_id` — CMS identifier (e.g., `L392XX` or `52840`)  
- `
