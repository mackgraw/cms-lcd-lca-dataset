# CMS LCD/LCA Dataset (Daily, Sharded, Auto‑Released)

**What you get (Release assets)**
- `document_codes.csv` — merged from all shards; per‑document code rows (ICD‑10, HCPCS/CPT, Revenue, Bill Type, HCPCS Modifier) with `coverage_flag`.
- `document_nocodes.csv` — documents that returned no code rows during this run.
- `codes_normalized.csv` — normalized codes across **Articles and LCDs** with a stable schema.
- `codes_changes.csv` — day‑over‑day diff vs the *previous release* (`Added`, `Removed`, `FlagChanged`).
- `dataset-YYYYMMDD-HHMMSS.zip` — zipped `document_codes.csv` + `document_nocodes.csv`.

**Download the latest**
➡️ https://github.com/mackgraw/cms-lcd-lca-dataset/releases/latest

**How it updates**
- GitHub Actions sharded workflow runs **daily at 06:00 UTC** (cron `0 6 * * *`).
- Each run builds shards in parallel, merges CSVs, computes normalized + changes files, and **creates a new Release** (marked latest).

**Notes**
- Data derived from CMS Coverage API. CPT is AMA IP; this redistributes code identifiers only, not proprietary CPT text. Not legal/clinical advice.

## File Schemas

### `document_codes.csv`
- `document_type` — `LCD` | `Article`
- `document_id` — CMS identifier (e.g., `L392XX` or `52840`)
- `code_system` — `ICD10-CM` | `HCPCS/CPT` | `Revenue` | `Bill Type` | `HCPCS Modifier`
- `code` — code value
- `description` — human‑readable description if provided by API
- `coverage_flag` — `covered` | `noncovered` | `` (n/a)

### `document_nocodes.csv`
- `document_type`, `document_id`, plus metadata about the fetch that yielded no code rows this run.

### `codes_normalized.csv`
- `doc_type` — `LCD` | `Article`
- `doc_id` — CMS identifier
- `code_system` — as above
- `code`
- `description`
- `coverage_flag`

### `codes_changes.csv`
- `change_type` — `Added` | `Removed` | `FlagChanged`
- `doc_type`
- `doc_id`
- `code_system`
- `code`
- `prev_flag`
- `curr_flag`

## Example (Python)
```python
import pandas as pd
codes = pd.read_csv("codes_normalized.csv")
changes = pd.read_csv("codes_changes.csv")
print(changes['change_type'].value_counts())
