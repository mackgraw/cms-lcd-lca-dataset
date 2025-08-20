
---

# DATA_DICTIONARY.md (replace entire file)

```markdown
# Data Dictionary

## document_codes.csv
Merged per‑document code rows from all shards.

- `document_type` (str): `LCD` | `Article`
- `document_id` (str): CMS identifier (e.g., `L392XX`, `52840`)
- `code_system` (str): `ICD10-CM` | `HCPCS/CPT` | `Revenue` | `Bill Type` | `HCPCS Modifier`
- `code` (str): Code value
- `description` (str): Optional text from API
- `coverage_flag` (str): `covered` | `noncovered` | `` (empty/n.a.)

## document_nocodes.csv
Documents fetched without any code rows this run.

- `document_type` (str)
- `document_id` (str)
- Additional fetch/context columns may be present.

## codes_normalized.csv
Stable, analysis‑friendly view across **Articles & LCDs**.

- `doc_type` (str): `LCD` | `Article`
- `doc_id` (str)
- `code_system` (str)
- `code` (str)
- `description` (str)
- `coverage_flag` (str)

## codes_changes.csv
Day‑over‑day diff vs the previous release.

- `change_type` (str): `Added` | `Removed` | `FlagChanged`
- `doc_type` (str)
- `doc_id` (str)
- `code_system` (str)
- `code` (str)
- `prev_flag` (str)
- `curr_flag` (str)
