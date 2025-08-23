# Data Dictionary — CMS LCD Dataset

## Files
- `document_codes.csv` — one row per code extracted from LCD/NCD documents.
- `document_nocodes.csv` — documents with no codes detected (auditable list).
- `codes_normalized.csv` — normalized, deduplicated codes with canonical fields.
- `codes_changes.csv` — diff since last release.

## Common Columns (example — adjust to your schema)
- `doc_id` (STRING) — LCD/NCD unique identifier
- `doc_type` (STRING) — LCD or NCD
- `code_system` (STRING) — e.g., CPT, HCPCS, ICD10
- `code` (STRING)
- `description` (STRING)
- `coverage_flag` (BOOLEAN) — Y/N or TRUE/FALSE

> Update this to reflect your actual headers and types.
