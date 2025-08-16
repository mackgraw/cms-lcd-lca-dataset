# Data Dictionary

## documents_latest.csv
- `doc_id` (str): LCD or Article identifier (e.g., L392XX or 52840)
- `doc_type` (str): `LCD` | `Article`
- `title` (str): Document title
- `contractor` (str): MAC/Contractor
- `state` (str): State/Jurisdiction code(s)
- `status` (str): A|R|F (Active/Retired/Future)
- `source_url` (str): Link to the CMS MCD page

## document_codes_latest.csv
- `article_id` (str): Article identifier
- `code_system` (str): `ICD10-CM` | `HCPCS/CPT` | `Revenue` | `Bill Type` | `HCPCS Modifier`
- `code` (str): Code value
- `description` (str): Human-readable description (if provided by API)
- `coverage_flag` (str): `covered` | `noncovered` | `` (n/a)

## changes_YYYY-MM.csv
- `change_type` (str): `Added` | `Removed` | `FlagChanged`
- `article_id` (str)
- `code_system` (str)
- `code` (str)
- `prev_flag` (str)
- `curr_flag` (str)
