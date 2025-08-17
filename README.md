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

## Dataset Fields

The dataset includes three main CSVs:

- **documents_latest.csv**  
  Contains metadata for each LCD and Article.  
  Columns:
  - `document_id`: CMS identifier
  - `doc_type`: LCD or Article
  - `title`: Document title
  - `status`: Current status (Active, Retired, Future)
  - `effective_date`: Effective date if present
  - `source_url`: Direct link to the CMS Medicare Coverage Database (LCD/Article page)

- **document_codes_latest.csv**  
  Contains ICD10, HCPCS, CPT, Bill Type, Revenue, and Modifier codes associated with articles.  
  Columns:
  - `article_id`
  - `code`
  - `description`
  - `coverage_flag` (e.g., covered, noncovered)
  - `code_system`

- **changes_YYYY-MM.csv**  
  Tracks changes in codes month-over-month.  
  Columns:
  - `change_type` (added/removed/changed)
  - `article_id`
  - `code_system`
  - `code`
  - `prev_flag`
  - `curr_flag`

## Example Usage

Load in Python with pandas:

```python
import pandas as pd

docs = pd.read_csv("dataset/documents_latest.csv")
codes = pd.read_csv("dataset/document_codes_latest.csv")

# See first few documents with direct CMS URLs
print(docs[["document_id", "title", "source_url"]].head())

