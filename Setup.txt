# CMS LCD/LCA Dataset Starter (Coverage API + GitHub Actions)

This starter pulls Medicare **Local Coverage Determinations (LCDs)** and **Local Coverage Articles (LCAs)** from the CMS **Coverage API**, extracts code mappings (ICD-10-CM, HCPCS/CPT, revenue, bill types), tracks revision history, and publishes three CSVs on a schedule.

- CMS Coverage API requires **no API key** and enforces throttling.
- **Most codes live in Articles, not LCDs** (DME LCDs may include HCPCS).
- Weekly **MCD Downloads** can be used to reconcile/backfill.

## Quick start

1. Create a new GitHub repo (e.g., `cms-lcd-lca-dataset`).
2. Download the starter ZIP from ChatGPT and unzip.
3. Local test:

```
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python scripts/run_once.py
```
4. Commit & push to GitHub, then enable Actions.
5. Run the **Update Dataset** workflow manually. When it finishes, check **Releases** for CSVs.

## Configure filters

Edit `config/config.yaml` or set repo **Actions → Variables**:

```yaml
states: ["FL","PA"]     # leave empty for all
status: "Active"        # Active | Retired | Future Effective | all
contractors: []         # optional contractor numbers or names
max_docs_per_run: 250
request_timeout_sec: 30
```

Env overrides: `COVERAGE_STATES`, `COVERAGE_STATUS`, `COVERAGE_CONTRACTORS`, `COVERAGE_MAX_DOCS`, `COVERAGE_TIMEOUT`.

## Outputs

- `dataset/documents_latest.csv` – LCD/Article metadata.
- `dataset/document_codes_latest.csv` – one row per document–code.
- `dataset/changes_YYYY-MM.csv` – diff vs prior snapshot.

## Notes

- Include `source_url` to CMS MCD for traceability.
- Limit public redistribution of proprietary CPT text (codes/IDs are fine).
