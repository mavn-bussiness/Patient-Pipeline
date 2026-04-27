# Global Patent Intelligence Data Pipeline

A complete data engineering pipeline that collects, cleans, stores, and analyses
U.S. patent data from the USPTO PatentsView dataset.

---

## Project Structure

```
patent-pipeline/
├── data/
│   ├── raw/          # downloaded .zip files from USPTO
│   ├── extracted/    # (intermediate, auto-created)
│   └── clean/        # cleaned CSVs output by 02_clean.py
├── db/
│   ├── schema.sql    # standalone DDL for submission
│   └── patents.db    # SQLite database (created by 03_load_db.py)
├── reports/
│   ├── top_inventors.csv
│   ├── top_companies.csv
│   ├── country_trends.csv
│   ├── yearly_trends.csv
│   └── report.json
├── scripts/
│   ├── 01_scraper.py    # download bulk data from USPTO ODP
│   ├── 02_clean.py      # clean & normalise with pandas
│   ├── 03_load_db.py    # load into SQLite
│   ├── 04_queries.sql   # all 7 analysis queries
│   └── 05_report.py     # generate console + CSV + JSON reports
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone <your-repo-url>
cd patent-pipeline
pip install -r requirements.txt
```

### 2. Download the data

```bash
# Download data for 2023 (recommended — ~1-3 GB)
python scripts/01_scraper.py --year 2023

# Preview available files without downloading
python scripts/01_scraper.py --year 2023 --list-only
```

> **Manual fallback:** If the API is unavailable, visit  
> https://data.uspto.gov/bulkdata/datasets/pvgpatdis  
> and manually download these five tables into `data/raw/`:
> - `g_patent.tsv.zip`
> - `g_inventor.tsv.zip`
> - `g_assignee.tsv.zip`
> - `g_patent_inventor.tsv.zip`
> - `g_patent_assignee.tsv.zip`

### 3. Clean the data

```bash
python scripts/02_clean.py
```

Outputs to `data/clean/`:
- `clean_patents.csv`
- `clean_inventors.csv`
- `clean_companies.csv`
- `clean_links.csv`

### 4. Load into SQLite

```bash
python scripts/03_load_db.py

# To reset and reload from scratch:
python scripts/03_load_db.py --reset
```

### 5. Generate reports

```bash
python scripts/05_report.py

# Show top 10 instead of 20:
python scripts/05_report.py --top 10
```

### 6. Run SQL queries directly

```bash
sqlite3 db/patents.db < scripts/04_queries.sql
```

---

## Pipeline Diagram

```
USPTO ODP API
    │
    ▼
01_scraper.py  →  data/raw/*.tsv.zip
    │
    ▼
02_clean.py    →  data/clean/*.csv
    │
    ▼
03_load_db.py  →  db/patents.db
    │
    ├──► 04_queries.sql  (run directly in SQLite)
    │
    ▼
05_report.py   →  reports/top_inventors.csv
               →  reports/top_companies.csv
               →  reports/country_trends.csv
               →  reports/yearly_trends.csv
               →  reports/report.json
               →  Console output
```

---

## Database Schema

```sql
patents       (patent_id PK, title, abstract, filing_date, year)
inventors     (inventor_id PK, name, country)
companies     (company_id PK, name, country)
relationships (id PK, patent_id FK, inventor_id FK, company_id FK)
```

Full DDL is in `db/schema.sql`.

---

## SQL Queries (04_queries.sql)

| # | Query | Description |
|---|-------|-------------|
| Q1 | Top Inventors | Most patents by inventor |
| Q2 | Top Companies | Most patents by assignee |
| Q3 | Countries | Patent share by inventor country |
| Q4 | Trends Over Time | Patents granted per year |
| Q5 | JOIN Query | Patents with inventors and companies |
| Q6 | CTE Query | Top inventors per country (WITH clause) |
| Q7 | Ranking Query | Window functions: RANK, DENSE_RANK, NTILE |

---

## Data Source

**PatentsView Granted Patent Disambiguated Data**  
https://data.uspto.gov/bulkdata/datasets/pvgpatdis

U.S. Patent and Trademark Office. PatentsView data is released under
Creative Commons Attribution 4.0 International License.

---

## Reproducibility

Anyone can clone this repo and run the five scripts in order to reproduce
identical results from the same source data:

```bash
pip install -r requirements.txt
python scripts/01_scraper.py --year 2023
python scripts/02_clean.py
python scripts/03_load_db.py
python scripts/05_report.py
```