"""
02_clean.py
-----------
Reads the raw .tsv.zip files from data/raw/, cleans them with pandas,
and writes four clean CSVs to data/clean/:

  clean_patents.csv    – patent_id, title, abstract, filing_date, year
  clean_inventors.csv  – inventor_id, name, country
  clean_companies.csv  – company_id, name, country
  clean_links.csv      – patent_id, inventor_id, company_id  (relationship table)

Cleaning steps applied:
  - Strip whitespace from all string columns
  - Normalise column names to snake_case
  - Drop rows with null primary keys
  - Remove exact duplicate rows
  - Parse dates; extract year
  - Truncate overlong abstracts (keep first 2 000 chars)
  - Standardise country codes to 2-letter ISO (upper)
  - Fill missing names with 'Unknown'

Usage:
  python scripts/02_clean.py
"""

import re
import zipfile
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE      = Path(__file__).parent.parent
RAW_DIR   = BASE / "data" / "raw"
CLEAN_DIR = BASE / "data" / "clean"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# ── Column mappings ───────────────────────────────────────────────────────────
# PatentsView TSV column names → our canonical names
PATENT_COLS = {
    "patent_id":    "patent_id",
    "patent_title": "title",
    "patent_abstract": "abstract",
    "patent_date":  "filing_date",       # grant date used as proxy
    "date":         "filing_date",
}

INVENTOR_COLS = {
    "disambig_inventor_id": "inventor_id",
    "inventor_id":          "inventor_id",
    "disambig_inventor_name_first": "first_name",
    "disambig_inventor_name_last":  "last_name",
    "inventor_name_first":  "first_name",
    "inventor_name_last":   "last_name",
    "country_transformed":  "country",
    "country":              "country",
}

ASSIGNEE_COLS = {
    "disambig_assignee_id":           "company_id",
    "assignee_id":                    "company_id",
    "disambig_assignee_organization": "name",
    "assignee_organization":          "name",
    "assignee_individual_name_first": "first_name",
    "assignee_individual_name_last":  "last_name",
    "country_transformed":            "country",
    "country":                        "country",
}

PAT_INV_COLS = {
    "patent_id":   "patent_id",
    "inventor_id": "inventor_id",
    "disambig_inventor_id": "inventor_id",
}

PAT_ASS_COLS = {
    "patent_id":   "patent_id",
    "assignee_id": "company_id",
    "disambig_assignee_id": "company_id",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def read_tsv_zip(pattern: str) -> pd.DataFrame | None:
    """Find the first .zip in RAW_DIR matching *pattern* and read its TSV."""
    matches = sorted(RAW_DIR.glob(f"*{pattern}*.zip")) + \
              sorted(RAW_DIR.glob(f"*{pattern}*.tsv"))
    if not matches:
        print(f"  [WARN] No file found for pattern '*{pattern}*'")
        return None

    path = matches[0]
    print(f"  Reading {path.name} …", end=" ")

    try:
        if path.suffix == ".zip":
            with zipfile.ZipFile(path) as zf:
                # Find the TSV inside
                tsvs = [n for n in zf.namelist() if n.endswith(".tsv") or n.endswith(".txt")]
                if not tsvs:
                    print(f"[ERROR] No TSV inside {path.name}")
                    return None
                with zf.open(tsvs[0]) as f:
                    df = pd.read_csv(f, sep="\t", low_memory=False, on_bad_lines="skip")
        else:
            df = pd.read_csv(path, sep="\t", low_memory=False, on_bad_lines="skip")

        print(f"{len(df):,} rows")
        return df
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return None


def normalise_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Lowercase column names, then rename using the provided mapping."""
    df.columns = [c.strip().lower() for c in df.columns]
    rename = {k: v for k, v in mapping.items() if k in df.columns}
    df = df.rename(columns=rename)
    return df


def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from all object columns."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    return df


def iso_country(series: pd.Series) -> pd.Series:
    """Upper-case, keep only 2-letter codes; set rest to NaN."""
    s = series.str.upper().str.strip()
    return s.where(s.str.len() == 2)


# ── 1. Patents ────────────────────────────────────────────────────────────────

def clean_patents() -> pd.DataFrame:
    print("\n[1/4] Patents")
    raw = read_tsv_zip("g_patent")
    if raw is None:
        return pd.DataFrame()

    df = normalise_columns(raw, PATENT_COLS)
    df = clean_strings(df)

    # Keep only needed columns
    keep = [c for c in ["patent_id", "title", "abstract", "filing_date"] if c in df.columns]
    df = df[keep].drop_duplicates(subset=["patent_id"]).dropna(subset=["patent_id"])

    # Parse dates and extract year
    if "filing_date" in df.columns:
        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df["year"] = df["filing_date"].dt.year.astype("Int64")
        df["filing_date"] = df["filing_date"].dt.strftime("%Y-%m-%d")
    else:
        df["filing_date"] = pd.NA
        df["year"] = pd.NA

    # Truncate long abstracts
    if "abstract" in df.columns:
        df["abstract"] = df["abstract"].str[:2000]

    # Fill missing titles
    df["title"] = df.get("title", pd.Series(dtype=str)).fillna("(no title)")

    final_cols = ["patent_id", "title", "abstract", "filing_date", "year"]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[final_cols]

    path = CLEAN_DIR / "clean_patents.csv"
    df.to_csv(path, index=False)
    print(f"  → {len(df):,} patents saved to {path.name}")
    return df


# ── 2. Inventors ──────────────────────────────────────────────────────────────

def clean_inventors() -> pd.DataFrame:
    print("\n[2/4] Inventors")
    raw = read_tsv_zip("g_inventor")
    if raw is None:
        return pd.DataFrame()

    df = normalise_columns(raw, INVENTOR_COLS)
    df = clean_strings(df)

    # Build a full name column
    if "first_name" in df.columns and "last_name" in df.columns:
        df["name"] = (
            df["first_name"].fillna("") + " " + df["last_name"].fillna("")
        ).str.strip()
    elif "name" not in df.columns:
        df["name"] = "Unknown"

    df["name"] = df["name"].replace("", "Unknown").fillna("Unknown")

    if "country" in df.columns:
        df["country"] = iso_country(df["country"])

    df = df.dropna(subset=["inventor_id"]).drop_duplicates(subset=["inventor_id"])

    final_cols = ["inventor_id", "name", "country"]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[final_cols]

    path = CLEAN_DIR / "clean_inventors.csv"
    df.to_csv(path, index=False)
    print(f"  → {len(df):,} inventors saved to {path.name}")
    return df


# ── 3. Companies (Assignees) ──────────────────────────────────────────────────

def clean_companies() -> pd.DataFrame:
    print("\n[3/4] Companies")
    raw = read_tsv_zip("g_assignee")
    if raw is None:
        return pd.DataFrame()

    df = normalise_columns(raw, ASSIGNEE_COLS)
    df = clean_strings(df)

    # Organisation name fallback: individual names
    if "name" not in df.columns or df.get("name", pd.Series()).isna().all():
        if "first_name" in df.columns and "last_name" in df.columns:
            df["name"] = (
                df["first_name"].fillna("") + " " + df["last_name"].fillna("")
            ).str.strip()

    df["name"] = df.get("name", pd.Series(dtype=str)).replace("", "Unknown").fillna("Unknown")

    if "country" in df.columns:
        df["country"] = iso_country(df["country"])

    df = df.dropna(subset=["company_id"]).drop_duplicates(subset=["company_id"])

    final_cols = ["company_id", "name", "country"]
    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[final_cols]

    path = CLEAN_DIR / "clean_companies.csv"
    df.to_csv(path, index=False)
    print(f"  → {len(df):,} companies saved to {path.name}")
    return df


# ── 4. Relationship Table ─────────────────────────────────────────────────────

def clean_links() -> pd.DataFrame:
    """
    Build the relationships table by joining patent-inventor and
    patent-assignee link tables.
    """
    print("\n[4/4] Relationships (link tables)")

    # Patent ↔ Inventor
    raw_pi = read_tsv_zip("g_patent_inventor")
    if raw_pi is not None:
        pi = normalise_columns(raw_pi, PAT_INV_COLS)
        pi = pi[[c for c in ["patent_id", "inventor_id"] if c in pi.columns]]
        pi = pi.dropna().drop_duplicates()
    else:
        pi = pd.DataFrame(columns=["patent_id", "inventor_id"])

    # Patent ↔ Assignee
    raw_pa = read_tsv_zip("g_patent_assignee")
    if raw_pa is not None:
        pa = normalise_columns(raw_pa, PAT_ASS_COLS)
        pa = pa[[c for c in ["patent_id", "company_id"] if c in pa.columns]]
        pa = pa.dropna().drop_duplicates()
    else:
        pa = pd.DataFrame(columns=["patent_id", "company_id"])

    # Outer merge on patent_id to form a single relationship table
    links = pd.merge(pi, pa, on="patent_id", how="outer")

    path = CLEAN_DIR / "clean_links.csv"
    links.to_csv(path, index=False)
    print(f"  → {len(links):,} relationship rows saved to {path.name}")
    return links


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"\n{'='*60}")
    print("  PatentsView Data Cleaning Pipeline")
    print(f"  Source : {RAW_DIR.resolve()}")
    print(f"  Output : {CLEAN_DIR.resolve()}")
    print(f"{'='*60}")

    clean_patents()
    clean_inventors()
    clean_companies()
    clean_links()

    print(f"\n✓ Cleaning complete. Files in {CLEAN_DIR.resolve()}\n")


if __name__ == "__main__":
    main()