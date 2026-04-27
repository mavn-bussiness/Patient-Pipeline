"""
02_clean.py
-----------
Reads raw .tsv.zip (or plain .tsv) files from data/raw/, cleans them with
pandas, and writes four clean CSVs to data/clean/:

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

import zipfile
from pathlib import Path

import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE      = Path(__file__).parent.parent
RAW_DIR   = BASE / "data" / "raw"
CLEAN_DIR = BASE / "data" / "clean"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# ── Column mappings ────────────────────────────────────────────────────────────
PATENT_COLS = {
    "patent_id":       "patent_id",
    "patent_title":    "title",
    "patent_abstract": "abstract",
    "patent_date":     "filing_date",
    "date":            "filing_date",
}

INVENTOR_COLS = {
    "disambig_inventor_id":          "inventor_id",
    "inventor_id":                   "inventor_id",
    "disambig_inventor_name_first":  "first_name",
    "disambig_inventor_name_last":   "last_name",
    "inventor_name_first":           "first_name",
    "inventor_name_last":            "last_name",
    "country_transformed":           "country",
    "country":                       "country",
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
    "patent_id":            "patent_id",
    "inventor_id":          "inventor_id",
    "disambig_inventor_id": "inventor_id",
}

PAT_ASS_COLS = {
    "patent_id":              "patent_id",
    "assignee_id":            "company_id",
    "disambig_assignee_id":   "company_id",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def read_tsv_zip(pattern: str) -> pd.DataFrame | None:
    """
    Find the first matching file in RAW_DIR and load it as a TSV.

    Priority order:
      1. *.zip  – try to open as a ZIP; if it's a fake zip (plain TSV
                  with a .zip extension), fall back to reading directly.
      2. *.tsv  – read directly.

    This handles the common PatentsView quirk where files are distributed
    as plain TSVs that carry a .zip extension.
    """
    candidates = (
        sorted(RAW_DIR.glob(f"*{pattern}*.zip"))
        + sorted(RAW_DIR.glob(f"*{pattern}*.tsv"))
    )
    if not candidates:
        print(f"  [WARN] No file found matching '*{pattern}*'")
        return None

    path = candidates[0]
    print(f"  Reading {path.name} …", end=" ", flush=True)

    try:
        if path.suffix == ".zip":
            df = _read_zip_or_plain(path)
        else:
            df = _read_plain_tsv(path)

        if df is not None:
            print(f"{len(df):,} rows")
        return df

    except Exception as exc:
        print(f"[ERROR] Unexpected error: {exc}")
        return None


def _read_zip_or_plain(path: Path) -> pd.DataFrame | None:
    """Open a .zip file; if it is not a valid ZIP, read it as a plain TSV."""
    try:
        with zipfile.ZipFile(path) as zf:
            tsvs = [
                name for name in zf.namelist()
                if name.endswith(".tsv") or name.endswith(".txt")
            ]
            if not tsvs:
                print(f"[ERROR] No TSV entry found inside {path.name}")
                return None
            with zf.open(tsvs[0]) as f:
                return pd.read_csv(f, sep="\t", low_memory=False, on_bad_lines="skip")
    except zipfile.BadZipFile:
        # The file has a .zip extension but is really a plain TSV.
        print("[WARN: not a real ZIP, reading as plain TSV] …", end=" ", flush=True)
        return _read_plain_tsv(path)


def _read_plain_tsv(path: Path) -> pd.DataFrame | None:
    """Read a plain TSV file (any extension)."""
    try:
        return pd.read_csv(path, sep="\t", low_memory=False, on_bad_lines="skip")
    except Exception as exc:
        print(f"[ERROR] Could not read {path.name}: {exc}")
        return None


def normalise_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Lowercase column names, then apply the canonical rename mapping."""
    df.columns = [c.strip().lower() for c in df.columns]
    rename = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=rename)


def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from every string column."""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
    return df


def iso_country(series: pd.Series) -> pd.Series:
    """Upper-case and keep only valid 2-letter ISO country codes; rest → NaN."""
    s = series.str.upper().str.strip()
    return s.where(s.str.len() == 2)


def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add any missing columns as pd.NA so the final select never fails."""
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    return df[cols]


# ── 1. Patents ─────────────────────────────────────────────────────────────────

def clean_patents() -> pd.DataFrame:
    print("\n[1/4] Patents")
    raw = read_tsv_zip("g_patent")
    if raw is None:
        return pd.DataFrame()

    df = normalise_columns(raw, PATENT_COLS)
    df = clean_strings(df)

    # Keep only the columns we care about
    keep = [c for c in ["patent_id", "title", "abstract", "filing_date"] if c in df.columns]
    df = df[keep].drop_duplicates(subset=["patent_id"]).dropna(subset=["patent_id"])

    # Parse dates and derive year
    if "filing_date" in df.columns:
        df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
        df["year"]        = df["filing_date"].dt.year.astype("Int64")
        df["filing_date"] = df["filing_date"].dt.strftime("%Y-%m-%d")
    else:
        df["filing_date"] = pd.NA
        df["year"]        = pd.NA

    # Truncate long abstracts and fill blank titles
    if "abstract" in df.columns:
        df["abstract"] = df["abstract"].str[:2_000]

    df["title"] = df.get("title", pd.Series(dtype=str)).fillna("(no title)")

    df = ensure_columns(df, ["patent_id", "title", "abstract", "filing_date", "year"])

    out = CLEAN_DIR / "clean_patents.csv"
    df.to_csv(out, index=False)
    print(f"  → {len(df):,} patents  →  {out.name}")
    return df


# ── 2. Inventors ───────────────────────────────────────────────────────────────

def clean_inventors() -> pd.DataFrame:
    print("\n[2/4] Inventors")
    raw = read_tsv_zip("g_inventor")
    if raw is None:
        return pd.DataFrame()

    df = normalise_columns(raw, INVENTOR_COLS)
    df = clean_strings(df)

    # Build full name from first + last if no 'name' column exists
    if "name" not in df.columns:
        if "first_name" in df.columns and "last_name" in df.columns:
            df["name"] = (
                df["first_name"].fillna("") + " " + df["last_name"].fillna("")
            ).str.strip()
        else:
            df["name"] = "Unknown"

    df["name"] = df["name"].replace("", "Unknown").fillna("Unknown")

    if "country" in df.columns:
        df["country"] = iso_country(df["country"])

    df = df.dropna(subset=["inventor_id"]).drop_duplicates(subset=["inventor_id"])
    df = ensure_columns(df, ["inventor_id", "name", "country"])

    out = CLEAN_DIR / "clean_inventors.csv"
    df.to_csv(out, index=False)
    print(f"  → {len(df):,} inventors  →  {out.name}")
    return df


# ── 3. Companies (Assignees) ───────────────────────────────────────────────────

def clean_companies() -> pd.DataFrame:
    print("\n[3/4] Companies")
    raw = read_tsv_zip("g_assignee")
    if raw is None:
        return pd.DataFrame()

    df = normalise_columns(raw, ASSIGNEE_COLS)
    df = clean_strings(df)

    # Organisation name fallback: use individual name components
    name_missing = "name" not in df.columns or df.get("name", pd.Series()).isna().all()
    if name_missing and "first_name" in df.columns and "last_name" in df.columns:
        df["name"] = (
            df["first_name"].fillna("") + " " + df["last_name"].fillna("")
        ).str.strip()

    df["name"] = df.get("name", pd.Series(dtype=str)).replace("", "Unknown").fillna("Unknown")

    if "country" in df.columns:
        df["country"] = iso_country(df["country"])

    df = df.dropna(subset=["company_id"]).drop_duplicates(subset=["company_id"])
    df = ensure_columns(df, ["company_id", "name", "country"])

    out = CLEAN_DIR / "clean_companies.csv"
    df.to_csv(out, index=False)
    print(f"  → {len(df):,} companies  →  {out.name}")
    return df


# ── 4. Relationship Table ──────────────────────────────────────────────────────

def clean_links() -> pd.DataFrame:
    """
    Build a single relationship table by joining the patent↔inventor and
    patent↔assignee link files on patent_id (outer merge).
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

    links = pd.merge(pi, pa, on="patent_id", how="outer")

    out = CLEAN_DIR / "clean_links.csv"
    links.to_csv(out, index=False)
    print(f"  → {len(links):,} relationship rows  →  {out.name}")
    return links


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"\n{'=' * 60}")
    print("  PatentsView Data Cleaning Pipeline")
    print(f"  Source : {RAW_DIR.resolve()}")
    print(f"  Output : {CLEAN_DIR.resolve()}")
    print(f"{'=' * 60}")

    clean_patents()
    clean_inventors()
    clean_companies()
    clean_links()

    print(f"\n✓ Cleaning complete. Files written to {CLEAN_DIR.resolve()}\n")


if __name__ == "__main__":
    main()