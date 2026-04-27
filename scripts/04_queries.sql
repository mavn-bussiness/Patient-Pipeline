"""
03_load_db.py
-------------
Creates the SQLite database (db/patents.db) from the clean CSVs
produced by 02_clean.py.

Schema:
  patents      (patent_id PK, title, abstract, filing_date, year)
  inventors    (inventor_id PK, name, country)
  companies    (company_id PK, name, country)
  relationships(patent_id, inventor_id, company_id, FK constraints)

Usage:
  python scripts/03_load_db.py [--reset]

  --reset   Drop and recreate all tables before loading.
"""

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE      = Path(__file__).parent.parent
CLEAN_DIR = BASE / "data" / "clean"
DB_PATH   = BASE / "db" / "patents.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── DDL ───────────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS patents (
    patent_id    TEXT PRIMARY KEY,
    title        TEXT,
    abstract     TEXT,
    filing_date  TEXT,
    year         INTEGER
);

CREATE TABLE IF NOT EXISTS inventors (
    inventor_id  TEXT PRIMARY KEY,
    name         TEXT NOT NULL DEFAULT 'Unknown',
    country      TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    company_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL DEFAULT 'Unknown',
    country      TEXT
);

CREATE TABLE IF NOT EXISTS relationships (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    patent_id    TEXT,
    inventor_id  TEXT,
    company_id   TEXT,
    FOREIGN KEY (patent_id)   REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id),
    FOREIGN KEY (company_id)  REFERENCES companies(company_id)
);

-- Indexes for fast JOINs and GROUP BYs
CREATE INDEX IF NOT EXISTS idx_patents_year       ON patents(year);
CREATE INDEX IF NOT EXISTS idx_inventors_country  ON inventors(country);
CREATE INDEX IF NOT EXISTS idx_companies_country  ON companies(country);
CREATE INDEX IF NOT EXISTS idx_rel_patent_id      ON relationships(patent_id);
CREATE INDEX IF NOT EXISTS idx_rel_inventor_id    ON relationships(inventor_id);
CREATE INDEX IF NOT EXISTS idx_rel_company_id     ON relationships(company_id);
"""

DROP_SQL = """
DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS patents;
DROP TABLE IF EXISTS inventors;
DROP TABLE IF EXISTS companies;
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def load_csv(table: str, csv_file: Path, conn: sqlite3.Connection,
             chunk_size: int = 100_000) -> int:
    """Load a CSV into a SQLite table in chunks using INSERT OR IGNORE."""
    if not csv_file.exists():
        print(f"  [SKIP] {csv_file.name} not found — run 02_clean.py first.")
        return 0

    total = 0
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False):
        chunk = chunk.where(pd.notna(chunk), None)   # NaN → None (SQL NULL)
        chunk.to_sql(table, conn, if_exists="append", index=False, method="multi")
        total += len(chunk)
        print(f"    {table}: {total:,} rows inserted…", end="\r")

    print(f"    {table}: {total:,} rows inserted.   ")
    return total


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load clean CSVs into SQLite.")
    parser.add_argument("--reset", action="store_true",
                        help="Drop all tables and reload from scratch.")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  PatentsView → SQLite Loader")
    print(f"  Database : {DB_PATH.resolve()}")
    print(f"{'='*60}\n")

    conn = get_connection()

    if args.reset:
        print("Dropping existing tables…")
        conn.executescript(DROP_SQL)
        conn.commit()

    print("Creating schema…")
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    print("\nLoading tables:")
    load_csv("patents",       CLEAN_DIR / "clean_patents.csv",   conn)
    load_csv("inventors",     CLEAN_DIR / "clean_inventors.csv", conn)
    load_csv("companies",     CLEAN_DIR / "clean_companies.csv", conn)
    load_csv("relationships", CLEAN_DIR / "clean_links.csv",     conn)

    conn.commit()

    # Row-count sanity check
    print("\nRow counts:")
    for tbl in ("patents", "inventors", "companies", "relationships"):
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:<16} {n:>12,}")

    conn.close()
    print(f"\n✓ Database ready at {DB_PATH.resolve()}\n")


if __name__ == "__main__":
    main()