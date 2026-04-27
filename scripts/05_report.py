"""
05_report.py
------------
Runs all analysis queries against db/patents.db and produces:

  Console  – formatted terminal report
  CSV      – reports/top_inventors.csv
             reports/top_companies.csv
             reports/country_trends.csv
             reports/yearly_trends.csv
  JSON     – reports/report.json

Usage:
  python scripts/05_report.py [--top N]

  --top N   Number of top entries to show (default: 20)
"""

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).parent.parent
DB_PATH     = BASE / "db" / "patents.db"
REPORTS_DIR = BASE / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ── DB helper ─────────────────────────────────────────────────────────────────

def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn, params=params)


# ── Queries ───────────────────────────────────────────────────────────────────

def get_totals() -> dict:
    totals = {}
    for tbl in ("patents", "inventors", "companies", "relationships"):
        n = query(f"SELECT COUNT(*) AS n FROM {tbl}")["n"][0]
        totals[tbl] = int(n)
    return totals


def get_top_inventors(n: int) -> pd.DataFrame:
    return query("""
        SELECT
            i.name                        AS inventor,
            i.country,
            COUNT(DISTINCT r.patent_id)   AS patent_count
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        WHERE i.name != 'Unknown'
        GROUP BY i.inventor_id
        ORDER BY patent_count DESC
        LIMIT ?
    """, (n,))


def get_top_companies(n: int) -> pd.DataFrame:
    return query("""
        SELECT
            c.name                        AS company,
            c.country,
            COUNT(DISTINCT r.patent_id)   AS patent_count
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        WHERE c.name != 'Unknown'
        GROUP BY c.company_id
        ORDER BY patent_count DESC
        LIMIT ?
    """, (n,))


def get_country_trends(n: int) -> pd.DataFrame:
    total_q = query("SELECT COUNT(*) AS n FROM patents")["n"][0]
    df = query("""
        SELECT
            i.country,
            COUNT(DISTINCT r.patent_id)   AS patent_count
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        WHERE i.country IS NOT NULL
        GROUP BY i.country
        ORDER BY patent_count DESC
        LIMIT ?
    """, (n,))
    df["share_pct"] = (df["patent_count"] / total_q * 100).round(2)
    return df


def get_yearly_trends() -> pd.DataFrame:
    return query("""
        SELECT
            year,
            COUNT(*) AS patents_granted
        FROM patents
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


# ── Console Report ────────────────────────────────────────────────────────────

def print_section(title: str) -> None:
    width = 60
    print(f"\n{'─' * width}")
    print(f"  {title}")
    print(f"{'─' * width}")


def console_report(totals: dict, inventors: pd.DataFrame,
                   companies: pd.DataFrame, countries: pd.DataFrame,
                   yearly: pd.DataFrame, top_n: int) -> None:

    width = 60
    print(f"\n{'=' * width}")
    print("  PATENT INTELLIGENCE REPORT")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * width}")

    print_section("DATABASE SUMMARY")
    for k, v in totals.items():
        print(f"  {k:<16} {v:>12,}")

    print_section(f"TOP {top_n} INVENTORS")
    for i, row in inventors.iterrows():
        country = f"({row.get('country', '?') or '?'})"
        print(f"  {i+1:>3}. {row['inventor']:<35} {row['patent_count']:>6,}  {country}")

    print_section(f"TOP {top_n} COMPANIES")
    for i, row in companies.iterrows():
        country = f"({row.get('country', '?') or '?'})"
        print(f"  {i+1:>3}. {row['company']:<35} {row['patent_count']:>6,}  {country}")

    print_section(f"TOP {top_n} COUNTRIES")
    for i, row in countries.iterrows():
        share = f"{row.get('share_pct', 0):.1f}%"
        print(f"  {i+1:>3}. {row['country']:<8} {row['patent_count']:>8,}  ({share})")

    if not yearly.empty:
        print_section("YEARLY TREND (last 10 years)")
        tail = yearly.tail(10)
        for _, row in tail.iterrows():
            bar = "█" * min(int(row["patents_granted"] / max(yearly["patents_granted"].max(), 1) * 40), 40)
            print(f"  {int(row['year'])}: {bar:<40} {int(row['patents_granted']):>8,}")

    print(f"\n{'=' * width}\n")


# ── Export CSVs ───────────────────────────────────────────────────────────────

def export_csvs(inventors: pd.DataFrame, companies: pd.DataFrame,
                countries: pd.DataFrame, yearly: pd.DataFrame) -> None:
    inventors.to_csv(REPORTS_DIR / "top_inventors.csv",  index=False)
    companies.to_csv(REPORTS_DIR / "top_companies.csv",  index=False)
    countries.to_csv(REPORTS_DIR / "country_trends.csv", index=False)
    yearly.to_csv(REPORTS_DIR    / "yearly_trends.csv",  index=False)
    print(f"  CSV reports written to {REPORTS_DIR.resolve()}/")


# ── Export JSON ───────────────────────────────────────────────────────────────

def export_json(totals: dict, inventors: pd.DataFrame,
                companies: pd.DataFrame, countries: pd.DataFrame,
                yearly: pd.DataFrame) -> None:
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_patents": totals.get("patents", 0),
        "total_inventors": totals.get("inventors", 0),
        "total_companies": totals.get("companies", 0),
        "top_inventors": [
            {
                "rank": i + 1,
                "name": row["inventor"],
                "country": row.get("country"),
                "patents": int(row["patent_count"]),
            }
            for i, row in inventors.iterrows()
        ],
        "top_companies": [
            {
                "rank": i + 1,
                "name": row["company"],
                "country": row.get("country"),
                "patents": int(row["patent_count"]),
            }
            for i, row in companies.iterrows()
        ],
        "top_countries": [
            {
                "rank": i + 1,
                "country": row["country"],
                "patents": int(row["patent_count"]),
                "share_pct": float(row.get("share_pct", 0)),
            }
            for i, row in countries.iterrows()
        ],
        "yearly_trends": [
            {"year": int(row["year"]), "patents": int(row["patents_granted"])}
            for _, row in yearly.iterrows()
        ],
    }

    path = REPORTS_DIR / "report.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    print(f"  JSON report written to {path.resolve()}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate patent reports.")
    parser.add_argument("--top", type=int, default=20,
                        help="Number of top entries to include (default: 20).")
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. Run 03_load_db.py first."
        )

    print(f"\n{'='*60}")
    print("  Generating Patent Intelligence Reports")
    print(f"  Database : {DB_PATH.resolve()}")
    print(f"{'='*60}\n")

    print("Querying database…")
    totals    = get_totals()
    inventors = get_top_inventors(args.top)
    companies = get_top_companies(args.top)
    countries = get_country_trends(args.top)
    yearly    = get_yearly_trends()

    # Console
    console_report(totals, inventors, companies, countries, yearly, args.top)

    # Files
    print("Writing reports…")
    export_csvs(inventors, companies, countries, yearly)
    export_json(totals, inventors, companies, countries, yearly)

    print("\n✓ All reports complete.\n")


if __name__ == "__main__":
    main()