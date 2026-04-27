"""
01_scraper.py  (v3)
-------------------
Downloads PatentsView Granted Patent Disambiguated Data from the
USPTO Open Data Portal (ODP).

Correct API endpoints (from ODP Swagger / connector docs):
  GET https://data.uspto.gov/api/v1/datasets/products/{productIdentifier}
      ?fileDataFromDate=YYYY-MM-DD&fileDataToDate=YYYY-MM-DD&includeFiles=true

Tables we want (all .tsv.zip):
  g_patent, g_inventor, g_assignee, g_patent_inventor, g_patent_assignee

Usage:
  python scripts/01_scraper.py [--year YEAR] [--list-only] [--debug]
                               [--proxy http://host:port]
                               [--proxy-user USER] [--proxy-pass PASS]

Proxy examples:
  # HTTP proxy (no auth)
  python scripts/01_scraper.py --proxy http://proxy.example.com:8080

  # HTTP proxy with credentials
  python scripts/01_scraper.py --proxy http://proxy.example.com:8080 \\
      --proxy-user alice --proxy-pass secret

  # SOCKS5 proxy (requires:  pip install requests[socks])
  python scripts/01_scraper.py --proxy socks5://127.0.0.1:1080

  # Read proxy from environment (no flag needed if already set)
  set HTTPS_PROXY=http://proxy.example.com:8080   # Windows
  export HTTPS_PROXY=http://proxy.example.com:8080 # Linux/Mac

  # Disable SSL verification on corporate MITM proxies
  python scripts/01_scraper.py --proxy http://corp-proxy:8080 --no-verify-ssl
"""

import argparse
import os
import sys
import time
from pathlib import Path

import requests
import urllib3
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────
PRODUCT_ID = "pvgpatdis"
RAW_DIR    = Path(__file__).parent.parent / "data" / "raw"

API_BASES = [
    "https://data.uspto.gov/api/v1",
    "https://data.uspto.gov/apis/bulk-data",
]

WANTED_TABLES = {
    "g_patent",
    "g_inventor",
    "g_assignee",
    "g_patent_inventor",
    "g_patent_assignee",
}

DIRECT_URL_TEMPLATE = (
    "https://data.uspto.gov/bulkdata/datasets/pvgpatdis/{filename}"
)

# Module-level session; replaced in main() once proxy args are known
SESSION: requests.Session = requests.Session()


def build_session(
    proxy: str | None = None,
    proxy_user: str | None = None,
    proxy_pass: str | None = None,
    verify_ssl: bool = True,
) -> requests.Session:
    """
    Build a requests.Session pre-configured with proxy and SSL settings.

    Priority for proxy URL:
      1. --proxy CLI argument
      2. HTTPS_PROXY environment variable
      3. HTTP_PROXY environment variable
      4. No proxy
    """
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "PatentPipeline/3.0 (academic research)",
    })

    # Resolve proxy URL
    proxy_url = (
        proxy
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )

    if proxy_url:
        # Embed credentials into the URL if supplied separately
        if proxy_user and proxy_pass:
            from urllib.parse import urlparse, urlunparse
            p = urlparse(proxy_url)
            proxy_url = urlunparse(p._replace(
                netloc=f"{proxy_user}:{proxy_pass}@{p.hostname}:{p.port or ''}"
            ))
        elif proxy_user:
            from urllib.parse import urlparse, urlunparse
            p = urlparse(proxy_url)
            proxy_url = urlunparse(p._replace(
                netloc=f"{proxy_user}@{p.hostname}:{p.port or ''}"
            ))

        s.proxies = {"http": proxy_url, "https": proxy_url}
        # Strip credentials for the display message
        from urllib.parse import urlparse
        safe = urlparse(proxy_url)._replace(netloc=(
            f"{urlparse(proxy_url).hostname}:{urlparse(proxy_url).port or ''}"
        ))
        from urllib.parse import urlunparse
        print(f"  Proxy : {urlunparse(safe)}")
    else:
        print("  Proxy : (none — using direct connection)")

    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        s.verify = False
        print("  SSL   : verification DISABLED (--no-verify-ssl)")

    return s


# ── API discovery ─────────────────────────────────────────────────────────────

def _extract_files(data) -> list[dict]:
    """Pull a file list out of whatever shape the API returns."""
    if isinstance(data, list):
        return data
    for key in ("productFiles", "files", "data", "results", "items"):
        val = data.get(key)
        if val and isinstance(val, list):
            return val
    return []


def try_endpoint(url: str, params: dict, debug: bool) -> list[dict]:
    try:
        r = SESSION.get(url, params=params, timeout=30)
        if debug:
            print(f"\n  [DEBUG] GET {r.url}")
            print(f"  [DEBUG] Status : {r.status_code}")
            print(f"  [DEBUG] CType  : {r.headers.get('content-type','?')}")
            print(f"  [DEBUG] Body   : {r.text[:600]}\n")
        if r.status_code == 200 and r.text.strip():
            return _extract_files(r.json())
    except Exception as exc:
        if debug:
            print(f"  [DEBUG] Exception: {exc}")
    return []


def discover_files(year: int, debug: bool) -> list[dict]:
    date_params = {
        "fileDataFromDate": f"{year}-01-01",
        "fileDataToDate":   f"{year}-12-31",
        "includeFiles":     "true",
        "includeProductFiles": "true",
        "rows": "200",
    }

    for base in API_BASES:
        # Try /datasets/products/{id}
        url = f"{base}/datasets/products/{PRODUCT_ID}"
        print(f"  → {url}")
        files = try_endpoint(url, date_params, debug)
        if files:
            return files

        # Try /datasets/products/search
        url = f"{base}/datasets/products/search"
        print(f"  → {url}")
        files = try_endpoint(url, {"productIdentifier": PRODUCT_ID, **date_params}, debug)
        if files:
            return files

    return []


def filter_wanted(files: list[dict]) -> list[dict]:
    out = []
    for f in files:
        fname = (
            f.get("fileName") or f.get("name") or
            f.get("fileIdentifier") or ""
        ).lower()
        if any(t in fname for t in WANTED_TABLES):
            out.append(f)
    return out


def get_download_url(f: dict, fname: str) -> str:
    for key in ("downloadUrl", "fileUrl", "url", "href", "fileDownloadUrl"):
        if val := f.get(key):
            return val
    return DIRECT_URL_TEMPLATE.format(filename=fname)


# ── Downloader ────────────────────────────────────────────────────────────────

def stream_download(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"  [SKIP] Already exists: {dest.name}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  Downloading → {dest.name}")
    with SESSION.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as fh, tqdm(
            total=total, unit="B", unit_scale=True,
            unit_divisor=1024, desc=dest.name[:40], leave=False,
        ) as bar:
            for chunk in r.iter_content(chunk_size=256 * 1024):
                fh.write(chunk)
                bar.update(len(chunk))


# ── Direct-URL fallback ───────────────────────────────────────────────────────

def try_direct_downloads(year: int) -> None:
    """
    Probe known filename patterns directly.
    PatentsView releases use g_<table>_<year>.tsv.zip or g_<table>.tsv.zip.
    """
    print("\n  Trying known filename patterns directly…")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    found = 0

    base_names = list(WANTED_TABLES)
    candidates: list[str] = []
    for t in base_names:
        candidates.append(f"{t}_{year}.tsv.zip")   # e.g. g_patent_2023.tsv.zip
        candidates.append(f"{t}.tsv.zip")           # e.g. g_patent.tsv.zip

    for fname in candidates:
        dest = RAW_DIR / fname
        if dest.exists():
            print(f"  [SKIP] {fname}")
            found += 1
            continue
        url = DIRECT_URL_TEMPLATE.format(filename=fname)
        try:
            head = SESSION.head(url, timeout=15, allow_redirects=True)
            if head.status_code == 200:
                stream_download(url, dest)
                found += 1
                time.sleep(0.3)
        except Exception:
            pass

    if found == 0:
        print(
            "\n"
            "  ╔══════════════════════════════════════════════════════╗\n"
            "  ║  MANUAL DOWNLOAD REQUIRED                            ║\n"
            "  ╠══════════════════════════════════════════════════════╣\n"
            "  ║  Please download these 5 files from the USPTO site   ║\n"
            "  ║  and place them in the  data/raw/  folder:           ║\n"
            "  ║                                                      ║\n"
            "  ║  • g_patent.tsv.zip                                  ║\n"
            "  ║  • g_inventor.tsv.zip                                ║\n"
            "  ║  • g_assignee.tsv.zip                                ║\n"
            "  ║  • g_patent_inventor.tsv.zip                         ║\n"
            "  ║  • g_patent_assignee.tsv.zip                         ║\n"
            "  ║                                                      ║\n"
            "  ║  URL:                                                ║\n"
            "  ║  https://data.uspto.gov/bulkdata/datasets/pvgpatdis  ║\n"
            "  ║                                                      ║\n"
            "  ║  After downloading, run:                             ║\n"
            "  ║    python scripts/02_clean.py                        ║\n"
            "  ╚══════════════════════════════════════════════════════╝\n"
        )
    else:
        print(f"\n  ✓ {found} file(s) ready in {RAW_DIR.resolve()}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download PatentsView bulk data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Proxy examples:
  --proxy http://proxy.corp.com:8080
  --proxy http://proxy.corp.com:8080 --proxy-user alice --proxy-pass secret
  --proxy socks5://127.0.0.1:1080       (needs: pip install requests[socks])
  --no-verify-ssl                        (for corporate MITM/SSL-inspection proxies)

  Or set environment variables before running:
    set HTTPS_PROXY=http://proxy.corp.com:8080   (Windows)
    export HTTPS_PROXY=http://proxy.corp.com:8080 (Linux/Mac)
        """,
    )
    parser.add_argument("--year",           type=int, default=2023)
    parser.add_argument("--list-only",      action="store_true")
    parser.add_argument("--debug",          action="store_true",
                        help="Print raw API responses.")
    parser.add_argument("--proxy",          type=str, default=None,
                        metavar="URL",
                        help="Proxy URL, e.g. http://host:port or socks5://host:port")
    parser.add_argument("--proxy-user",     type=str, default=None,
                        metavar="USER",
                        help="Proxy username (optional)")
    parser.add_argument("--proxy-pass",     type=str, default=None,
                        metavar="PASS",
                        help="Proxy password (optional)")
    parser.add_argument("--no-verify-ssl",  action="store_true",
                        help="Disable SSL certificate verification (use with "
                             "corporate proxies that do SSL inspection)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  PatentsView Bulk Data Scraper  (v3)")
    print(f"  Product : {PRODUCT_ID}   Year: {args.year}")
    print(f"{'='*60}\n")

    # Build the session with proxy settings
    global SESSION
    SESSION = build_session(
        proxy=args.proxy,
        proxy_user=args.proxy_user,
        proxy_pass=args.proxy_pass,
        verify_ssl=not args.no_verify_ssl,
    )
    print()

    print("Step 1/2 – Querying USPTO ODP API…")
    all_files = discover_files(args.year, args.debug)
    wanted    = filter_wanted(all_files)

    if wanted:
        print(f"\n  Found {len(wanted)} matching file(s):\n")
        for f in wanted:
            name = f.get("fileName") or f.get("name") or "(unknown)"
            size = f.get("fileSize") or f.get("size") or "?"
            print(f"    {name:<55} {size}")

        if args.list_only:
            print("\n[--list-only] Exiting without downloading.")
            return

        print(f"\nStep 2/2 – Downloading to {RAW_DIR}/…")
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        ok = fail = 0
        for f in wanted:
            fname = f.get("fileName") or f.get("name") or "unknown.zip"
            url   = get_download_url(f, fname)
            try:
                stream_download(url, RAW_DIR / fname)
                ok += 1
            except Exception as exc:
                print(f"  [ERROR] {fname}: {exc}")
                (RAW_DIR / fname).unlink(missing_ok=True)
                fail += 1
            time.sleep(0.3)

        print(f"\n✓ Done. {ok} downloaded, {fail} failed.")

    else:
        print("  No files returned from API.")
        if args.list_only:
            return
        print("\nStep 2/2 – Trying direct URL fallback…")
        try_direct_downloads(args.year)

    print(f"\n  Raw files in: {RAW_DIR.resolve()}\n")


if __name__ == "__main__":
    main()