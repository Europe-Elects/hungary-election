#!/usr/bin/env python3
"""
Hungary 2026 Election Live Scraper
===================================
Fetches results from valasztas.hu and updates results.json.
Both maps auto-refresh from results.json every 30 seconds.

Usage:
  python3 scraper.py                          # single fetch
  python3 scraper.py --loop                   # continuous polling (every 60s)
  python3 scraper.py --loop --interval 30     # custom interval
  python3 scraper.py --loop --push            # poll + auto git push to GitHub Pages
  python3 scraper.py --csv hungary_NA_national_unicameral_constituencies_2026_04.csv
  python3 scraper.py --csv-county hungary_NA_national_unicameral_counties_2026_04.csv
  python3 scraper.py --csv hungary_NA_national_unicameral_constituencies_2026_04.csv --csv-county hungary_NA_national_unicameral_counties_2026_04.csv --push
  python3 scraper.py --test                   # inject test data
  python3 scraper.py --clear --push           # reset to empty + push

Before election day:
  1. Visit https://vtr.valasztas.hu/ogy2026 in your browser
  2. Open DevTools > Network tab, filter by XHR/Fetch
  3. Find the JSON endpoints for results data
  4. Update BASE_URL and endpoint paths below
"""

import json
import time
import argparse
import csv
import os
import sys
import subprocess
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

# ──────────────────────────────────────────────
#  CONFIGURATION — update these on election day
# ──────────────────────────────────────────────

# Base URL for the election results API
# For 2022 it was: https://vtr.valasztas.hu/ogy2022/data
# For 2026 it will likely be: https://vtr.valasztas.hu/ogy2026/data
BASE_URL = "https://vtr.valasztas.hu/ogy2026/data"

# Alternative: direct static JSON endpoints (check on election day)
# These are common patterns used by valasztas.hu
ENDPOINTS = {
    "config": f"{BASE_URL}/config.json",
    # These will need to be discovered on election day:
    # "constituency": f"{BASE_URL}/oevk.json",
    # "party_list": f"{BASE_URL}/partlista.json",
}

# Output file path
RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json")

# MAZ code → county name mapping
MAZ_TO_COUNTY = {
    '01': 'Budapest', '02': 'Baranya', '03': 'Bács-Kiskun', '04': 'Békés',
    '05': 'Borsod-Abaúj-Zemplén', '06': 'Csongrád-Csanád', '07': 'Fejér',
    '08': 'Győr-Moson-Sopron', '09': 'Hajdú-Bihar', '10': 'Heves',
    '11': 'Jász-Nagykun-Szolnok', '12': 'Komárom-Esztergom', '13': 'Nógrád',
    '14': 'Pest', '15': 'Somogy', '16': 'Szabolcs-Szatmár-Bereg',
    '17': 'Tolna', '18': 'Vas', '19': 'Veszprém', '20': 'Zala'
}

MAZ_TO_ID = {
    '01': 'BP', '02': 'BA', '03': 'BK', '04': 'BE', '05': 'BO', '06': 'CS',
    '07': 'FE', '08': 'GY', '09': 'HB', '10': 'HE', '11': 'JN', '12': 'KE',
    '13': 'NO', '14': 'PE', '15': 'SO', '16': 'SZ', '17': 'TO', '18': 'VA',
    '19': 'VE', '20': 'ZA'
}

# Registered parties for 2026
PARTIES = ['FIDESZ-KDNP', 'TISZA', 'DK', 'Mi Hazánk', 'MKKP']

COUNTIES = list(MAZ_TO_COUNTY.values())


def fetch_json(url):
    """Fetch JSON from a URL with error handling."""
    try:
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0 Hungary-Election-Tracker'})
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except URLError as e:
        print(f"  [ERROR] Failed to fetch {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"  [ERROR] Invalid JSON from {url}: {e}")
        return None


def load_current_results():
    """Load existing results.json or return empty structure."""
    try:
        with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return empty_results()


def empty_results():
    """Return empty results structure."""
    county_list = {}
    for county in COUNTIES:
        county_list[county] = {p: 0 for p in PARTIES}
    return {
        "lastUpdated": datetime.now().isoformat(),
        "constituencies": {},
        "countyList": county_list
    }


def save_results(results):
    """Write results to results.json."""
    results['lastUpdated'] = datetime.now().isoformat()
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Saved results.json at {results['lastUpdated']}")


def scrape_results():
    """
    Main scraping function.

    ON ELECTION DAY: Update this function once you discover the actual
    API endpoints and data format from valasztas.hu.

    The function should:
    1. Fetch constituency results (106 districts)
    2. Fetch national list results (by county)
    3. Parse into our results.json format
    4. Return the results dict
    """
    results = load_current_results()

    # Step 1: Try to fetch config to check if data is available
    config = fetch_json(ENDPOINTS.get("config", f"{BASE_URL}/config.json"))
    if config:
        print(f"  Config: {config}")
        # The config usually contains version strings that change when new data arrives
        # Use these to construct versioned data URLs

    # Step 2: Fetch constituency results
    # TODO: Update URL pattern on election day
    # Example patterns to try:
    #   {BASE_URL}/{version}/egyeni.json
    #   {BASE_URL}/oevk/{maz}/{evk}.json
    #   {BASE_URL}/results.json

    # Step 3: Fetch national list results
    # TODO: Update URL pattern on election day
    # Example patterns to try:
    #   {BASE_URL}/{version}/lista.json
    #   {BASE_URL}/partlista/{county}.json

    print("  [INFO] Auto-scraping not yet configured. Use --test for test data,")
    print("         or update scraper.py with discovered endpoints on election day.")

    return results


def update_constituency(results, district_id, party_results, counted=None):
    """
    Helper to update a single constituency result.

    Args:
        results: the full results dict
        district_id: e.g. "BP-01", "GY-03"
        party_results: dict like {"FIDESZ-KDNP": 45.2, "TISZA": 38.1, ...}
        counted: optional counting progress percentage
    """
    entry = dict(party_results)
    if counted is not None:
        entry['counted'] = counted
    results['constituencies'][district_id] = entry


def update_county(results, county_name, party_results):
    """
    Helper to update a county's national list result.

    Args:
        results: the full results dict
        county_name: e.g. "Budapest", "Pest"
        party_results: dict like {"FIDESZ-KDNP": 45.2, "TISZA": 38.1, ...}
    """
    if county_name in results['countyList']:
        results['countyList'][county_name] = party_results


def import_csv(csv_path):
    """
    Import constituency results from a CSV file.

    Expected CSV format (tab-separated):
      Code  Választókerületek  Székhely  Fidesz-KDNP (PfE)  Tisza (EPP)  Mi Hazánk (ESN)  DK (S&D)  MKKP (→Greens/EFA)  Other  Winner

    Code format: MAZ*100 + EVK, e.g. 101 = Budapest 01, 1401 = Pest 01

    Usage:
      python3 scraper.py --csv constituencies.csv
      python3 scraper.py --csv constituencies.csv --push
    """
    results = load_current_results()

    # CSV column name → results.json party name mapping
    CSV_PARTY_MAP = {
        'Fidesz-KDNP (PfE)': 'FIDESZ-KDNP',
        'Tisza (EPP)': 'TISZA',
        'DK (S&D)': 'DK',
        'Mi Hazánk (ESN)': 'Mi Hazánk',
        'MKKP (→Greens/EFA)': 'MKKP',
    }

    # Detect delimiter (tab or comma)
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        sample = f.readline()
        delimiter = '\t' if '\t' in sample else ','

    count = 0
    county_totals = {}  # county → {party: [pct, ...]} for averaging into county list

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        for row in reader:
            code_str = row.get('Code', '').strip()
            if not code_str or not code_str.isdigit():
                continue

            code = int(code_str)
            maz = f"{code // 100:02d}"
            evk = code % 100

            if maz not in MAZ_TO_ID:
                print(f"  [WARN] Unknown MAZ code {maz} from Code={code}, skipping")
                continue

            prefix = MAZ_TO_ID[maz]
            district_id = f"{prefix}-{evk:02d}"
            county = MAZ_TO_COUNTY[maz]

            # Parse party percentages
            party_results = {}
            for csv_col, json_party in CSV_PARTY_MAP.items():
                val_str = ''
                # Try exact match first, then fuzzy
                for key in row:
                    if csv_col.lower() in key.lower():
                        val_str = row[key].strip()
                        break

                # Parse: "45.2%", "45.2", or empty
                val_str = val_str.replace('%', '').replace(',', '.').strip()
                try:
                    party_results[json_party] = float(val_str) if val_str else 0.0
                except ValueError:
                    party_results[json_party] = 0.0

            update_constituency(results, district_id, party_results)
            count += 1

            # Accumulate for county averages
            if county not in county_totals:
                county_totals[county] = {p: [] for p in PARTIES}
            for p in PARTIES:
                if party_results.get(p, 0) > 0:
                    county_totals[county][p].append(party_results[p])

    # Update county list with averages from constituency data
    for county, totals in county_totals.items():
        county_avg = {}
        for party, values in totals.items():
            county_avg[party] = round(sum(values) / len(values), 1) if values else 0
        update_county(results, county, county_avg)

    save_results(results)
    print(f"  [CSV] Imported {count} constituencies from {csv_path}")
    print(f"  [CSV] Updated {len(county_totals)} county averages")


def import_csv_county(csv_path):
    """
    Import national list results by county from a CSV file.

    Expected CSV format (tab or comma separated):
      County  Fidesz-KDNP (PfE)  Tisza (EPP)  Mi Hazánk (ESN)  DK (S&D)  MKKP (→Greens/EFA)  Other  Winner

    County names should match: Budapest, Baranya, Bács-Kiskun, Békés, etc.

    Usage:
      python3 scraper.py --csv-county county-results.csv
      python3 scraper.py --csv-county county-results.csv --push
    """
    results = load_current_results()

    CSV_PARTY_MAP = {
        'Fidesz-KDNP (PfE)': 'FIDESZ-KDNP',
        'Tisza (EPP)': 'TISZA',
        'DK (S&D)': 'DK',
        'Mi Hazánk (ESN)': 'Mi Hazánk',
        'MKKP (→Greens/EFA)': 'MKKP',
    }

    # Also allow matching by partial county name
    COUNTY_ALIASES = {}
    for county in COUNTIES:
        COUNTY_ALIASES[county.lower()] = county
        # Add short versions: "Borsod" → "Borsod-Abaúj-Zemplén"
        COUNTY_ALIASES[county.split('-')[0].lower()] = county
    # Manual aliases
    COUNTY_ALIASES['csongrád'] = 'Csongrád-Csanád'
    COUNTY_ALIASES['győr'] = 'Győr-Moson-Sopron'
    COUNTY_ALIASES['komárom'] = 'Komárom-Esztergom'
    COUNTY_ALIASES['szabolcs'] = 'Szabolcs-Szatmár-Bereg'
    COUNTY_ALIASES['jász'] = 'Jász-Nagykun-Szolnok'

    def resolve_county(name):
        name = name.strip()
        if name in results['countyList']:
            return name
        low = name.lower()
        if low in COUNTY_ALIASES:
            return COUNTY_ALIASES[low]
        # Fuzzy: check if any county starts with the input
        for county in COUNTIES:
            if county.lower().startswith(low):
                return county
        return None

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        sample = f.readline()
        delimiter = '\t' if '\t' in sample else ','

    count = 0
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=delimiter)

        # Find the county column (first column or one named County/Vármegye/Megye)
        county_col = None
        for col in reader.fieldnames:
            if col.lower() in ('county', 'vármegye', 'megye', 'county name'):
                county_col = col
                break
        if not county_col:
            county_col = reader.fieldnames[0]

        for row in reader:
            county_raw = row.get(county_col, '').strip()
            if not county_raw:
                continue

            county = resolve_county(county_raw)
            if not county:
                print(f"  [WARN] Unknown county '{county_raw}', skipping")
                continue

            party_results = {}
            for csv_col, json_party in CSV_PARTY_MAP.items():
                val_str = ''
                for key in row:
                    if csv_col.lower() in key.lower():
                        val_str = row[key].strip()
                        break

                val_str = val_str.replace('%', '').replace(',', '.').strip()
                try:
                    party_results[json_party] = float(val_str) if val_str else 0.0
                except ValueError:
                    party_results[json_party] = 0.0

            update_county(results, county, party_results)
            count += 1

    save_results(results)
    print(f"  [CSV] Imported {count} county results from {csv_path}")


def inject_test_data():
    """Inject realistic test data to verify the maps render correctly."""
    results = empty_results()

    import random
    random.seed(42)

    # Generate constituency results for all 106 districts
    for maz, prefix in MAZ_TO_ID.items():
        # Budapest gets 16, others vary
        counts = {
            '01': 16, '02': 4, '03': 6, '04': 4, '05': 7, '06': 4,
            '07': 5, '08': 5, '09': 6, '10': 3, '11': 4, '12': 3,
            '13': 2, '14': 14, '15': 4, '16': 6, '17': 3, '18': 3,
            '19': 4, '20': 3
        }
        n_evk = counts[maz]
        county = MAZ_TO_COUNTY[maz]

        # County-level base (varies by region)
        is_urban = county in ('Budapest', 'Pest')
        base_fidesz = random.uniform(25, 40) if is_urban else random.uniform(35, 55)
        base_tisza = random.uniform(30, 45) if is_urban else random.uniform(20, 38)

        for evk in range(1, n_evk + 1):
            fidesz = max(5, base_fidesz + random.gauss(0, 5))
            tisza = max(5, base_tisza + random.gauss(0, 5))
            dk = max(1, random.uniform(3, 15))
            mihazank = max(1, random.uniform(4, 14))
            mkkp = max(0.5, random.uniform(1, 6))
            total = fidesz + tisza + dk + mihazank + mkkp
            # Normalize to ~95% (other candidates get ~5%)
            factor = random.uniform(92, 98) / total

            district_id = f"{prefix}-{evk:02d}"
            update_constituency(results, district_id, {
                'FIDESZ-KDNP': round(fidesz * factor, 1),
                'TISZA': round(tisza * factor, 1),
                'DK': round(dk * factor, 1),
                'Mi Hazánk': round(mihazank * factor, 1),
                'MKKP': round(mkkp * factor, 1),
            }, counted=round(random.uniform(60, 100), 1))

    # Generate county-level national list results
    for county in COUNTIES:
        is_urban = county in ('Budapest',)
        is_suburban = county in ('Pest', 'Fejér', 'Komárom-Esztergom')

        if is_urban:
            f, t, d, m, k = 28, 42, 12, 8, 5
        elif is_suburban:
            f, t, d, m, k = 38, 35, 9, 10, 3
        else:
            f, t, d, m, k = 48, 28, 6, 11, 2

        noise = lambda v: round(max(1, v + random.gauss(0, 3)), 1)
        update_county(results, county, {
            'FIDESZ-KDNP': noise(f), 'TISZA': noise(t),
            'DK': noise(d), 'Mi Hazánk': noise(m), 'MKKP': noise(k)
        })

    save_results(results)
    print("  [TEST] Injected test data for all 106 constituencies and 20 counties")


def git_push():
    """Commit results.json and push to GitHub."""
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        ts = datetime.now().strftime('%H:%M:%S')
        subprocess.run(
            ['git', 'add', 'results.json'],
            cwd=repo_dir, check=True, capture_output=True
        )
        subprocess.run(
            ['git', 'commit', '-m', f'Update results {ts}'],
            cwd=repo_dir, check=True, capture_output=True
        )
        subprocess.run(
            ['git', 'push'],
            cwd=repo_dir, check=True, capture_output=True
        )
        print(f"  [PUSH] Committed and pushed at {ts}")
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode().strip() if e.stderr else ''
        if 'nothing to commit' in stderr:
            print("  [PUSH] No changes to push")
        else:
            print(f"  [PUSH ERROR] {stderr or e}")


def clear_results():
    """Reset results.json to empty state."""
    save_results(empty_results())
    print("  [CLEAR] Reset to empty results")


def main():
    parser = argparse.ArgumentParser(description='Hungary 2026 Election Live Scraper')
    parser.add_argument('--loop', action='store_true', help='Run continuously')
    parser.add_argument('--interval', type=int, default=60, help='Polling interval in seconds (default: 60)')
    parser.add_argument('--test', action='store_true', help='Inject test data')
    parser.add_argument('--clear', action='store_true', help='Reset to empty results')
    parser.add_argument('--csv', type=str, metavar='FILE', help='Import constituency results from CSV file')
    parser.add_argument('--csv-county', type=str, metavar='FILE', help='Import national list results by county from CSV file')
    parser.add_argument('--push', action='store_true', help='Git commit & push results.json after each update')
    args = parser.parse_args()

    print("=== Hungary 2026 Election Scraper ===")
    print(f"Results file: {RESULTS_FILE}")
    if args.push:
        print("Auto-push: ON")

    if args.csv or args.csv_county:
        if args.csv:
            import_csv(args.csv)
        if args.csv_county:
            import_csv_county(args.csv_county)
        if args.push:
            git_push()
        return

    if args.test:
        inject_test_data()
        if args.push:
            git_push()
        return

    if args.clear:
        clear_results()
        if args.push:
            git_push()
        return

    if args.loop:
        print(f"Polling every {args.interval}s. Press Ctrl+C to stop.")
        while True:
            try:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching results...")
                results = scrape_results()
                save_results(results)
                if args.push:
                    git_push()
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        results = scrape_results()
        save_results(results)
        if args.push:
            git_push()


if __name__ == '__main__':
    main()
