#!/usr/bin/env python3
"""
Hungary 2026 Election Live Scraper
===================================
Fetches results from NVI VIP API (XML) and updates results.json.
Both maps auto-refresh from results.json every 30 seconds.

Usage:
  # Live polling from NVI VIP API (election day)
  python3 scraper.py --loop --push                    # poll every 60s + push
  python3 scraper.py --loop --push --interval 30      # poll every 30s + push

  # Single fetch from NVI VIP API
  python3 scraper.py                                  # single fetch
  python3 scraper.py --push                           # single fetch + push

  # Load test data from NVI rehearsal ZIP
  python3 scraper.py --load-rehearsal /tmp/foproba.zip          # load latest snapshot
  python3 scraper.py --load-rehearsal /tmp/foproba.zip --push   # load + push

  # CSV manual import (fallback for colleagues)
  python3 scraper.py --csv constituencies.csv --csv-county county.csv --push

  # Utilities
  python3 scraper.py --test                   # inject random test data
  python3 scraper.py --clear --push           # reset to empty + push
"""

import json
import time
import argparse
import csv
import os
import sys
import subprocess
import zipfile
import io
import ssl
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError
import base64

# ──────────────────────────────────────────────
#  NVI VIP API CONFIGURATION
# ──────────────────────────────────────────────

# Two redundant servers provided by NVI
NVI_SERVERS = [
    "https://195.228.10.231",
    "https://84.206.8.58",
]

# VIP credentials
NVI_USER = "europeelects_micleaa"
NVI_PASS = "cnYf7pRigT9DBFIhn0fI"

# Base path on the NVI server.
# Rehearsal (current):  /ogy2026/proba
# Election day:         to be communicated by NVI — set via --api-path flag or NVI_BASE_PATH env var
NVI_BASE_PATH = os.environ.get('NVI_BASE_PATH', '/ogy2026/proba')

# On election day, the live data structure is:
#   {NVI_BASE_PATH}/verzio.xml          — check for new version
#   {NVI_BASE_PATH}/torzs.zip           — master data (candidates, parties)
#   {NVI_BASE_PATH}/nap.zip             — daytime turnout
#   {NVI_BASE_PATH}/valtozo1.zip        — aggregated results
#   {NVI_BASE_PATH}/valtozo2.zip        — polling station results

# ──────────────────────────────────────────────
#  PARTY MAPPING CONFIGURATION
# ──────────────────────────────────────────────

# Map NVI JLCS party names → our display names
# Update this once real party registrations are published in torzs.zip
# The test data uses OP01, OP02, etc. — real data will have actual party names.
#
# Individual candidates (OEVK): each candidate has a JLCS code.
#   Multiple JLCS codes may map to the same display party because parties
#   can form different coalitions per constituency.
#   Example: "Fidesz-KDNP" alone (JLCS X) and "Fidesz-KDNP" in coalition (JLCS Y)
#   both map to display name "FIDESZ-KDNP".
#
# National lists: each list has a JLCS code and a name.
#   We match the list name to display parties.

JLCS_TO_PARTY = {
    # Will be populated from torzs.zip on first run.
    # Format: jlcs_code (str) → display party name (str)
    # Example for real election:
    #   '2320': 'FIDESZ-KDNP',
    #   '2321': 'TISZA',
    #   '2322': 'DK',
    # For test data, will be auto-mapped from JLCS names.
}

# Keyword matching: if a JLCS name contains any of these keywords,
# map it to the corresponding display party.
# This handles the real election where JLCS names contain actual party names.
PARTY_KEYWORDS = {
    'FIDESZ-KDNP': ['fidesz', 'kdnp'],
    'TISZA': ['tisza'],
    'DK': ['demokratikus koalíció', 'dk'],
    'Mi Hazánk': ['mi hazánk', 'hazánk'],
    'MKKP': ['mkkp', 'kétfarkú'],
}

# ──────────────────────────────────────────────
#  GENERAL CONFIGURATION
# ──────────────────────────────────────────────

RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.json")

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

PARTIES = ['FIDESZ-KDNP', 'TISZA', 'DK', 'Mi Hazánk', 'MKKP']
COUNTIES = list(MAZ_TO_COUNTY.values())

# Cache for master data (loaded once from torzs.zip)
_master_data = None


# ──────────────────────────────────────────────
#  NVI VIP API FUNCTIONS
# ──────────────────────────────────────────────

def nvi_fetch(path, server_index=0):
    """Fetch a resource from NVI VIP API with Basic Auth.
    Returns bytes on success, None on failure.
    Tries fallback server if primary fails."""
    # Create SSL context that accepts NVI's certificate
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    auth = base64.b64encode(f"{NVI_USER}:{NVI_PASS}".encode()).decode()

    for i in range(len(NVI_SERVERS)):
        server = NVI_SERVERS[(server_index + i) % len(NVI_SERVERS)]
        url = f"{server}{path}"
        try:
            req = Request(url, headers={
                'Authorization': f'Basic {auth}',
                'User-Agent': 'EuropeElects-HU2026/1.0',
            })
            with urlopen(req, timeout=30, context=ctx) as resp:
                return resp.read()
        except Exception as e:
            print(f"  [WARN] Failed {url}: {e}")
            continue
    return None


def nvi_fetch_version():
    """Fetch verzio.xml and return version info dict."""
    data = nvi_fetch(f"{NVI_BASE_PATH}/verzio.xml")
    if not data:
        return None
    root = ET.fromstring(data)
    ver = root.findtext('.//ver')
    return {
        'version': ver,
        'type': root.findtext('.//tjel'),       # 1=master, 2=turnout, 3=results
        'mandate_done': root.findtext('.//eng1'),
        'mandate_persons': root.findtext('.//eng2'),
        'processing_pct': root.findtext('.//feldar'),
        'mail_processing_pct': root.findtext('.//levell'),
    }


def nvi_fetch_zip(path):
    """Fetch a ZIP from NVI and return a ZipFile object."""
    data = nvi_fetch(path)
    if not data:
        return None
    return zipfile.ZipFile(io.BytesIO(data))


def load_master_data(source_zip=None):
    """Load master data (parties, candidates, lists) from torzs.zip.
    source_zip: if provided, extract torzs.zip from this rehearsal ZIP.
    Otherwise fetch from NVI API."""
    global _master_data
    if _master_data:
        return _master_data

    torzs_zip = None

    if source_zip:
        # Extract torzs.zip from rehearsal ZIP
        names = source_zip.namelist()
        torzs_path = [n for n in names if n.endswith('/torzs.zip')]
        if torzs_path:
            torzs_data = source_zip.read(torzs_path[0])
            torzs_zip = zipfile.ZipFile(io.BytesIO(torzs_data))
    else:
        # Fetch from NVI API
        torzs_zip = nvi_fetch_zip(f"{NVI_BASE_PATH}/torzs.zip")

    if not torzs_zip:
        print("  [ERROR] Could not load master data (torzs.zip)")
        return None

    _master_data = parse_master_data(torzs_zip)
    return _master_data


def parse_master_data(torzs_zip):
    """Parse the master data ZIP into lookup dictionaries."""
    master = {}

    # Parse county names
    if 'terulet.xml' in torzs_zip.namelist():
        root = ET.fromstring(torzs_zip.read('terulet.xml'))
        master['counties'] = {}
        for r in root.findall('.//teruletr'):
            master['counties'][r.findtext('maz')] = r.findtext('mnev')

    # Parse JLCS (party groups)
    if 'jlcs.xml' in torzs_zip.namelist():
        data = torzs_zip.read('jlcs.xml')
        # Handle ISO-8859-2 encoding
        text = data.decode('iso-8859-2', errors='replace')
        root = ET.fromstring(text)
        master['jlcs'] = {}
        for r in root.findall('.//jlcsr'):
            code = r.findtext('jlcs')
            name = r.findtext('nevt')
            master['jlcs'][code] = name

    # Parse individual candidates (eid → jlcs, maz, evk)
    if 'ejelolt.xml' in torzs_zip.namelist():
        data = torzs_zip.read('ejelolt.xml')
        text = data.decode('iso-8859-2', errors='replace')
        root = ET.fromstring(text)
        master['candidates'] = {}
        for r in root.findall('.//ejeloltr'):
            eid = r.findtext('eid')
            master['candidates'][eid] = {
                'jlcs': r.findtext('jlcs'),
                'maz': r.findtext('maz'),
                'evk': r.findtext('evk'),
                'name': r.findtext('nev'),
            }

    # Parse national lists (tlid → jlcs, name)
    if 'tlista.xml' in torzs_zip.namelist():
        data = torzs_zip.read('tlista.xml')
        text = data.decode('iso-8859-2', errors='replace')
        root = ET.fromstring(text)
        master['lists'] = {}
        for r in root.findall('.//tlistar'):
            tlid = r.findtext('tlid')
            master['lists'][tlid] = {
                'jlcs': r.findtext('jlcs'),
                'name': r.findtext('tnev'),
                'type': r.findtext('ltip'),  # O=standalone, K=joint, N=nationality
            }

    # Build JLCS → display party mapping
    master['jlcs_to_party'] = build_jlcs_party_map(
        master.get('jlcs', {}),
        master.get('lists', {})
    )

    # Build list tlid → display party mapping
    master['tlid_to_party'] = {}
    for tlid, info in master.get('lists', {}).items():
        jlcs = info['jlcs']
        if jlcs in master['jlcs_to_party']:
            master['tlid_to_party'][tlid] = master['jlcs_to_party'][jlcs]
        else:
            # Try matching list name directly
            party = match_party_name(info['name'])
            if party:
                master['tlid_to_party'][tlid] = party

    print(f"  [MASTER] Loaded: {len(master.get('candidates', {}))} candidates, "
          f"{len(master.get('lists', {}))} lists, {len(master.get('jlcs', {}))} party groups")
    print(f"  [MASTER] JLCS→party mapping ({len(master['jlcs_to_party'])} entries):")
    for code, party in sorted(master['jlcs_to_party'].items()):
        jlcs_name = master.get('jlcs', {}).get(code, '?')
        print(f"    {code} ({jlcs_name}) → {party}")

    return master


def build_jlcs_party_map(jlcs_dict, lists_dict):
    """Build JLCS code → display party name mapping.
    Strategy:
      1. Keyword matching on JLCS names (real election data)
      2. Fallback for rehearsal: extract OP-codes from names and map by position
    """
    mapping = {}

    # Strategy 1: Keyword matching (real party names like "Fidesz-KDNP")
    for code, name in jlcs_dict.items():
        if code == '0':  # Független (independent)
            continue
        party = match_party_name(name)
        if party:
            mapping[code] = party

    # If keyword matching found all parties, we're done
    mapped_parties = set(mapping.values())
    if mapped_parties >= set(PARTIES):
        return mapping

    # Strategy 2: Rehearsal fallback — JLCS names are like "OP01", "OP01-OP02"
    # Check if this looks like rehearsal data
    sample_names = [name for name in jlcs_dict.values() if name and name != 'Független']
    is_rehearsal = sample_names and any('OP' in n.upper() for n in sample_names)

    if is_rehearsal and not mapped_parties:
        print("  [MASTER] Rehearsal data detected (OP-codes). Mapping via list order.")
        # For rehearsal: use the order of party lists (tlista.xml) to assign display parties.
        # Get standalone+joint lists (not nationality) sorted by tlid
        party_lists = sorted(
            [(tlid, info) for tlid, info in lists_dict.items()
             if info.get('type') in ('O', 'K')],
            key=lambda x: int(x[0])
        )

        # Map: each list's JLCS code → next display party
        # Then also map all "sub-JLCS" containing the same OP-codes
        import re
        op_to_party = {}
        party_idx = 0
        for tlid, info in party_lists:
            if party_idx >= len(PARTIES):
                break
            name = info.get('name', '')
            ops = re.findall(r'OP\d+', name.upper())
            for op in ops:
                if op not in op_to_party:
                    op_to_party[op] = PARTIES[party_idx]
            if ops:
                party_idx += 1

        # Now map every JLCS code based on which OP-codes it contains
        for code, name in jlcs_dict.items():
            if code == '0' or not name:
                continue
            ops = re.findall(r'OP\d+', name.upper())
            if not ops:
                continue
            # Use the first OP code's party as the mapping
            for op in ops:
                if op in op_to_party:
                    mapping[code] = op_to_party[op]
                    break

    return mapping


def match_party_name(name):
    """Try to match a JLCS/list name to a display party via keywords."""
    if not name:
        return None
    name_lower = name.lower()
    for party, keywords in PARTY_KEYWORDS.items():
        for kw in keywords:
            if kw in name_lower:
                return party
    return None


def parse_turnout_xml(nap_zip):
    """Parse napkozi.xml from nap.zip into turnout dict.
    Returns the latest snapshot per (level, location)."""
    if 'napkozi.xml' not in nap_zip.namelist():
        return None

    data = nap_zip.read('napkozi.xml')
    try:
        root = ET.fromstring(data.decode('iso-8859-2', errors='replace'))
    except ET.ParseError:
        root = ET.fromstring(data)

    turnout = {
        'national': None,
        'counties': {},
        'constituencies': {},
        'lastReportTime': None,
    }

    # napszint codes:
    #   5 = national, 4 = county, 1 = OEVK, 2 = municipality, 3 = polling station
    # We want the latest non-zero snapshot for each entity.
    # The 18:30 snapshot is the most complete.

    # Group entries by (level, key) and keep the latest by jisorsz
    latest = {}  # (napszint, location_key) → entry

    for r in root.findall('.//napkozir'):
        napszint = r.findtext('napszint')
        if napszint not in ('1', '4', '5'):
            continue

        ido = r.findtext('ido') or ''
        jisorsz = int(r.findtext('jisorsz') or 0)
        maz = r.findtext('maz')
        evk = r.findtext('evk')
        szaz = r.findtext('szaz')
        megj = r.findtext('megj')
        vp = r.findtext('vp')

        if napszint == '5':
            key = ('5', 'national')
        elif napszint == '4':
            key = ('4', maz)
        elif napszint == '1':
            key = ('1', f"{maz}-{evk}")

        prev = latest.get(key)
        if prev is None or jisorsz > prev['jisorsz']:
            latest[key] = {
                'jisorsz': jisorsz,
                'ido': ido,
                'pct': float(szaz) if szaz else 0.0,
                'megj': int(megj) if megj else 0,
                'vp': int(vp) if vp else 0,
            }

    # Track latest reported snapshot time
    max_jisorsz = max((e['jisorsz'] for e in latest.values()), default=0)
    for e in latest.values():
        if e['jisorsz'] == max_jisorsz:
            turnout['lastReportTime'] = e['ido']
            break

    # Map into final structure
    for (level, loc_key), entry in latest.items():
        record = {
            'pct': entry['pct'],
            'megj': entry['megj'],
            'vp': entry['vp'],
            'time': entry['ido'],
        }
        if level == '5':
            turnout['national'] = record
        elif level == '4':
            county = MAZ_TO_COUNTY.get(loc_key)
            if county:
                turnout['counties'][county] = record
        elif level == '1':
            maz, evk = loc_key.split('-')
            if maz in MAZ_TO_ID and evk:
                prefix = MAZ_TO_ID[maz]
                district_id = f"{prefix}-{int(evk):02d}"
                turnout['constituencies'][district_id] = record

    return turnout


def fetch_and_parse_turnout(source_zip=None):
    """Fetch nap.zip from NVI (or rehearsal source) and return parsed turnout dict."""
    nap_zip = None

    if source_zip:
        # Find the latest nap.zip in the rehearsal ZIP
        nap_paths = sorted([n for n in source_zip.namelist() if n.endswith('/nap.zip')])
        if nap_paths:
            nap_data = source_zip.read(nap_paths[-1])
            nap_zip = zipfile.ZipFile(io.BytesIO(nap_data))
            print(f"  Using latest turnout snapshot: {nap_paths[-1]}")
    else:
        nap_zip = nvi_fetch_zip(f"{NVI_BASE_PATH}/nap.zip")

    if not nap_zip:
        return None

    turnout = parse_turnout_xml(nap_zip)
    if turnout:
        n_const = len(turnout['constituencies'])
        n_county = len(turnout['counties'])
        nat_pct = turnout['national']['pct'] if turnout['national'] else 0
        print(f"  [TURNOUT] {n_const} OEVKs, {n_county} counties, "
              f"national {nat_pct}% (last report: {turnout['lastReportTime']})")
    return turnout


def parse_results_xml(valtozo1_zip, master):
    """Parse szeredmf.xml + szeredmt.xml from a valtozo1.zip into results.json format."""
    results = empty_results()

    szeredmf_data = valtozo1_zip.read('szeredmf.xml')
    szeredmt_data = valtozo1_zip.read('szeredmt.xml')

    # Handle encoding
    try:
        tree_f = ET.fromstring(szeredmf_data.decode('iso-8859-2', errors='replace'))
    except ET.ParseError:
        tree_f = ET.fromstring(szeredmf_data)

    try:
        tree_t = ET.fromstring(szeredmt_data.decode('iso-8859-2', errors='replace'))
    except ET.ParseError:
        tree_t = ET.fromstring(szeredmt_data)

    # Index szeredmf.xml (header data)
    sfid_data = {}
    for r in tree_f.findall('.//szeredmfr'):
        sfid = r.findtext('sfid')
        sfid_data[sfid] = {
            'oszint': r.findtext('oszint'),
            'valtip': r.findtext('valtip'),
            'maz': r.findtext('sfmaz'),
            'evk': r.findtext('sfevk'),
            'valid_votes': int(r.findtext('n') or 0),
            'feldar': float(r.findtext('feldar') or 0),
            'winner_eid': r.findtext('eid'),
        }

    # Index szeredmt.xml (vote details) by sfid
    sfid_votes = {}
    for r in tree_t.findall('.//szeredmtr'):
        sfid = r.findtext('sfid')
        if sfid not in sfid_votes:
            sfid_votes[sfid] = []
        sfid_votes[sfid].append({
            'jlid': r.findtext('jlid'),
            'szav': int(r.findtext('szav') or 0),
        })

    jlcs_to_party = master.get('jlcs_to_party', {})
    candidates = master.get('candidates', {})
    lists_data = master.get('lists', {})
    tlid_to_party = master.get('tlid_to_party', {})

    # ── Process OEVK individual results (oszint=1, valtip=J) ──
    for sfid, info in sfid_data.items():
        if info['oszint'] != '1':
            continue

        maz = info['maz']
        evk = info['evk']
        if not maz or maz not in MAZ_TO_ID:
            continue

        prefix = MAZ_TO_ID[maz]
        evk_num = int(evk) if evk else 0
        district_id = f"{prefix}-{evk_num:02d}"
        total_valid = info['valid_votes']

        party_results = {p: 0.0 for p in PARTIES}
        other_pct = 0.0

        for vote in sfid_votes.get(sfid, []):
            eid = vote['jlid']
            szav = vote['szav']
            pct = round(szav / total_valid * 100, 1) if total_valid else 0

            # Look up candidate → JLCS → display party
            cand = candidates.get(eid, {})
            jlcs = cand.get('jlcs', '')

            if jlcs == '0':  # Independent
                other_pct += pct
            elif jlcs in jlcs_to_party:
                party = jlcs_to_party[jlcs]
                party_results[party] += pct
            else:
                other_pct += pct

        # Store results with counting progress
        entry = dict(party_results)
        entry['counted'] = info['feldar']
        results['constituencies'][district_id] = entry

    # ── Process county list results (oszint=4, valtip=L) ──
    for sfid, info in sfid_data.items():
        if info['oszint'] != '4':
            continue

        maz = info['maz']
        if not maz or maz not in MAZ_TO_COUNTY:
            continue

        county = MAZ_TO_COUNTY[maz]
        total_valid = info['valid_votes']

        party_results = {p: 0.0 for p in PARTIES}

        for vote in sfid_votes.get(sfid, []):
            tlid = vote['jlid']
            szav = vote['szav']
            pct = round(szav / total_valid * 100, 1) if total_valid else 0

            if tlid in tlid_to_party:
                party = tlid_to_party[tlid]
                party_results[party] += pct
            # Nationality lists (HORVÁT, NÉMET, ROMA) → skip (go to "Other")

        results['countyList'][county] = party_results

    # Get version info from verzio.xml inside the ZIP
    if 'verzio.xml' in valtozo1_zip.namelist():
        ver_data = valtozo1_zip.read('verzio.xml')
        ver_root = ET.fromstring(ver_data)
        ver_str = ver_root.findtext('.//ver')
        feldar = ver_root.findtext('.//feldar')
        if ver_str:
            results['nviVersion'] = ver_str
        if feldar:
            results['processingPct'] = float(feldar)

    n_const = len(results['constituencies'])
    n_county = sum(1 for c in results['countyList'].values() if any(v > 0 for v in c.values()))
    print(f"  [RESULTS] {n_const} constituencies, {n_county} counties with data")

    return results


# ──────────────────────────────────────────────
#  MAIN SCRAPING FUNCTIONS
# ──────────────────────────────────────────────

def scrape_results():
    """Fetch latest results + turnout from NVI VIP API."""
    # Step 1: Load master data (cached after first call)
    master = load_master_data()
    if not master:
        print("  [ERROR] Cannot scrape without master data. Run with torzs.zip first.")
        return load_current_results()

    # Step 2: Fetch turnout data (nap.zip)
    print("  Fetching nap.zip...")
    turnout = fetch_and_parse_turnout()

    # Step 3: Fetch valtozo1.zip (aggregated results)
    print("  Fetching valtozo1.zip...")
    v1_zip = nvi_fetch_zip(f"{NVI_BASE_PATH}/valtozo1.zip")

    if v1_zip:
        results = parse_results_xml(v1_zip, master)
    else:
        print("  [WARN] No results data yet (polls may still be open)")
        results = empty_results()

    if turnout:
        results['turnout'] = turnout

    return results


def scrape_loop(interval, push):
    """Continuous polling loop checking verzio.xml for new data."""
    print(f"  Polling every {interval}s. Press Ctrl+C to stop.")

    # Load master data once
    master = load_master_data()
    if not master:
        print("  [ERROR] Cannot start loop without master data.")
        print("  Run first: python3 scraper.py  (to fetch torzs.zip)")
        return

    last_version = None

    while True:
        try:
            ts = datetime.now().strftime('%H:%M:%S')

            # Check version
            ver_info = nvi_fetch_version()
            if ver_info:
                current_version = ver_info['version']
                processing = ver_info['processing_pct']

                if current_version == last_version:
                    print(f"  [{ts}] No new data (v={current_version}, {processing}% processed)")
                    time.sleep(interval)
                    continue

                print(f"  [{ts}] New version: {current_version} ({processing}% processed)")
                last_version = current_version
            else:
                print(f"  [{ts}] Could not fetch version, trying results anyway...")

            # Fetch turnout (always — works during the day)
            turnout = fetch_and_parse_turnout()

            # Fetch results (works after polls close)
            v1_zip = nvi_fetch_zip(f"{NVI_BASE_PATH}/valtozo1.zip")
            if v1_zip:
                results = parse_results_xml(v1_zip, master)
            elif turnout:
                # Pre-results phase: only turnout available
                results = load_current_results()
                # Reset constituencies/county results to empty (don't show stale data)
                if 'constituencies' not in results:
                    results['constituencies'] = {}
                if 'countyList' not in results:
                    results['countyList'] = empty_results()['countyList']
            else:
                print(f"  [{ts}] No data available")
                time.sleep(interval)
                continue

            if turnout:
                results['turnout'] = turnout

            save_results(results)
            if push:
                git_push()

            time.sleep(interval)

        except KeyboardInterrupt:
            print("\nStopped.")
            break


def load_rehearsal(zip_path):
    """Load the latest result snapshot from an NVI rehearsal ZIP file."""
    print(f"  Loading rehearsal data from {zip_path}")

    with zipfile.ZipFile(zip_path) as main_zip:
        # Load master data from torzs.zip inside the rehearsal ZIP
        master = load_master_data(source_zip=main_zip)
        if not master:
            print("  [ERROR] Could not load master data from rehearsal ZIP")
            return None

        # Load turnout data
        turnout = fetch_and_parse_turnout(source_zip=main_zip)

        # Find the latest valtozo1.zip (sorted by timestamp directory name)
        v1_paths = sorted([n for n in main_zip.namelist() if n.endswith('/valtozo1.zip')])
        if not v1_paths:
            print("  [ERROR] No valtozo1.zip found in rehearsal ZIP")
            if turnout:
                # Return turnout-only snapshot
                results = empty_results()
                results['turnout'] = turnout
                return results
            return None

        latest = v1_paths[-1]
        print(f"  Using latest snapshot: {latest}")

        # Extract and parse
        v1_data = main_zip.read(latest)
        v1_zip = zipfile.ZipFile(io.BytesIO(v1_data))
        results = parse_results_xml(v1_zip, master)

        # Attach turnout data
        if turnout:
            results['turnout'] = turnout

        return results


# ──────────────────────────────────────────────
#  HELPER FUNCTIONS (unchanged from before)
# ──────────────────────────────────────────────

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
        "countyList": county_list,
        "turnout": {
            "national": None,             # {pct, megj, vp, time}
            "counties": {},               # county_name → {pct, megj, vp, time}
            "constituencies": {},         # district_id → {pct, megj, vp, time}
            "lastReportTime": None,       # latest reported snapshot time (e.g. "13:00")
        }
    }


def save_results(results):
    """Write results to results.json."""
    results['lastUpdated'] = datetime.now().isoformat()
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Saved results.json at {results['lastUpdated']}")


def update_constituency(results, district_id, party_results, counted=None):
    """Helper to update a single constituency result."""
    entry = dict(party_results)
    if counted is not None:
        entry['counted'] = counted
    results['constituencies'][district_id] = entry


def update_county(results, county_name, party_results):
    """Helper to update a county's national list result."""
    if county_name in results['countyList']:
        results['countyList'][county_name] = party_results


def import_csv(csv_path):
    """Import constituency results from a CSV file (manual fallback)."""
    results = load_current_results()

    CSV_PARTY_MAP = {
        'Fidesz-KDNP (PfE)': 'FIDESZ-KDNP',
        'Tisza (EPP)': 'TISZA',
        'DK (S&D)': 'DK',
        'Mi Hazánk (ESN)': 'Mi Hazánk',
        'MKKP (→Greens/EFA)': 'MKKP',
    }

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        sample = f.readline()
        delimiter = '\t' if '\t' in sample else ','

    count = 0
    county_totals = {}

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

            update_constituency(results, district_id, party_results)
            count += 1

            if county not in county_totals:
                county_totals[county] = {p: [] for p in PARTIES}
            for p in PARTIES:
                if party_results.get(p, 0) > 0:
                    county_totals[county][p].append(party_results[p])

    for county, totals in county_totals.items():
        county_avg = {}
        for party, values in totals.items():
            county_avg[party] = round(sum(values) / len(values), 1) if values else 0
        update_county(results, county, county_avg)

    save_results(results)
    print(f"  [CSV] Imported {count} constituencies from {csv_path}")
    print(f"  [CSV] Updated {len(county_totals)} county averages")


def import_csv_county(csv_path):
    """Import national list results by county from a CSV file (manual fallback)."""
    results = load_current_results()

    CSV_PARTY_MAP = {
        'Fidesz-KDNP (PfE)': 'FIDESZ-KDNP',
        'Tisza (EPP)': 'TISZA',
        'DK (S&D)': 'DK',
        'Mi Hazánk (ESN)': 'Mi Hazánk',
        'MKKP (→Greens/EFA)': 'MKKP',
    }

    COUNTY_ALIASES = {}
    for county in COUNTIES:
        COUNTY_ALIASES[county.lower()] = county
        COUNTY_ALIASES[county.split('-')[0].lower()] = county
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

    for maz, prefix in MAZ_TO_ID.items():
        counts = {
            '01': 16, '02': 4, '03': 6, '04': 4, '05': 7, '06': 4,
            '07': 5, '08': 5, '09': 6, '10': 3, '11': 4, '12': 3,
            '13': 2, '14': 14, '15': 4, '16': 6, '17': 3, '18': 3,
            '19': 4, '20': 3
        }
        n_evk = counts[maz]
        county = MAZ_TO_COUNTY[maz]
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
            factor = random.uniform(92, 98) / total

            district_id = f"{prefix}-{evk:02d}"
            update_constituency(results, district_id, {
                'FIDESZ-KDNP': round(fidesz * factor, 1),
                'TISZA': round(tisza * factor, 1),
                'DK': round(dk * factor, 1),
                'Mi Hazánk': round(mihazank * factor, 1),
                'MKKP': round(mkkp * factor, 1),
            }, counted=round(random.uniform(60, 100), 1))

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


# ──────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Hungary 2026 Election Live Scraper')
    parser.add_argument('--loop', action='store_true', help='Run continuously, polling NVI VIP API')
    parser.add_argument('--interval', type=int, default=60, help='Polling interval in seconds (default: 60)')
    parser.add_argument('--test', action='store_true', help='Inject random test data')
    parser.add_argument('--clear', action='store_true', help='Reset to empty results')
    parser.add_argument('--csv', type=str, metavar='FILE', help='Import constituency results from CSV')
    parser.add_argument('--csv-county', type=str, metavar='FILE', help='Import county results from CSV')
    parser.add_argument('--push', action='store_true', help='Git commit & push after update')
    parser.add_argument('--load-rehearsal', type=str, metavar='FILE',
                        help='Load latest snapshot from NVI rehearsal ZIP (foproba.zip)')
    parser.add_argument('--load-master', action='store_true',
                        help='Fetch and cache master data (torzs.zip) from NVI')
    parser.add_argument('--api-path', type=str, metavar='PATH',
                        help='Override NVI base path (default: /ogy2026/proba)')
    args = parser.parse_args()

    if args.api_path:
        global NVI_BASE_PATH
        NVI_BASE_PATH = args.api_path
        print(f"  API path: {NVI_BASE_PATH}")

    print("=== Hungary 2026 Election Scraper ===")
    print(f"Results file: {RESULTS_FILE}")
    if args.push:
        print("Auto-push: ON")

    # Manual CSV import
    if args.csv or args.csv_county:
        if args.csv:
            import_csv(args.csv)
        if args.csv_county:
            import_csv_county(args.csv_county)
        if args.push:
            git_push()
        return

    # Test data
    if args.test:
        inject_test_data()
        if args.push:
            git_push()
        return

    # Clear
    if args.clear:
        clear_results()
        if args.push:
            git_push()
        return

    # Load rehearsal data
    if args.load_rehearsal:
        results = load_rehearsal(args.load_rehearsal)
        if results:
            save_results(results)
            if args.push:
                git_push()
        return

    # Fetch master data only
    if args.load_master:
        master = load_master_data()
        if master:
            print("  [OK] Master data loaded and cached")
        return

    # Live polling loop
    if args.loop:
        scrape_loop(args.interval, args.push)
        return

    # Single fetch
    results = scrape_results()
    save_results(results)
    if args.push:
        git_push()


if __name__ == '__main__':
    main()
