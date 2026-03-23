#!/usr/bin/env python3
"""
Fetch and process DHCS Medi-Cal data into JSON for the dashboard.
Downloads three datasets:
  1. Managed Care Enrollment (monthly, by county/plan)
  2. Primary Language of Newly Eligible (quarterly, statewide)
  3. Threshold Languages by County (quarterly, county-level)
"""

import csv
import json
import os
import re
import sys
from datetime import datetime
from urllib.request import urlopen, Request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.join(SCRIPT_DIR, "..")
TMP_DIR = os.path.join(REPO_DIR, "tmp")
OUT_DIR = os.path.join(REPO_DIR, "data")

# === DHCS Data URLs ===
# These use the CKAN datastore_search API which is more reliable than direct CSV download
# Enrollment: ~300K rows, we fetch 2020+ to keep size manageable
ENROLLMENT_URL = (
    "https://data.chhs.ca.gov/dataset/c6ccef54-e7a9-4ebd-b79a-850b72c4dd8c"
    "/resource/95358a7a-2c9d-41c6-a0e0-405a7e5c5f18/download/"
    "open-data-portal-managed-care-enrollment-count-february-2026.csv"
)

# Language of newly eligible (statewide, quarterly)
LANGUAGE_URL = (
    "https://data.chhs.ca.gov/dataset/bb70b475-8698-45b8-9667-91bb7228fe78"
    "/resource/706bf0a7-9bb4-4674-9b58-917daac10d25/download/"
    "3.9-medi-cal-language-q2-2025-odp.csv"
)

# Threshold languages by county (quarterly) - CKAN API approach
LANG_COUNTY_DATASET = "quarterly-certified-eligible-counts-by-month-of-eligibility-county-and-threshold-language"
CKAN_API = "https://data.chhs.ca.gov/api/3/action/package_show?id="

USER_AGENT = "Mozilla/5.0 (Medi-Cal Dashboard Bot; +https://github.com)"
HEADERS = {"User-Agent": USER_AGENT}


def fetch_url(url, label=""):
    """Fetch URL content with retries."""
    print(f"  Downloading: {label or url[:80]}...")
    for attempt in range(3):
        try:
            req = Request(url, headers=HEADERS)
            with urlopen(req, timeout=300) as resp:
                data = resp.read()
                print(f"  Downloaded {len(data) / (1024*1024):.1f} MB")
                return data.decode("utf-8-sig")
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt == 2:
                raise
    return None


def parse_int(val):
    """Parse integer from messy CSV value."""
    if not val:
        return 0
    cleaned = re.sub(r"[\s,\"\u00a0]", "", str(val).strip())
    if cleaned in ("", "None", "N/A", "*"):
        return 0
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return 0


def process_enrollment():
    """Download and process managed care enrollment data."""
    print("\n=== Enrollment Data ===")
    try:
        text = fetch_url(ENROLLMENT_URL, "Managed Care Enrollment CSV")
    except Exception as e:
        print(f"  FAILED to download: {e}")
        # Try alternate URL pattern (month changes)
        for month_name in ["january-2026", "december-2025", "november-2025", "march-2026"]:
            alt_url = ENROLLMENT_URL.rsplit("/", 1)[0] + f"/open-data-portal-managed-care-enrollment-count-{month_name}.csv"
            try:
                text = fetch_url(alt_url, f"Trying {month_name}")
                break
            except:
                continue
        else:
            print("  ALL download attempts failed")
            return None

    reader = csv.DictReader(text.splitlines())
    headers = [h.strip() for h in (reader.fieldnames or [])]
    print(f"  Headers: {headers}")

    # Find the count column (varies: "Count of Enrollees" or similar)
    count_col = None
    for h in headers:
        if "count" in h.lower() and "enrollee" in h.lower():
            count_col = h
            break
    if not count_col:
        # Fallback: last numeric-looking column
        count_col = headers[-1]
    print(f"  Count column: '{count_col}'")

    rows = []
    for r in reader:
        h = {k.strip(): v for k, v in r.items()}
        month = h.get("Enrollment Month", "").strip()
        if not month or month < "2020-01":
            continue
        rows.append({
            "m": month,
            "pt": h.get("Plan Type", "").strip(),
            "co": h.get("County", "").strip(),
            "p": h.get("Plan Name", "").strip(),
            "c": parse_int(h.get(count_col, "")),
        })

    print(f"  Parsed {len(rows)} records (2020-01 onward)")

    # Debug: show plan names for key MCOs
    plans = set(r["p"] for r in rows)
    for kw in ["molina", "health net", "anthem", "la care", "kaiser", "blue shield", "caloptima"]:
        hits = sorted([p for p in plans if kw in p.lower()])
        if hits:
            print(f"  '{kw}' plans: {hits}")

    # Debug: latest month county coverage for key MCOs
    if rows:
        latest_m = max(r["m"] for r in rows)
        print(f"  Latest month: {latest_m}")
        for kw in ["molina", "health net", "anthem"]:
            ctys = sorted(set(r["co"] for r in rows if r["m"] == latest_m and kw in r["p"].lower()))
            print(f"  {kw} counties ({latest_m}): {len(ctys)} — {ctys[:8]}{'...' if len(ctys) > 8 else ''}")

    return rows


def process_language():
    """Download and process primary language of newly eligible (statewide)."""
    print("\n=== Language Data (Newly Eligible, Statewide) ===")
    try:
        text = fetch_url(LANGUAGE_URL, "Primary Language CSV")
    except Exception as e:
        print(f"  FAILED: {e}")
        # Try alternate quarter names
        base = LANGUAGE_URL.rsplit("/", 1)[0]
        for qname in ["q4-2025", "q3-2025", "q1-2026", "q1-2025"]:
            alt = f"{base}/3.9-medi-cal-language-{qname}-odp.csv"
            try:
                text = fetch_url(alt, f"Trying {qname}")
                break
            except:
                continue
        else:
            print("  All language download attempts failed")
            return None

    reader = csv.DictReader(text.splitlines())
    headers = [h.strip() for h in (reader.fieldnames or [])]
    print(f"  Headers: {headers}")

    # Detect columns
    period_col = next((h for h in headers if "period" in h.lower() or "quarter" in h.lower()), headers[0])
    lang_col = next((h for h in headers if "language" in h.lower()), headers[1] if len(headers) > 1 else None)
    count_col = next(
        (h for h in headers if "number" in h.lower() or "eligible" in h.lower() or "count" in h.lower()),
        headers[-1]
    )

    if not lang_col:
        print("  WARNING: Cannot detect language column")
        return None

    print(f"  Using: period='{period_col}', lang='{lang_col}', count='{count_col}'")

    rows = []
    for r in reader:
        h = {k.strip(): v for k, v in r.items()}
        period = h.get(period_col, "").strip()
        lang = h.get(lang_col, "").strip()
        count = parse_int(h.get(count_col, ""))
        if not period or not lang:
            continue
        rows.append({"q": period, "lang": lang, "c": count})

    periods = sorted(set(r["q"] for r in rows))
    langs = sorted(set(r["lang"] for r in rows))
    print(f"  Periods: {periods}")
    print(f"  Languages: {langs}")
    print(f"  Total: {len(rows)} records")
    return rows


def find_resource_url(dataset_name):
    """Use CKAN API to find the CSV download URL for a dataset."""
    try:
        url = CKAN_API + dataset_name
        text = fetch_url(url, f"CKAN API lookup: {dataset_name}")
        import json as jmod
        data = jmod.loads(text)
        resources = data.get("result", {}).get("resources", [])
        for res in resources:
            if res.get("format", "").upper() == "CSV":
                return res.get("url")
    except Exception as e:
        print(f"  CKAN lookup failed: {e}")
    return None


def process_language_county():
    """Download and process threshold languages by county."""
    print("\n=== Language Data (County-Level Threshold Languages) ===")

    # Try CKAN API first to get current URL
    csv_url = find_resource_url(LANG_COUNTY_DATASET)
    if not csv_url:
        print("  Could not find CSV URL via CKAN API, skipping")
        return None

    try:
        text = fetch_url(csv_url, "Threshold Language by County CSV")
    except Exception as e:
        print(f"  FAILED: {e}")
        return None

    reader = csv.DictReader(text.splitlines())
    headers = [h.strip() for h in (reader.fieldnames or [])]
    print(f"  Headers: {headers}")

    # Detect columns: Month of Eligibility, County, Language, Certified Eligible Count
    moe_col = next((h for h in headers if "month" in h.lower() or "eligibility" in h.lower()), headers[0])
    county_col = next((h for h in headers if "county" in h.lower()), None)
    lang_col = next((h for h in headers if "language" in h.lower()), None)
    count_col = next(
        (h for h in headers if "certified" in h.lower() or "count" in h.lower() or "eligible" in h.lower()),
        None
    )

    if not all([county_col, lang_col, count_col]):
        print(f"  WARNING: Cannot detect all columns. Found: county={county_col}, lang={lang_col}, count={count_col}")
        return None

    print(f"  Using: month='{moe_col}', county='{county_col}', lang='{lang_col}', count='{count_col}'")

    rows = []
    for r in reader:
        h = {k.strip(): v for k, v in r.items()}
        month = h.get(moe_col, "").strip()
        if not month or month < "2020-01":
            continue
        rows.append({
            "m": month,
            "co": h.get(county_col, "").strip(),
            "lang": h.get(lang_col, "").strip(),
            "c": parse_int(h.get(count_col, "")),
        })

    if rows:
        counties = set(r["co"] for r in rows)
        langs = set(r["lang"] for r in rows)
        months = sorted(set(r["m"] for r in rows))
        print(f"  Counties: {len(counties)}, Languages: {len(langs)}")
        print(f"  Date range: {months[0]} to {months[-1]}")
        print(f"  Sample languages: {sorted(langs)[:10]}")
        print(f"  Total: {len(rows)} records")
    else:
        print("  No records parsed")

    return rows


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    errors = []

    # 1. Enrollment (required)
    enrollment = process_enrollment()
    if enrollment:
        path = os.path.join(OUT_DIR, "enrollment.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": enrollment}, f)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"\n  ✓ Wrote enrollment.json ({size_mb:.1f} MB, {len(enrollment)} records)")
    else:
        errors.append("enrollment")
        print("\n  ✗ FAILED: No enrollment data processed")

    # 2. Language - statewide newly eligible (optional)
    language = process_language()
    if language:
        path = os.path.join(OUT_DIR, "language.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": language}, f)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"  ✓ Wrote language.json ({size_mb:.2f} MB, {len(language)} records)")
    else:
        print("  ⚠ Skipped language.json")

    # 3. Language - county threshold (optional)
    lang_county = process_language_county()
    if lang_county:
        path = os.path.join(OUT_DIR, "language_county.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": lang_county}, f)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"  ✓ Wrote language_county.json ({size_mb:.1f} MB, {len(lang_county)} records)")
    else:
        print("  ⚠ Skipped language_county.json")

    print(f"\n{'='*50}")
    if errors:
        print(f"ERRORS: {errors}")
        sys.exit(1)
    else:
        print("Done! All critical data processed successfully.")


if __name__ == "__main__":
    main()
