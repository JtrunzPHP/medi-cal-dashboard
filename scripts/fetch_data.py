#!/usr/bin/env python3
"""Fetch DHCS Medi-Cal data and convert to JSON for the dashboard."""

import csv
import json
import io
import os
import re
from datetime import datetime
from urllib.request import urlopen, Request

ENROLLMENT_URL = (
    "https://data.chhs.ca.gov/dataset/c6ccef54-e7a9-4ebd-b79a-850b72c4dd8c"
    "/resource/95358a7a-2c9d-41c6-a0e0-405a7e5c5f18/download/"
    "open-data-portal-managed-care-enrollment-count-february-2026.csv"
)

LANGUAGE_URL = (
    "https://data.chhs.ca.gov/dataset/8c897320-ba87-4574-bc37-bae974191c35"
    "/resource/b698c8d7-aacd-43c6-a7dd-737ff5692284/download/"
    "t5_eligibility_by_primary_language_201001_202602.csv"
)

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")


def parse_int(val):
    """Parse integer from potentially messy CSV value like ' 293,311 '."""
    if not val or val.strip() in ("", "None"):
        return 0
    cleaned = re.sub(r"[\s,\"]", "", val.strip())
    try:
        return int(cleaned)
    except ValueError:
        return 0


def fetch_csv(url):
    """Download CSV content from URL."""
    print(f"  Downloading: {url[:80]}...")
    req = Request(url, headers={"User-Agent": "MediCalDashboard/1.0"})
    with urlopen(req, timeout=180) as resp:
        raw = resp.read().decode("utf-8-sig")
    return raw


def process_enrollment():
    """Fetch and process the managed care enrollment CSV."""
    print("=== Enrollment Data ===")
    raw = fetch_csv(ENROLLMENT_URL)
    reader = csv.DictReader(io.StringIO(raw))

    rows = []
    for r in reader:
        h = {k.strip(): v for k, v in r.items()}
        month = h.get("Enrollment Month", "").strip()
        if not month or month < "2023-01":
            continue

        count_val = ""
        for key in h:
            if "count" in key.lower() and "enrollee" in key.lower():
                count_val = h[key]
                break

        rows.append({
            "m": month,
            "pt": h.get("Plan Type", "").strip(),
            "co": h.get("County", "").strip(),
            "p": h.get("Plan Name", "").strip(),
            "c": parse_int(count_val),
        })

    print(f"  Parsed {len(rows)} records (2023-01 onward)")
    return rows


def process_language():
    """Fetch and process the primary language CSV."""
    print("\n=== Language Data ===")
    try:
        raw = fetch_csv(LANGUAGE_URL)
    except Exception as e:
        print(f"  WARNING: Could not fetch language data: {e}")
        return None

    reader = csv.DictReader(io.StringIO(raw))
    headers = [h.strip() for h in (reader.fieldnames or [])]
    print(f"  Headers detected: {headers[:6]}...")

    moe_col = next((h for h in headers if "month" in h.lower() or "eligibility" in h.lower()), headers[0])
    county_col = next((h for h in headers if "county" in h.lower()), headers[1] if len(headers) > 1 else None)
    lang_col = next((h for h in headers if "language" in h.lower()), headers[2] if len(headers) > 2 else None)
    count_col = next(
        (h for h in headers if "certified" in h.lower() or "count" in h.lower() or "eligible" in h.lower()),
        headers[3] if len(headers) > 3 else None,
    )

    if not all([county_col, lang_col, count_col]):
        print(f"  WARNING: Could not detect all columns. Found: moe={moe_col}, county={county_col}, lang={lang_col}, count={count_col}")
        return None

    print(f"  Using columns: month={moe_col}, county={county_col}, language={lang_col}, count={count_col}")

    rows = []
    for r in reader:
        h = {k.strip(): v for k, v in r.items()}
        month = h.get(moe_col, "").strip()
        if not month or month < "2023-01":
            continue
        rows.append({
            "m": month,
            "co": h.get(county_col, "").strip(),
            "lang": h.get(lang_col, "").strip(),
            "c": parse_int(h.get(count_col, "")),
        })

    print(f"  Parsed {len(rows)} records (2023-01 onward)")
    return rows


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    enrollment = process_enrollment()
    enrollment_path = os.path.join(OUT_DIR, "enrollment.json")
    with open(enrollment_path, "w") as f:
        json.dump({"updated": now, "data": enrollment}, f)
    size_mb = os.path.getsize(enrollment_path) / (1024 * 1024)
    print(f"  Wrote {enrollment_path} ({size_mb:.1f} MB)")

    language = process_language()
    if language:
        language_path = os.path.join(OUT_DIR, "language.json")
        with open(language_path, "w") as f:
            json.dump({"updated": now, "data": language}, f)
        size_mb = os.path.getsize(language_path) / (1024 * 1024)
        print(f"  Wrote {language_path} ({size_mb:.1f} MB)")
    else:
        print("  Skipped language.json (data unavailable)")

    print("\nDone!")


if __name__ == "__main__":
    main()
