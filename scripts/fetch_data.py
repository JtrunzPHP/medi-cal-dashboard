#!/usr/bin/env python3
"""Process downloaded DHCS CSV files into JSON for the dashboard."""

import csv
import json
import io
import os
import re
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.join(SCRIPT_DIR, "..")
ENROLLMENT_CSV = os.path.join(REPO_DIR, "tmp", "enrollment.csv")
LANGUAGE_CSV = os.path.join(REPO_DIR, "tmp", "language.csv")
OUT_DIR = os.path.join(REPO_DIR, "data")


def parse_int(val):
    if not val or val.strip() in ("", "None"):
        return 0
    cleaned = re.sub(r"[\s,\"\u00a0]", "", val.strip())
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return 0


def process_enrollment():
    print("=== Processing Enrollment CSV ===")
    if not os.path.exists(ENROLLMENT_CSV):
        print("  ERROR: enrollment.csv not found")
        return None

    size_mb = os.path.getsize(ENROLLMENT_CSV) / (1024 * 1024)
    print(f"  CSV file size: {size_mb:.1f} MB")

    with open(ENROLLMENT_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Headers: {headers}")

        rows = []
        skipped = 0
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            month = h.get("Enrollment Month", "").strip()
            if not month or month < "2020-01":
                skipped += 1
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

    print(f"  Parsed {len(rows)} records, skipped {skipped} (pre-2020)")

    # Debug: show unique plan names containing key MCO names
    plans = set(r["p"] for r in rows)
    for keyword in ["molina", "health net", "anthem", "la care", "l.a. care"]:
        matches = [p for p in plans if keyword in p.lower()]
        print(f"  Plans matching '{keyword}': {matches[:5]}")

    return rows


def process_language():
    print("\n=== Processing Language CSV ===")
    if not os.path.exists(LANGUAGE_CSV):
        print("  WARNING: language.csv not found")
        return None

    size = os.path.getsize(LANGUAGE_CSV)
    if size < 100:
        print(f"  WARNING: language.csv too small ({size} bytes), likely empty/error")
        return None

    print(f"  CSV file size: {size / (1024 * 1024):.1f} MB")

    with open(LANGUAGE_CSV, "r", encoding="utf-8-sig") as f:
        first_line = f.readline()
        print(f"  First line: {first_line[:200]}")
        f.seek(0)

        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Headers: {headers[:8]}")

        moe_col = next((h for h in headers if "month" in h.lower() or "eligibility" in h.lower()), headers[0])
        county_col = next((h for h in headers if "county" in h.lower()), headers[1] if len(headers) > 1 else None)
        lang_col = next((h for h in headers if "language" in h.lower()), headers[2] if len(headers) > 2 else None)
        count_col = next(
            (h for h in headers if "certified" in h.lower() or "count" in h.lower() or "eligible" in h.lower()),
            headers[3] if len(headers) > 3 else None,
        )

        if not all([county_col, lang_col, count_col]):
            print(f"  WARNING: Could not detect columns")
            return None

        print(f"  Columns: month={moe_col}, county={county_col}, lang={lang_col}, count={count_col}")

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

    print(f"  Parsed {len(rows)} records")
    if rows:
        counties = set(r["co"] for r in rows)
        langs = set(r["lang"] for r in rows)
        print(f"  Counties: {len(counties)}, Languages: {len(langs)}")
        print(f"  Sample languages: {list(langs)[:10]}")

    return rows


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    enrollment = process_enrollment()
    if enrollment:
        path = os.path.join(OUT_DIR, "enrollment.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": enrollment}, f)
        print(f"  Wrote enrollment.json ({os.path.getsize(path) / (1024*1024):.1f} MB)")
    else:
        print("  FAILED: No enrollment data")
        exit(1)

    language = process_language()
    if language:
        path = os.path.join(OUT_DIR, "language.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": language}, f)
        print(f"  Wrote language.json ({os.path.getsize(path) / (1024*1024):.1f} MB)")
    else:
        print("  Skipped language.json")

    print("\nDone!")


if __name__ == "__main__":
    main()
