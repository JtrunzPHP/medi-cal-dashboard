#!/usr/bin/env python3
"""
Process DHCS Medi-Cal CSV files (downloaded by curl in GitHub Action) into
compact JSON for the dashboard.

Expected input files in tmp/:
  - enrollment.csv    (Managed Care Enrollment Report)
  - language.csv      (Primary Language of Newly Eligible — statewide quarterly)
  - language_county.csv (Threshold Languages by County — quarterly)

Output files in data/:
  - enrollment.json
  - language.json
  - language_county.json
"""

import csv
import json
import os
import re
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.join(SCRIPT_DIR, "..")
TMP_DIR = os.path.join(REPO_DIR, "tmp")
OUT_DIR = os.path.join(REPO_DIR, "data")

E_CSV = os.path.join(TMP_DIR, "enrollment.csv")
L_CSV = os.path.join(TMP_DIR, "language.csv")
LC_CSV = os.path.join(TMP_DIR, "language_county.csv")


def parse_int(val):
    """Parse integer from messy CSV value like ' 293,311 ' or '12345.0'."""
    if not val:
        return 0
    cleaned = re.sub(r"[\s,\"\u00a0]", "", str(val).strip())
    if cleaned in ("", "None", "N/A", "*", "-"):
        return 0
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return 0


def check_file(path, label):
    """Check if a CSV file exists and has content. Returns True if valid."""
    if not os.path.exists(path):
        print(f"  ⚠ {label}: file not found at {path}")
        return False
    size = os.path.getsize(path)
    if size < 100:
        print(f"  ⚠ {label}: file too small ({size} bytes), likely an error page")
        # Print contents for debugging
        with open(path, "r", errors="replace") as f:
            print(f"     Contents: {f.read(500)}")
        return False
    size_mb = size / (1024 * 1024)
    print(f"  ✓ {label}: {size_mb:.2f} MB")
    return True


def detect_column(headers, *keywords):
    """Find the first header containing any of the keywords (case-insensitive)."""
    for h in headers:
        hl = h.lower()
        for kw in keywords:
            if kw in hl:
                return h
    return None


def process_enrollment():
    """Process the managed care enrollment CSV."""
    print("\n=== Enrollment Data ===")
    if not check_file(E_CSV, "enrollment.csv"):
        return None

    with open(E_CSV, "r", encoding="utf-8-sig") as f:
        # Read first line to show header
        first_line = f.readline().strip()
        print(f"  Header: {first_line[:150]}...")
        f.seek(0)

        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns ({len(headers)}): {headers}")

        # Detect count column
        count_col = detect_column(headers, "count of enrollee", "enrollee count", "count_of")
        if not count_col:
            # Fallback: last column that looks numeric
            count_col = headers[-1] if headers else None
        print(f"  Count column: '{count_col}'")

        rows = []
        skipped = 0
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            month = h.get("Enrollment Month", "").strip()
            if not month:
                skipped += 1
                continue
            if month < "2020-01":
                continue
            rows.append({
                "m": month,
                "pt": h.get("Plan Type", "").strip(),
                "co": h.get("County", "").strip(),
                "p": h.get("Plan Name", "").strip(),
                "c": parse_int(h.get(count_col, "") if count_col else "0"),
            })

    print(f"  Parsed {len(rows)} records (2020-01+), skipped {skipped} bad rows")

    if not rows:
        print("  ERROR: Zero rows parsed!")
        return None

    # Debug diagnostics
    plans = sorted(set(r["p"] for r in rows))
    latest_m = max(r["m"] for r in rows)
    counties = sorted(set(r["co"] for r in rows if r["m"] == latest_m))
    print(f"  Latest month: {latest_m}")
    print(f"  Total unique plans: {len(plans)}")
    print(f"  Counties in latest month: {len(counties)}")

    for kw in ["molina", "health net", "anthem", "la care", "kaiser", "inland", "caloptima"]:
        hits = sorted([p for p in plans if kw in p.lower()])
        if hits:
            print(f"    '{kw}' → {hits}")

    for kw in ["molina", "health net", "anthem"]:
        ctys = sorted(set(
            r["co"] for r in rows
            if r["m"] == latest_m and kw in r["p"].lower()
        ))
        print(f"    {kw} counties ({latest_m}): {len(ctys)} — {ctys[:10]}{'...' if len(ctys) > 10 else ''}")

    return rows


def process_language():
    """Process primary language of newly eligible individuals (statewide quarterly)."""
    print("\n=== Language Data (Statewide Newly Eligible) ===")
    if not check_file(L_CSV, "language.csv"):
        return None

    with open(L_CSV, "r", encoding="utf-8-sig") as f:
        first_line = f.readline().strip()
        print(f"  Header: {first_line[:150]}...")
        f.seek(0)

        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns ({len(headers)}): {headers}")

        # Detect columns
        period_col = detect_column(headers, "period", "quarter", "reporting")
        if not period_col:
            period_col = headers[0] if headers else None
        lang_col = detect_column(headers, "language")
        count_col = detect_column(headers, "number", "eligible", "count", "individual")
        if not count_col:
            count_col = headers[-1] if headers else None

        if not lang_col:
            print("  ⚠ Cannot detect language column, skipping")
            return None

        print(f"  Using: period='{period_col}', lang='{lang_col}', count='{count_col}'")

        rows = []
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            period = h.get(period_col, "").strip() if period_col else ""
            lang = h.get(lang_col, "").strip() if lang_col else ""
            count = parse_int(h.get(count_col, "") if count_col else "0")
            if not period or not lang:
                continue
            rows.append({"q": period, "lang": lang, "c": count})

    if rows:
        periods = sorted(set(r["q"] for r in rows))
        langs = sorted(set(r["lang"] for r in rows))
        print(f"  Periods: {periods}")
        print(f"  Languages ({len(langs)}): {langs[:12]}{'...' if len(langs) > 12 else ''}")
        print(f"  Total: {len(rows)} records")
    else:
        print("  ⚠ No rows parsed")

    return rows if rows else None


def process_language_county():
    """Process threshold languages by county (quarterly)."""
    print("\n=== Language Data (County Threshold Languages) ===")
    if not check_file(LC_CSV, "language_county.csv"):
        return None

    with open(LC_CSV, "r", encoding="utf-8-sig") as f:
        first_line = f.readline().strip()
        print(f"  Header: {first_line[:200]}...")
        f.seek(0)

        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns ({len(headers)}): {headers}")

        # Detect columns
        moe_col = detect_column(headers, "month of eligibility", "month_of_eligibility", "eligibility_month")
        if not moe_col:
            moe_col = detect_column(headers, "month", "date", "period")
        if not moe_col:
            moe_col = headers[0] if headers else None

        county_col = detect_column(headers, "county")
        lang_col = detect_column(headers, "language")
        count_col = detect_column(headers, "certified eligible", "eligible count", "count")
        if not count_col:
            # Try to find the last numeric-looking column
            count_col = headers[-1] if headers else None

        if not all([county_col, lang_col]):
            print(f"  ⚠ Cannot detect required columns. county={county_col}, lang={lang_col}")
            return None

        print(f"  Using: month='{moe_col}', county='{county_col}', lang='{lang_col}', count='{count_col}'")

        rows = []
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            month = h.get(moe_col, "").strip() if moe_col else ""
            if not month or month < "2020-01":
                continue
            county = h.get(county_col, "").strip() if county_col else ""
            lang = h.get(lang_col, "").strip() if lang_col else ""
            count = parse_int(h.get(count_col, "") if count_col else "0")
            if not county or not lang:
                continue
            rows.append({
                "m": month,
                "co": county,
                "lang": lang,
                "c": count,
            })

    if rows:
        counties = sorted(set(r["co"] for r in rows))
        langs = sorted(set(r["lang"] for r in rows))
        months = sorted(set(r["m"] for r in rows))
        print(f"  Date range: {months[0]} to {months[-1]}")
        print(f"  Counties: {len(counties)} — {counties[:8]}...")
        print(f"  Languages ({len(langs)}): {langs[:10]}{'...' if len(langs) > 10 else ''}")
        print(f"  Total: {len(rows)} records")
    else:
        print("  ⚠ No rows parsed")

    return rows if rows else None


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    errors = []

    # 1. Enrollment (required — script fails if this is missing)
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

    # 2. Language — statewide newly eligible (optional)
    language = process_language()
    if language:
        path = os.path.join(OUT_DIR, "language.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": language}, f)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"  ✓ Wrote language.json ({size_mb:.2f} MB, {len(language)} records)")
    else:
        print("  ⚠ Skipped language.json (data unavailable or unparseable)")

    # 3. Language — county threshold (optional)
    lang_county = process_language_county()
    if lang_county:
        path = os.path.join(OUT_DIR, "language_county.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": lang_county}, f)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        print(f"  ✓ Wrote language_county.json ({size_mb:.1f} MB, {len(lang_county)} records)")
    else:
        print("  ⚠ Skipped language_county.json (data unavailable or unparseable)")

    # Summary
    print(f"\n{'='*50}")
    if errors:
        print(f"CRITICAL ERRORS: {errors}")
        sys.exit(1)
    else:
        print("Done! All critical data processed successfully.")


if __name__ == "__main__":
    main()
