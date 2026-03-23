#!/usr/bin/env python3
"""
Process DHCS Medi-Cal CSV files (downloaded by curl in GitHub Action) into
compact JSON for the dashboard.

Input files in tmp/ (downloaded by curl):
  - enrollment.csv          (Managed Care Enrollment Report)
  - language.csv            (Primary Language of Newly Eligible — statewide quarterly)
  - language_county.csv     (Threshold Languages by County — quarterly)
  - eligibles.csv           (Certified Eligibles by County — for penetration rates)

Output files in data/:
  - enrollment.json
  - language.json
  - language_county.json
  - eligibles.json
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
EL_CSV = os.path.join(TMP_DIR, "eligibles.csv")


def parse_int(val):
    """Parse integer from messy CSV value like ' 293,311 ' or '12345.0' or '*'."""
    if not val:
        return 0
    cleaned = re.sub(r"[\s,\"\u00a0]", "", str(val).strip())
    if cleaned in ("", "None", "N/A", "*", "-", "null", "0.0"):
        return 0
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return 0


def check_file(path, label):
    """Check if a CSV file exists and has content."""
    if not os.path.exists(path):
        print(f"  WARNING: {label}: file not found at {path}")
        return False
    size = os.path.getsize(path)
    if size < 100:
        print(f"  WARNING: {label}: file too small ({size} bytes)")
        with open(path, "r", errors="replace") as f:
            print(f"     Contents: {f.read(500)}")
        return False
    print(f"  OK: {label}: {size / (1024*1024):.2f} MB")
    return True


def process_enrollment():
    """Process the managed care enrollment CSV."""
    print("\n=== Enrollment Data ===")
    if not check_file(E_CSV, "enrollment.csv"):
        return None

    with open(E_CSV, "r", encoding="utf-8-sig") as f:
        first_line = f.readline().strip()
        print(f"  Header: {first_line[:200]}")
        f.seek(0)

        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns ({len(headers)}): {headers}")

        # Find count column
        count_col = None
        for h in headers:
            if "count" in h.lower() and "enrollee" in h.lower():
                count_col = h
                break
        if not count_col:
            count_col = headers[-1] if headers else None
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
                "c": parse_int(h.get(count_col, "") if count_col else "0"),
            })

    print(f"  Parsed {len(rows)} records (2020-01+)")
    if rows:
        latest_m = max(r["m"] for r in rows)
        plans = sorted(set(r["p"] for r in rows))
        print(f"  Latest month: {latest_m}")
        print(f"  Unique plans: {len(plans)}")
        for kw in ["molina", "health net", "anthem", "kaiser", "calviva", "california health", "wellcare", "community health plan", "blue shield"]:
            hits = [p for p in plans if kw in p.lower()]
            if hits:
                print(f"    '{kw}' -> {hits}")
    return rows


def process_language():
    """Process primary language of newly eligible (statewide quarterly)."""
    print("\n=== Language Data (Statewide) ===")
    if not check_file(L_CSV, "language.csv"):
        return None

    with open(L_CSV, "r", encoding="utf-8-sig") as f:
        first_line = f.readline().strip()
        print(f"  Header: {first_line[:200]}")
        f.seek(0)

        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns ({len(headers)}): {headers}")

        # Detect columns
        period_col = None
        lang_col = None
        count_col = None
        for h in headers:
            hl = h.lower()
            if not period_col and ("period" in hl or "quarter" in hl or "reporting" in hl):
                period_col = h
            if not lang_col and "language" in hl:
                lang_col = h
            if not count_col and ("number" in hl or "eligible" in hl or "count" in hl or "individual" in hl):
                count_col = h

        if not period_col:
            period_col = headers[0] if headers else None
        if not count_col:
            count_col = headers[-1] if headers else None

        if not lang_col:
            print("  WARNING: Cannot detect language column, skipping")
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
        print(f"  Periods: {sorted(set(r['q'] for r in rows))}")
        print(f"  Languages: {sorted(set(r['lang'] for r in rows))}")
        print(f"  Total: {len(rows)} records")
        print(f"  Sample values: {[r['c'] for r in rows[:5]]}")
    return rows if rows else None


def process_language_county():
    """Process threshold languages by county (quarterly).
    
    CRITICAL FIX: Previous version was outputting 0 for all counts.
    This version prints extensive debug info and tries multiple column
    detection strategies.
    """
    print("\n=== Language Data (County Threshold) ===")
    if not check_file(LC_CSV, "language_county.csv"):
        return None

    with open(LC_CSV, "r", encoding="utf-8-sig") as f:
        first_line = f.readline().strip()
        print(f"  Header: {first_line[:300]}")
        f.seek(0)

        reader = csv.DictReader(f)
        raw_headers = reader.fieldnames or []
        headers = [h.strip() for h in raw_headers]
        print(f"  Raw columns ({len(raw_headers)}): {raw_headers}")
        print(f"  Stripped columns ({len(headers)}): {headers}")

        # Read first 3 rows for debugging
        f.seek(0)
        reader2 = csv.DictReader(f)
        sample_rows = []
        for i, r in enumerate(reader2):
            if i >= 3:
                break
            sample_rows.append({k.strip(): v for k, v in r.items()})
        print(f"  Sample row 1: {sample_rows[0] if sample_rows else 'NONE'}")
        if len(sample_rows) > 1:
            print(f"  Sample row 2: {sample_rows[1]}")

        # Detect columns with multiple strategies
        moe_col = None
        county_col = None
        lang_col = None
        count_col = None

        for h in headers:
            hl = h.lower()
            # Month column
            if not moe_col and ("month" in hl and "eligib" in hl):
                moe_col = h
            elif not moe_col and ("month_of_elig" in hl):
                moe_col = h
            elif not moe_col and hl in ("month of eligibility", "month_of_eligibility", "moe"):
                moe_col = h
            # County column
            if not county_col and "county" in hl:
                county_col = h
            # Language column
            if not lang_col and "language" in hl:
                lang_col = h
            # Count column - try specific first, then general
            if "certified" in hl and ("eligible" in hl or "count" in hl):
                count_col = h
            elif not count_col and ("eligible" in hl and "count" in hl):
                count_col = h

        # Fallback: if count_col still not found, try any column with "count" or "number"
        if not count_col:
            for h in headers:
                hl = h.lower()
                if "count" in hl or "number" in hl or "total" in hl:
                    count_col = h
                    break

        # Fallback: if still not found, try the last column (often the numeric one)
        if not count_col and len(headers) >= 4:
            count_col = headers[-1]

        # Fallback for month column
        if not moe_col:
            for h in headers:
                if "month" in h.lower() or "date" in h.lower() or "period" in h.lower():
                    moe_col = h
                    break
            if not moe_col and headers:
                moe_col = headers[0]

        print(f"  Detected: month='{moe_col}', county='{county_col}', lang='{lang_col}', count='{count_col}'")

        if not all([county_col, lang_col]):
            print(f"  ERROR: Missing required columns")
            return None

        # Test parsing on sample rows
        if sample_rows:
            for i, sr in enumerate(sample_rows):
                raw_count = sr.get(count_col, "MISSING")
                parsed = parse_int(raw_count)
                print(f"  Test row {i}: count_col='{count_col}' -> raw='{raw_count}' -> parsed={parsed}")
                # If the detected count column gives 0, try ALL columns for numeric values
                if parsed == 0:
                    print(f"    Checking all columns for numeric values:")
                    for col_name, col_val in sr.items():
                        pv = parse_int(col_val)
                        if pv > 0:
                            print(f"      '{col_name}' = '{col_val}' -> {pv} *** FOUND NON-ZERO ***")
                            count_col = col_name
                            print(f"    Switching count_col to '{count_col}'")
                            break

        print(f"  Final count column: '{count_col}'")

        # Re-read and process all rows
        f.seek(0)
        reader3 = csv.DictReader(f)
        rows = []
        nonzero_count = 0
        for r in reader3:
            h = {k.strip(): v for k, v in r.items()}
            month = h.get(moe_col, "").strip() if moe_col else ""
            if not month or month < "2020-01":
                continue
            county = h.get(county_col, "").strip() if county_col else ""
            lang = h.get(lang_col, "").strip() if lang_col else ""
            count = parse_int(h.get(count_col, "") if count_col else "0")
            if not county or not lang:
                continue
            if count > 0:
                nonzero_count += 1
            county_total = parse_int(h.get("County Total", "") if "County Total" in h else "0")
            rows.append({
                "m": month,
                "co": county,
                "lang": lang,
                "c": count,
                "ct": county_total,
            })

    print(f"  Total rows: {len(rows)}, Non-zero counts: {nonzero_count}")
    if rows:
        counties = sorted(set(r["co"] for r in rows))
        langs = sorted(set(r["lang"] for r in rows))
        months = sorted(set(r["m"] for r in rows))
        print(f"  Date range: {months[0]} to {months[-1]}")
        print(f"  Counties: {len(counties)} — {counties[:5]}...")
        print(f"  Languages: {langs}")
    if nonzero_count == 0:
        print("  WARNING: All counts are zero! The count column detection may have failed.")
        print("  Returning data anyway (zeros) — check CSV structure manually.")
    return rows if rows else None


def process_eligibles():
    """Process certified eligibles by county (for penetration rates).
    
    The CSV has rows broken out by Age Group and Gender, so we need to
    aggregate (sum) by Month + County to get county totals.
    The count column is 'Total Eligibles' (not 'County').
    """
    print("\n=== Certified Eligibles Data ===")
    if not check_file(EL_CSV, "eligibles.csv"):
        return None

    with open(EL_CSV, "r", encoding="utf-8-sig") as f:
        first_line = f.readline().strip()
        print(f"  Header: {first_line[:300]}")
        f.seek(0)

        reader = csv.DictReader(f)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns ({len(headers)}): {headers}")

        # Explicit column detection — prioritize 'Total Eligibles' over 'County'
        moe_col = None
        county_col = None
        count_col = None

        for h in headers:
            hl = h.lower()
            if not moe_col and "month" in hl and "eligib" in hl:
                moe_col = h
            if not county_col and hl == "county":
                county_col = h
            if hl == "total eligibles":
                count_col = h

        if not moe_col:
            moe_col = headers[0] if headers else None
        if not county_col:
            for h in headers:
                if "county" in h.lower():
                    county_col = h
                    break

        # Fallback: if Total Eligibles not found, scan sample rows
        if not count_col:
            f.seek(0)
            sr = csv.DictReader(f)
            first_row = next(sr, None)
            if first_row:
                h = {k.strip(): v for k, v in first_row.items()}
                for col_name, col_val in h.items():
                    if col_name.lower() != "county" and parse_int(col_val) > 0:
                        count_col = col_name
                        print(f"  Fallback: found numeric column '{count_col}'")
                        break

        print(f"  Using: month='{moe_col}', county='{county_col}', count='{count_col}'")

        if not county_col or not count_col:
            print("  ERROR: Missing required columns")
            return None

        # Read sample for debugging
        f.seek(0)
        reader2 = csv.DictReader(f)
        for i, r in enumerate(reader2):
            if i >= 2:
                break
            h = {k.strip(): v for k, v in r.items()}
            print(f"  Sample: month='{h.get(moe_col,'')}', county='{h.get(county_col,'')}', count='{h.get(count_col,'')}' -> {parse_int(h.get(count_col,''))}")

        # Process: read all rows, then aggregate by month+county
        f.seek(0)
        reader3 = csv.DictReader(f)
        agg = {}  # key = "month||county" -> sum of counts
        raw_count = 0
        for r in reader3:
            h = {k.strip(): v for k, v in r.items()}
            month = h.get(moe_col, "").strip() if moe_col else ""
            if not month or month < "2023-01":
                continue
            county = h.get(county_col, "").strip() if county_col else ""
            count = parse_int(h.get(count_col, "") if count_col else "0")
            if not county:
                continue
            raw_count += 1
            key = f"{month}||{county}"
            agg[key] = agg.get(key, 0) + count

        rows = []
        for key, total in agg.items():
            parts = key.split("||")
            rows.append({"m": parts[0], "co": parts[1], "c": total})

    if rows:
        months = sorted(set(r["m"] for r in rows))
        counties = sorted(set(r["co"] for r in rows))
        nonzero = sum(1 for r in rows if r["c"] > 0)
        print(f"  Raw rows read: {raw_count}")
        print(f"  Aggregated to: {len(rows)} month-county pairs")
        print(f"  Date range: {months[0]} to {months[-1]}")
        print(f"  Counties: {len(counties)}")
        print(f"  Non-zero counts: {nonzero}")
        # Show sample totals
        sample = sorted(rows, key=lambda r: -r["c"])[:5]
        for s in sample:
            print(f"    {s['m']} | {s['co']} | {s['c']:,}")
    return rows if rows else None


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
        print(f"\n  OK Wrote enrollment.json ({os.path.getsize(path)/(1024*1024):.1f} MB, {len(enrollment)} records)")
    else:
        errors.append("enrollment")
        print("\n  FAILED: No enrollment data")

    # 2. Language statewide (optional)
    language = process_language()
    if language:
        path = os.path.join(OUT_DIR, "language.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": language}, f)
        print(f"  OK Wrote language.json ({os.path.getsize(path)/(1024*1024):.2f} MB, {len(language)} records)")
    else:
        print("  SKIP language.json")

    # 3. Language county (optional)
    lang_county = process_language_county()
    if lang_county:
        path = os.path.join(OUT_DIR, "language_county.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": lang_county}, f)
        nonzero = sum(1 for r in lang_county if r["c"] > 0)
        print(f"  OK Wrote language_county.json ({os.path.getsize(path)/(1024*1024):.2f} MB, {len(lang_county)} records, {nonzero} non-zero)")
    else:
        print("  SKIP language_county.json")

    # 4. Certified eligibles (optional)
    eligibles = process_eligibles()
    if eligibles:
        path = os.path.join(OUT_DIR, "eligibles.json")
        with open(path, "w") as f:
            json.dump({"updated": now, "data": eligibles}, f)
        print(f"  OK Wrote eligibles.json ({os.path.getsize(path)/(1024*1024):.2f} MB, {len(eligibles)} records)")
    else:
        print("  SKIP eligibles.json")

    print(f"\n{'='*50}")
    if errors:
        print(f"CRITICAL ERRORS: {errors}")
        sys.exit(1)
    else:
        print("Done!")


if __name__ == "__main__":
    main()
