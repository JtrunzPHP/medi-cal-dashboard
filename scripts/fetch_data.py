#!/usr/bin/env python3
"""Process DHCS CSV files into compact JSON for the dashboard."""

import csv, json, os, re
from datetime import datetime

DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.join(DIR, "..")
E_CSV = os.path.join(REPO, "tmp", "enrollment.csv")
L_CSV = os.path.join(REPO, "tmp", "language.csv")
OUT = os.path.join(REPO, "data")

def pint(v):
    if not v: return 0
    c = re.sub(r"[\s,\"\u00a0]", "", str(v).strip())
    if c in ("", "None"): return 0
    try: return int(c)
    except: 
        try: return int(float(c))
        except: return 0

def process_enrollment():
    print("=== Enrollment ===")
    if not os.path.exists(E_CSV):
        print("  ERROR: enrollment.csv missing"); return None
    print(f"  Size: {os.path.getsize(E_CSV)/(1024*1024):.1f} MB")
    
    with open(E_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            m = h.get("Enrollment Month", "").strip()
            if not m or m < "2020-01": continue
            cv = ""
            for k in h:
                if "count" in k.lower() and "enrollee" in k.lower(): cv = h[k]; break
            rows.append({"m": m, "pt": h.get("Plan Type","").strip(),
                         "co": h.get("County","").strip(), "p": h.get("Plan Name","").strip(),
                         "c": pint(cv)})
    
    # Debug MCO mapping
    plans = set(r["p"] for r in rows)
    for kw in ["molina","health net","anthem","la care","blue cross","inland","kaiser"]:
        hits = sorted([p for p in plans if kw in p.lower()])
        if hits: print(f"  '{kw}': {hits[:6]}")
    
    # Debug county coverage
    latest_m = max(r["m"] for r in rows)
    for kw in ["molina","health net","anthem"]:
        ctys = sorted(set(r["co"] for r in rows if r["m"]==latest_m and kw in r["p"].lower()))
        print(f"  {kw} counties ({latest_m}): {ctys}")
    
    print(f"  Total: {len(rows)} records")
    return rows

def process_language():
    """Process newly-eligible language data (statewide, quarterly)."""
    print("\n=== Language (Newly Eligible) ===")
    if not os.path.exists(L_CSV):
        print("  WARNING: language.csv missing"); return None
    sz = os.path.getsize(L_CSV)
    if sz < 200:
        print(f"  WARNING: too small ({sz}b)"); return None
    print(f"  Size: {sz/(1024*1024):.2f} MB")
    
    with open(L_CSV, "r", encoding="utf-8-sig") as f:
        first = f.readline(); print(f"  Header: {first.strip()[:120]}")
        f.seek(0)
        reader = csv.DictReader(f)
        hdrs = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns: {hdrs}")
        
        # Expected: Year, Reporting Period, Primary Language, Number of Eligible Individuals
        period_col = next((h for h in hdrs if "period" in h.lower() or "quarter" in h.lower()), 
                         next((h for h in hdrs if "year" in h.lower()), hdrs[0]))
        lang_col = next((h for h in hdrs if "language" in h.lower()), hdrs[2] if len(hdrs)>2 else None)
        count_col = next((h for h in hdrs if "number" in h.lower() or "eligible" in h.lower() or "count" in h.lower()), 
                        hdrs[3] if len(hdrs)>3 else None)
        
        if not lang_col or not count_col:
            print(f"  WARNING: cant detect columns"); return None
        print(f"  Using: period={period_col}, lang={lang_col}, count={count_col}")
        
        rows = []
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            period = h.get(period_col, "").strip()
            lang = h.get(lang_col, "").strip()
            count = pint(h.get(count_col, ""))
            if not period or not lang: continue
            rows.append({"q": period, "lang": lang, "c": count})
    
    periods = sorted(set(r["q"] for r in rows))
    langs = sorted(set(r["lang"] for r in rows))
    print(f"  Periods: {periods[:5]}...{periods[-3:]}")
    print(f"  Languages: {langs[:8]}...")
    print(f"  Total: {len(rows)} records")
    return rows

def main():
    os.makedirs(OUT, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    
    enrollment = process_enrollment()
    if enrollment:
        p = os.path.join(OUT, "enrollment.json")
        with open(p, "w") as f: json.dump({"updated": now, "data": enrollment}, f)
        print(f"  Wrote enrollment.json ({os.path.getsize(p)/(1024*1024):.1f} MB)")
    else:
        print("  FAILED"); exit(1)
    
    language = process_language()
    if language:
        p = os.path.join(OUT, "language.json")
        with open(p, "w") as f: json.dump({"updated": now, "data": language}, f)
        print(f"  Wrote language.json ({os.path.getsize(p)/(1024*1024):.2f} MB)")
    
    print("\nDone!")

if __name__ == "__main__": main()
