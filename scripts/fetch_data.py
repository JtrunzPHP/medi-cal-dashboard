#!/usr/bin/env python3
"""Process DHCS CSV files into JSON with accurate MCO-to-ticker mapping.

Sources for plan ownership:
- CHCF "Medi-Cal Explained: 2024 Managed Care Plans by County" (June 2023)
- DHCS Medi-Cal Managed Care Health Plan Directory
- DHCS MCP County Table 2023-2024

Key complexities:
- Centene (CNC) owns Health Net AND California Health & Wellness (rebranded
  Health Net Community Solutions in 2024). CalViva Health also subcontracts 
  to Health Net. Community Health Plan of Imperial Valley also subs to HN.
- Molina (MOH) is a plan partner (subcontractor) under Health Net in LA County.
  Their LA enrollment may show under Health Net, not Molina.
- Elevance (ELV) operates as Anthem Blue Cross Partnership Plan. They exited 
  many counties in 2024.
- Kaiser (KP) expanded massively in 2024 but has special enrollment criteria.
- Blue Shield Promise is nonprofit, not a public equity play.
- L.A. Care, IEHP, CalOptima, etc. are public/local plans (not publicly traded).
"""

import csv, json, os, re
from datetime import datetime

DIR = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.join(DIR, "..")
E_CSV = os.path.join(REPO, "tmp", "enrollment.csv")
L_CSV = os.path.join(REPO, "tmp", "language.csv")
OUT = os.path.join(REPO, "data")

# ============================================================
# PLAN NAME -> PARENT COMPANY / TICKER MAPPING
# This is the critical mapping. Plan names come from DHCS data.
# We match using lowercase substring matching.
# Order matters: more specific matches should come first.
# ============================================================

PLAN_TO_TICKER = [
    # --- CENTENE (CNC) --- owns Health Net + CA Health & Wellness + CalViva subs
    # California Health & Wellness -> Centene (rebranded Health Net Community Solutions 2024)
    ("california health & wellness", "CNC", "Centene"),
    ("california health and wellness", "CNC", "Centene"),
    ("ca health & wellness", "CNC", "Centene"),
    ("health net community solutions", "CNC", "Centene"),
    # CalViva Health -> subcontracts to Health Net (Centene) in Fresno/Kings/Madera
    ("calviva", "CNC", "Centene"),
    # Community Health Plan of Imperial Valley -> subs to Health Net (Centene)
    ("community health plan of imperial", "CNC", "Centene"),
    # Health Net direct
    ("health net", "CNC", "Centene"),
    # Wellcare by Health Net (Medi-Medi)
    ("wellcare", "CNC", "Centene"),

    # --- MOLINA HEALTHCARE (MOH) ---
    # Note: In LA County, Molina is a plan partner under Health Net. 
    # If DHCS data shows "Molina" separately, we capture it.
    ("molina", "MOH", "Molina"),

    # --- ELEVANCE HEALTH (ELV) --- operates as Anthem Blue Cross Partnership Plan
    # Official contract name: Blue Cross of California Partnership Plan
    ("anthem blue cross", "ELV", "Elevance"),
    ("anthem", "ELV", "Elevance"),
    ("blue cross of california partnership", "ELV", "Elevance"),

    # --- KAISER PERMANENTE (KP) --- private/nonprofit but massive presence
    ("kaiser", "KP", "Kaiser"),

    # --- BLUE SHIELD OF CALIFORNIA PROMISE --- nonprofit
    ("blue shield", "BSC", "Blue Shield (nonprofit)"),

    # --- AETNA / CVS HEALTH (CVS) --- exited CA Medi-Cal in 2024
    ("aetna", "CVS", "CVS/Aetna"),

    # --- UNITEDHEALTH (UNH) --- limited CA Medi-Cal presence
    ("united", "UNH", "UnitedHealth"),

    # --- LOCAL / PUBLIC PLANS (not publicly traded) ---
    ("l.a. care", "LOCAL", "L.A. Care (public)"),
    ("la care", "LOCAL", "L.A. Care (public)"),
    ("inland emp", "LOCAL", "IEHP (public)"),
    ("caloptima", "LOCAL", "CalOptima (public)"),
    ("partnership health", "LOCAL", "Partnership HP (public)"),
    ("san francisco health", "LOCAL", "SFHP (public)"),
    ("santa clara family", "LOCAL", "SCFHP (public)"),
    ("contra costa health", "LOCAL", "CCHP (public)"),
    ("kern health", "LOCAL", "Kern HS (public)"),
    ("alameda alliance", "LOCAL", "Alameda Alliance (public)"),
    ("health plan of san joaquin", "LOCAL", "HPSJ (public)"),
    ("health plan of san mateo", "LOCAL", "HPSM (public)"),
    ("central california alliance", "LOCAL", "CCAH (public)"),
    ("gold coast", "LOCAL", "Gold Coast (public)"),
    ("cencal", "LOCAL", "CenCal (public)"),
    ("community health group", "LOCAL", "CHG (public)"),
]

def match_ticker(plan_name):
    """Match a DHCS plan name to ticker and parent company."""
    pn = plan_name.lower().strip()
    for pattern, ticker, parent in PLAN_TO_TICKER:
        if pattern in pn:
            return ticker, parent
    return "OTHER", "Other/Unknown"

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
        unmapped = {}
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            m = h.get("Enrollment Month", "").strip()
            if not m or m < "2020-01": continue
            cv = ""
            for k in h:
                if "count" in k.lower() and "enrollee" in k.lower(): cv = h[k]; break
            
            plan_name = h.get("Plan Name", "").strip()
            ticker, parent = match_ticker(plan_name)
            
            row = {
                "m": m,
                "pt": h.get("Plan Type", "").strip(),
                "co": h.get("County", "").strip(),
                "p": plan_name,
                "c": pint(cv),
                "tk": ticker,     # Ticker symbol
                "pa": parent,     # Parent company name
            }
            rows.append(row)
            
            if ticker == "OTHER":
                unmapped[plan_name] = unmapped.get(plan_name, 0) + 1

    # Debug output
    print(f"  Total records: {len(rows)}")
    
    # Summarize by ticker
    ticker_totals = {}
    latest_m = max(r["m"] for r in rows)
    for r in rows:
        if r["m"] == latest_m:
            tk = r["tk"]
            ticker_totals[tk] = ticker_totals.get(tk, 0) + r["c"]
    
    print(f"\n  === Ticker Summary ({latest_m}) ===")
    for tk in sorted(ticker_totals.keys(), key=lambda x: -ticker_totals[x]):
        print(f"    {tk}: {ticker_totals[tk]:>12,}")
    
    # Show unmapped plans
    if unmapped:
        print(f"\n  === Unmapped Plans (OTHER) ===")
        for pn in sorted(unmapped.keys(), key=lambda x: -unmapped[x])[:15]:
            print(f"    {pn}: {unmapped[pn]} records")
    
    # Show CNC breakdown
    print(f"\n  === CNC (Centene) Detail ({latest_m}) ===")
    cnc_plans = {}
    for r in rows:
        if r["m"] == latest_m and r["tk"] == "CNC":
            cnc_plans[r["p"]] = cnc_plans.get(r["p"], 0) + r["c"]
    for pn in sorted(cnc_plans.keys(), key=lambda x: -cnc_plans[x]):
        print(f"    {pn}: {cnc_plans[pn]:>10,}")
    
    # Show ELV breakdown
    print(f"\n  === ELV (Elevance/Anthem) Detail ({latest_m}) ===")
    elv_plans = {}
    for r in rows:
        if r["m"] == latest_m and r["tk"] == "ELV":
            elv_plans[r["p"]] = elv_plans.get(r["p"], 0) + r["c"]
    for pn in sorted(elv_plans.keys(), key=lambda x: -elv_plans[x]):
        print(f"    {pn}: {elv_plans[pn]:>10,}")

    # Show MOH breakdown  
    print(f"\n  === MOH (Molina) Detail ({latest_m}) ===")
    moh_plans = {}
    for r in rows:
        if r["m"] == latest_m and r["tk"] == "MOH":
            moh_plans[r["p"]] = moh_plans.get(r["p"], 0) + r["c"]
    for pn in sorted(moh_plans.keys(), key=lambda x: -moh_plans[x]):
        print(f"    {pn}: {moh_plans[pn]:>10,}")

    return rows

def process_language():
    print("\n=== Language (Newly Eligible) ===")
    if not os.path.exists(L_CSV):
        print("  WARNING: language.csv missing"); return None
    sz = os.path.getsize(L_CSV)
    if sz < 200:
        print(f"  WARNING: too small ({sz}b)"); return None
    print(f"  Size: {sz/(1024*1024):.2f} MB")

    with open(L_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        hdrs = [h.strip() for h in (reader.fieldnames or [])]
        print(f"  Columns: {hdrs}")

        period_col = next((h for h in hdrs if "period" in h.lower() or "quarter" in h.lower()),
                         next((h for h in hdrs if "year" in h.lower()), hdrs[0]))
        lang_col = next((h for h in hdrs if "language" in h.lower()), hdrs[2] if len(hdrs)>2 else None)
        count_col = next((h for h in hdrs if "number" in h.lower() or "eligible" in h.lower() or "count" in h.lower()),
                        hdrs[3] if len(hdrs)>3 else None)

        if not lang_col or not count_col:
            print("  WARNING: cant detect columns"); return None
        print(f"  Using: period={period_col}, lang={lang_col}, count={count_col}")

        rows = []
        for r in reader:
            h = {k.strip(): v for k, v in r.items()}
            period = h.get(period_col, "").strip()
            lang = h.get(lang_col, "").strip()
            count = pint(h.get(count_col, ""))
            if not period or not lang: continue
            rows.append({"q": period, "lang": lang, "c": count})

    print(f"  Total: {len(rows)} records")
    return rows

def main():
    os.makedirs(OUT, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    enrollment = process_enrollment()
    if enrollment:
        p = os.path.join(OUT, "enrollment.json")
        with open(p, "w") as f: json.dump({"updated": now, "data": enrollment}, f)
        print(f"\n  Wrote enrollment.json ({os.path.getsize(p)/(1024*1024):.1f} MB)")
    else:
        print("  FAILED"); exit(1)

    language = process_language()
    if language:
        p = os.path.join(OUT, "language.json")
        with open(p, "w") as f: json.dump({"updated": now, "data": language}, f)
        print(f"  Wrote language.json ({os.path.getsize(p)/(1024*1024):.2f} MB)")

    print("\nDone!")

if __name__ == "__main__": main()
