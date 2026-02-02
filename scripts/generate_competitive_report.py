#!/usr/bin/env python3
"""
Generate competitive position reports for PT clinics.

All rates are normalized to the clinic level using npi_groups mappings,
and deduplicated so the same rate isn't counted multiple times.

Creates CSV reports:
1. Clinic Competitive Comparison - Your clinic vs competitors by payer/CPT
2. Payer Summary - Median rates by CPT code per payer
3. Clinic Data Summary - Data coverage by clinic
4. Renegotiation Opportunities - Rates below market median

Usage:
    python scripts/generate_competitive_report.py
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

REPORTS_DIR = Path(__file__).parent.parent / "reports"
DB_PATH = Path(__file__).parent.parent / "data" / "rates.duckdb"

# Primary clinic NPI for comparison
PRIMARY_CLINIC_NPI = "1073185393"  # Maverick Physiotherapy


def get_cpt_descriptions():
    """Map of CPT codes to descriptions."""
    return {
        "97161": "PT eval low",
        "97162": "PT eval moderate",
        "97163": "PT eval high",
        "97164": "PT re-evaluation",
        "97110": "Therapeutic exercises",
        "97112": "Neuromuscular re-ed",
        "97116": "Gait training",
        "97140": "Manual therapy",
        "97530": "Therapeutic activities",
        "97535": "Self-care training",
        "97537": "Community/work reintegration",
        "97542": "Wheelchair mgmt",
        "97545": "Work hardening (first 2 hrs)",
        "97546": "Work hardening (+1 hr)",
        "97750": "Physical perf test",
        "97755": "Assistive tech assessment",
        "97760": "Orthotic training",
        "97761": "Prosthetic training",
        "97763": "Orthotic/prosthetic mgmt",
        "97010": "Hot/cold packs",
        "97012": "Mechanical traction",
        "97014": "Electrical stim (unattended)",
        "97016": "Vasopneumatic devices",
        "97018": "Paraffin bath",
        "97022": "Whirlpool",
        "97024": "Diathermy",
        "97032": "Electrical stim (manual)",
        "97033": "Iontophoresis",
        "97034": "Contrast baths",
        "97035": "Ultrasound",
        "97036": "Hubbard tank",
        "97150": "Group therapy",
        "97113": "Aquatic therapy",
        "20560": "Dry needling 1-2",
        "20561": "Dry needling 3+",
        "99211": "Office visit minimal",
        "99212": "Office visit straightforward",
        "99213": "Office visit low",
        "99214": "Office visit moderate",
    }


def write_csv(path: Path, rows: list, fieldnames: list):
    """Write rows to a CSV file."""
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def create_normalized_rates_view(conn):
    """
    Create a view that:
    1. Normalizes NPIs - rolls up individual NPIs to their parent clinic
    2. Deduplicates rates - same payer/code/rate combo counted once per clinic
    """
    conn.execute("""
        CREATE OR REPLACE TEMPORARY VIEW normalized_rates AS
        SELECT DISTINCT
            r.payer_name,
            r.billing_code,
            r.negotiated_rate,
            -- Normalize NPI: use org NPI if individual is mapped, otherwise use original
            COALESCE(g.organization_npi, TRIM(r.npi)) as clinic_npi,
            r.last_updated
        FROM rates r
        LEFT JOIN npi_groups g ON TRIM(r.npi) = g.individual_npi
    """)


def generate_clinic_comparison(conn) -> list:
    """Generate clinic-level competitive comparison report."""
    cpt_desc = get_cpt_descriptions()
    
    # Get all clinics
    clinics = conn.execute("""
        SELECT npi, provider_name 
        FROM nppes_providers 
        WHERE provider_type = 'Organization'
    """).fetchdf()
    
    if clinics.empty:
        return []
    
    clinic_set = set(clinics['npi'].tolist())
    npi_names = dict(zip(clinics['npi'], clinics['provider_name']))
    
    payers = conn.execute("""
        SELECT DISTINCT payer_name FROM normalized_rates ORDER BY payer_name
    """).fetchdf()['payer_name'].tolist()
    
    rows = []
    
    for payer in payers:
        # Get deduplicated rates per clinic
        rates_df = conn.execute(f"""
            SELECT 
                billing_code,
                clinic_npi,
                MEDIAN(negotiated_rate) as median_rate
            FROM normalized_rates
            WHERE payer_name = '{payer}'
              AND clinic_npi IN ({','.join(f"'{n}'" for n in clinic_set)})
            GROUP BY billing_code, clinic_npi
        """).fetchdf()
        
        if rates_df.empty:
            continue
        
        cpt_codes = sorted(rates_df['billing_code'].unique())
        
        for cpt in cpt_codes:
            cpt_rates = rates_df[rates_df['billing_code'] == cpt].copy()
            if cpt_rates.empty:
                continue
            
            cpt_rates = cpt_rates.sort_values('median_rate', ascending=False).reset_index(drop=True)
            total_clinics = len(cpt_rates)
            
            # Find our clinic's position
            our_row = cpt_rates[cpt_rates['clinic_npi'] == PRIMARY_CLINIC_NPI]
            if our_row.empty:
                continue
            
            our_rate = our_row['median_rate'].values[0]
            our_rank = our_row.index[0] + 1
            
            lowest_row = cpt_rates.iloc[-1]
            highest_row = cpt_rates.iloc[0]
            
            rows.append({
                'Payer': payer,
                'CPT': cpt,
                'Description': cpt_desc.get(cpt, ""),
                'Maverick Rate': round(our_rate, 2),
                'Rank': our_rank,
                'Total Clinics': total_clinics,
                'Lowest Rate': round(lowest_row['median_rate'], 2),
                'Lowest Clinic': npi_names.get(lowest_row['clinic_npi'], 'Unknown'),
                'Highest Rate': round(highest_row['median_rate'], 2),
                'Highest Clinic': npi_names.get(highest_row['clinic_npi'], 'Unknown'),
            })
    
    return rows


def generate_payer_summary(conn) -> tuple:
    """Generate median rates by CPT code per payer (using normalized rates)."""
    cpt_desc = get_cpt_descriptions()
    
    payers = conn.execute("""
        SELECT DISTINCT payer_name FROM normalized_rates ORDER BY payer_name
    """).fetchdf()['payer_name'].tolist()
    
    rates_df = conn.execute("""
        SELECT 
            payer_name,
            billing_code,
            MEDIAN(negotiated_rate) as median_rate
        FROM normalized_rates
        GROUP BY payer_name, billing_code
        ORDER BY billing_code, payer_name
    """).fetchdf()
    
    if rates_df.empty:
        return [], []
    
    cpt_codes = sorted(rates_df['billing_code'].unique(), key=lambda x: int(x) if x.isdigit() else 0)
    
    rows = []
    for cpt in cpt_codes:
        row = {
            'CPT': cpt,
            'Description': cpt_desc.get(cpt, ""),
        }
        for payer in payers:
            payer_rate = rates_df[(rates_df['billing_code'] == cpt) & (rates_df['payer_name'] == payer)]
            if not payer_rate.empty:
                row[payer] = round(payer_rate['median_rate'].values[0], 2)
            else:
                row[payer] = None
        rows.append(row)
    
    return rows, payers


def generate_clinic_data_summary(conn) -> list:
    """Generate data summary for clinics using normalized/deduplicated rates."""
    rows = []
    
    # Count unique rate records per clinic after normalization
    # Use INNER JOIN to only show clinics with actual rate data
    clinic_stats = conn.execute("""
        SELECT 
            p.npi,
            p.provider_name,
            p.city,
            COUNT(DISTINCT nr.payer_name) as payers,
            COUNT(*) as unique_rates
        FROM nppes_providers p
        INNER JOIN normalized_rates nr ON p.npi = nr.clinic_npi
        WHERE p.provider_type = 'Organization'
        GROUP BY p.npi, p.provider_name, p.city
        ORDER BY unique_rates DESC, p.provider_name
    """).fetchdf()
    
    for _, row in clinic_stats.iterrows():
        rows.append({
            'NPI': row['npi'],
            'Name': row['provider_name'] or '',
            'City': row['city'] or '',
            'Payers': int(row['payers'] or 0),
            'Unique Rates': int(row['unique_rates'] or 0),
        })
    
    return rows


def main():
    REPORTS_DIR.mkdir(exist_ok=True)
    
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    # Create normalized rates view (rolls up individuals to clinics, deduplicates)
    create_normalized_rates_view(conn)
    
    raw_count = conn.execute("SELECT COUNT(*) FROM rates").fetchone()[0]
    norm_count = conn.execute("SELECT COUNT(*) FROM normalized_rates").fetchone()[0]
    payer_count = conn.execute("SELECT COUNT(DISTINCT payer_name) FROM normalized_rates").fetchone()[0]
    print(f"Database: {raw_count:,} raw rates -> {norm_count:,} normalized rates from {payer_count} payers")
    
    # 1. Clinic Competitive Comparison
    print("Generating Clinic Comparison report...")
    comp_rows = generate_clinic_comparison(conn)
    comp_path = REPORTS_DIR / "clinic_competitive_comparison.csv"
    write_csv(comp_path, comp_rows, [
        'Payer', 'CPT', 'Description', 'Maverick Rate', 'Rank', 'Total Clinics',
        'Lowest Rate', 'Lowest Clinic', 'Highest Rate', 'Highest Clinic'
    ])
    print(f"  Saved: {comp_path}")
    
    # 2. Payer Summary
    print("Generating Payer Summary report...")
    summary_rows, payers = generate_payer_summary(conn)
    if summary_rows:
        summary_path = REPORTS_DIR / "payer_rates_by_cpt.csv"
        write_csv(summary_path, summary_rows, ['CPT', 'Description'] + payers)
        print(f"  Saved: {summary_path}")
    
    # 3. Clinic Data Summary
    print("Generating Clinic Data Summary report...")
    clinic_rows = generate_clinic_data_summary(conn)
    clinic_path = REPORTS_DIR / "clinic_data_summary.csv"
    write_csv(clinic_path, clinic_rows, ['NPI', 'Name', 'City', 'Payers', 'Unique Rates'])
    print(f"  Saved: {clinic_path}")
    
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
