#!/usr/bin/env python3
"""
Generate competitive position reports for PT providers.

Creates two reports:
1. NPI Type 1 Comparison - Individual PTs vs competitors
2. NPI Type 2 Comparison - Clinics vs competitor clinics

Usage:
    python scripts/generate_competitive_report.py
"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

REPORTS_DIR = Path(__file__).parent.parent / "reports"
DB_PATH = Path(__file__).parent.parent / "data" / "rates.duckdb"

# Target NPIs (customize these for your analysis)
PRIMARY_TYPE2_NPI = "1073185393"  # Primary clinic
PRIMARY_TYPE1_NPI_A = "1326610783"  # Primary individual A
PRIMARY_TYPE1_NPI_B = "1699341354"  # Primary individual B


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
        "97542": "Wheelchair mgmt",
        "97750": "Physical perf test",
        "97760": "Orthotic training",
        "97010": "Hot/cold packs",
        "97032": "Electrical stim",
        "97035": "Ultrasound",
        "97150": "Group therapy",
        "97113": "Aquatic therapy",
        "20560": "Dry needling 1-2",
        "20561": "Dry needling 3+",
    }


def generate_type2_report(conn) -> str:
    """Generate Type 2 (Clinic) comparison report."""
    cpt_desc = get_cpt_descriptions()
    
    # Payers that only bill by Type 1 NPI (no Type 2 data expected)
    TYPE1_ONLY_PAYERS = {"HealthPartners"}
    
    # Get all Type 2 providers
    type2_npis = conn.execute("""
        SELECT npi, provider_name 
        FROM nppes_providers 
        WHERE provider_type = '2'
    """).fetchdf()
    
    if type2_npis.empty:
        return "No Type 2 providers found.\n"
    
    type2_set = set(type2_npis['npi'].tolist())
    npi_names = dict(zip(type2_npis['npi'], type2_npis['provider_name']))
    
    # Get payers
    payers = conn.execute("SELECT DISTINCT payer_name FROM rates ORDER BY payer_name").fetchdf()['payer_name'].tolist()
    
    lines = ["# NPI Type 2 Comparison (Median Rate by CPT code)\n"]
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    
    for payer in payers:
        # Handle payers that only bill by Type 1 NPI
        if payer in TYPE1_ONLY_PAYERS:
            lines.append(f"\n## {payer}\n")
            lines.append(f"*{payer} bills by Type 1 (individual) NPIs only - no Type 2 data available.*\n")
            continue
        
        # Get median rates for Type 2 providers at this payer
        rates_df = conn.execute(f"""
            SELECT 
                billing_code,
                npi,
                MEDIAN(negotiated_rate) as median_rate
            FROM rates
            WHERE payer_name = '{payer}'
              AND npi IN ({','.join(f"'{n}'" for n in type2_set)})
            GROUP BY billing_code, npi
        """).fetchdf()
        
        if rates_df.empty:
            continue
        
        lines.append(f"\n## {payer}\n")
        lines.append("```")
        lines.append(f"{'CPT':<8}{'Description':<22}{'Our Rate':<11}{'Rank':<7}{'Lowest':<30}{'Highest':<30}")
        lines.append("─" * 108)
        
        # Get CPT codes with data
        cpt_codes = sorted(rates_df['billing_code'].unique())
        
        for cpt in cpt_codes:
            cpt_rates = rates_df[rates_df['billing_code'] == cpt].copy()
            if cpt_rates.empty:
                continue
            
            # Sort by rate descending (highest = rank 1)
            cpt_rates = cpt_rates.sort_values('median_rate', ascending=False).reset_index(drop=True)
            
            total_providers = len(cpt_rates)
            
            # Get primary clinic's rate and rank
            primary_row = cpt_rates[cpt_rates['npi'] == PRIMARY_TYPE2_NPI]
            if primary_row.empty:
                continue
            
            primary_rate = primary_row['median_rate'].values[0]
            primary_rank = cpt_rates[cpt_rates['npi'] == PRIMARY_TYPE2_NPI].index[0] + 1
            
            # Get lowest and highest
            lowest_row = cpt_rates.iloc[-1]
            highest_row = cpt_rates.iloc[0]
            
            lowest_name = npi_names.get(lowest_row['npi'], 'Unknown')[:20]
            highest_name = npi_names.get(highest_row['npi'], 'Unknown')[:20]
            
            desc = cpt_desc.get(cpt, "")[:20]
            
            lowest_str = f"${lowest_row['median_rate']:.2f} ({lowest_name})"
            highest_str = f"${highest_row['median_rate']:.2f} ({highest_name})"
            
            lines.append(
                f"{cpt:<8}{desc:<22}${primary_rate:<10.2f}{primary_rank}/{total_providers:<5}{lowest_str:<30}{highest_str:<30}"
            )
        
        lines.append("```\n")
    
    return "\n".join(lines)


def generate_type1_report(conn) -> str:
    """Generate Type 1 (Individual) comparison report."""
    cpt_desc = get_cpt_descriptions()
    
    # Payers that only bill by Type 2 NPI (no Type 1 data expected)
    TYPE2_ONLY_PAYERS = {"UCare"}
    
    # Get all Type 1 providers
    type1_npis = conn.execute("""
        SELECT npi, provider_name 
        FROM nppes_providers 
        WHERE provider_type = '1'
    """).fetchdf()
    
    if type1_npis.empty:
        return "No Type 1 providers found.\n"
    
    type1_set = set(type1_npis['npi'].tolist())
    npi_names = dict(zip(type1_npis['npi'], type1_npis['provider_name']))
    
    # Get payers
    payers = conn.execute("SELECT DISTINCT payer_name FROM rates ORDER BY payer_name").fetchdf()['payer_name'].tolist()
    
    lines = ["# NPI Type 1 Comparison (Median Rate by CPT code)\n"]
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    
    for payer in payers:
        # Handle payers that only bill by Type 2 NPI
        if payer in TYPE2_ONLY_PAYERS:
            lines.append(f"\n## {payer}\n")
            lines.append(f"*{payer} bills by Type 2 (organization) NPIs only - no Type 1 data available.*\n")
            continue
        # Get median rates for Type 1 providers at this payer
        rates_df = conn.execute(f"""
            SELECT 
                billing_code,
                npi,
                MEDIAN(negotiated_rate) as median_rate
            FROM rates
            WHERE payer_name = '{payer}'
              AND npi IN ({','.join(f"'{n}'" for n in type1_set)})
            GROUP BY billing_code, npi
        """).fetchdf()
        
        if rates_df.empty:
            continue
        
        lines.append(f"\n## {payer}\n")
        lines.append("```")
        lines.append(f"{'CPT':<8}{'Description':<22}{'Indiv A':<11}{'Rank':<7}{'Indiv B':<11}{'Rank':<7}{'Lowest':<28}{'Highest':<28}")
        lines.append("─" * 122)
        
        # Get CPT codes with data
        cpt_codes = sorted(rates_df['billing_code'].unique())
        
        for cpt in cpt_codes:
            cpt_rates = rates_df[rates_df['billing_code'] == cpt].copy()
            if cpt_rates.empty:
                continue
            
            # Sort by rate descending (highest = rank 1)
            cpt_rates = cpt_rates.sort_values('median_rate', ascending=False).reset_index(drop=True)
            
            total_providers = len(cpt_rates)
            
            # Get Individual A's rate and rank
            indiv_a_row = cpt_rates[cpt_rates['npi'] == PRIMARY_TYPE1_NPI_A]
            if not indiv_a_row.empty:
                indiv_a_rate = indiv_a_row['median_rate'].values[0]
                indiv_a_rank = cpt_rates[cpt_rates['npi'] == PRIMARY_TYPE1_NPI_A].index[0] + 1
                indiv_a_str = f"${indiv_a_rate:.2f}"
                indiv_a_rank_str = f"{indiv_a_rank}/{total_providers}"
            else:
                indiv_a_str = "N/A"
                indiv_a_rank_str = "-"
            
            # Get Individual B's rate and rank
            indiv_b_row = cpt_rates[cpt_rates['npi'] == PRIMARY_TYPE1_NPI_B]
            if not indiv_b_row.empty:
                indiv_b_rate = indiv_b_row['median_rate'].values[0]
                indiv_b_rank = cpt_rates[cpt_rates['npi'] == PRIMARY_TYPE1_NPI_B].index[0] + 1
                indiv_b_str = f"${indiv_b_rate:.2f}"
                indiv_b_rank_str = f"{indiv_b_rank}/{total_providers}"
            else:
                indiv_b_str = "N/A"
                indiv_b_rank_str = "-"
            
            # Skip if neither individual has data
            if indiv_a_str == "N/A" and indiv_b_str == "N/A":
                continue
            
            # Get lowest and highest
            lowest_row = cpt_rates.iloc[-1]
            highest_row = cpt_rates.iloc[0]
            
            lowest_name = npi_names.get(lowest_row['npi'], 'Unknown')[:18]
            highest_name = npi_names.get(highest_row['npi'], 'Unknown')[:18]
            
            desc = cpt_desc.get(cpt, "")[:20]
            
            lowest_str = f"${lowest_row['median_rate']:.2f} ({lowest_name})"
            highest_str = f"${highest_row['median_rate']:.2f} ({highest_name})"
            
            lines.append(
                f"{cpt:<8}{desc:<22}{indiv_a_str:<11}{indiv_a_rank_str:<7}{indiv_b_str:<11}{indiv_b_rank_str:<7}{lowest_str:<28}{highest_str:<28}"
            )
        
        lines.append("```\n")
    
    return "\n".join(lines)


def generate_renegotiation_opportunities(conn) -> str:
    """Generate report of primary provider rates below market median."""
    cpt_desc = get_cpt_descriptions()
    
    # Primary NPIs (Type 2 clinic + Type 1 individuals)
    primary_npis = [PRIMARY_TYPE2_NPI, PRIMARY_TYPE1_NPI_A, PRIMARY_TYPE1_NPI_B]
    primary_npi_list = ",".join(f"'{n}'" for n in primary_npis)
    
    # Get payer medians for each CPT code (across all providers)
    payer_medians = conn.execute("""
        SELECT 
            payer_name,
            billing_code,
            MEDIAN(negotiated_rate) as payer_median
        FROM rates
        GROUP BY payer_name, billing_code
    """).fetchdf()
    
    # Get primary provider's median rates
    primary_rates = conn.execute(f"""
        SELECT 
            payer_name,
            billing_code,
            MEDIAN(negotiated_rate) as our_rate
        FROM rates
        WHERE npi IN ({primary_npi_list})
        GROUP BY payer_name, billing_code
    """).fetchdf()
    
    if primary_rates.empty:
        return "No rate data available for primary providers.\n"
    
    # Merge and calculate difference
    merged = primary_rates.merge(
        payer_medians, 
        on=['payer_name', 'billing_code'], 
        how='inner'
    )
    
    merged['pct_below'] = ((merged['our_rate'] - merged['payer_median']) / merged['payer_median']) * 100
    
    # Filter to only where primary provider is below median
    below_median = merged[merged['pct_below'] < 0].copy()
    
    if below_median.empty:
        return "No rates below market median found.\n"
    
    # Sort by payer, then CPT
    below_median = below_median.sort_values(['payer_name', 'billing_code'])
    
    # Take top 10 by % below (most negative first within the sorted order)
    # Actually, let's get top 10 most below, then sort by payer/CPT for display
    top10 = below_median.nsmallest(10, 'pct_below').sort_values(['payer_name', 'billing_code'])
    
    lines = ["# Renegotiation Opportunities (Rates Below Market)\n"]
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    lines.append("*Top 10 rates where primary providers are most below market median*\n")
    
    lines.append("```")
    lines.append(f"{'Payer':<19}{'CPT':<8}{'Description':<23}{'Our Rate':<18}{'Payer Median':<15}{'% Below':<10}")
    lines.append("─" * 93)
    
    for _, row in top10.iterrows():
        desc = cpt_desc.get(row['billing_code'], "")[:21]
        lines.append(
            f"{row['payer_name']:<19}{row['billing_code']:<8}{desc:<23}"
            f"${row['our_rate']:<17.2f}${row['payer_median']:<14.2f}{row['pct_below']:<10.1f}%"
        )
    
    lines.append("```\n")
    
    return "\n".join(lines)


def generate_coverage_summary(conn) -> str:
    """Generate data coverage summary report."""
    
    lines = ["# Data Coverage Summary\n"]
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    
    # Payer coverage
    payer_stats = conn.execute("""
        SELECT 
            payer_name,
            COUNT(*) as total_rates,
            COUNT(DISTINCT billing_code) as cpt_codes,
            COUNT(DISTINCT npi) as npis,
            MAX(last_updated) as last_updated
        FROM rates
        GROUP BY payer_name
        ORDER BY payer_name
    """).fetchdf()
    
    lines.append("## Payer Coverage\n")
    lines.append("```")
    lines.append(f"{'Payer':<20}{'Total Rates':<14}{'CPT Codes':<12}{'NPIs':<8}{'Last Updated':<14}")
    lines.append("─" * 68)
    
    for _, row in payer_stats.iterrows():
        last_upd = str(row['last_updated'])[:10] if row['last_updated'] else '-'
        lines.append(
            f"{row['payer_name']:<20}{row['total_rates']:<14,}{row['cpt_codes']:<12}{row['npis']:<8}{last_upd:<14}"
        )
    
    lines.append("```\n")
    
    # NPI coverage
    npi_stats = conn.execute("""
        SELECT 
            p.npi,
            p.provider_name,
            p.city,
            p.provider_type,
            COUNT(DISTINCT r.payer_name) as payers,
            COUNT(r.id) as rates
        FROM nppes_providers p
        LEFT JOIN rates r ON p.npi = r.npi
        GROUP BY p.npi, p.provider_name, p.city, p.provider_type
        ORDER BY rates DESC, p.provider_name
    """).fetchdf()
    
    lines.append("## NPI Coverage\n")
    lines.append("```")
    lines.append(f"{'NPI':<14}{'Provider Name':<30}{'City':<18}{'Type':<6}{'Payers':<9}{'Rates':<8}")
    lines.append("─" * 85)
    
    for _, row in npi_stats.iterrows():
        name = (row['provider_name'] or 'Unknown')[:28]
        city = (row['city'] or '-')[:16]
        ptype = row['provider_type'] or '-'
        payers = row['payers'] if row['payers'] else 0
        rates = row['rates'] if row['rates'] else 0
        lines.append(
            f"{row['npi']:<14}{name:<30}{city:<18}{ptype:<6}{payers:<9}{rates:<8,}"
        )
    
    lines.append("```\n")
    
    # Data limitations section
    lines.append("## Data Limitations\n")
    
    lines.append("### UnitedHealthcare\n")
    lines.append("UHC publishes Transparency in Coverage files, and our target NPIs appear in their ")
    lines.append("`provider_references` section. However, these provider groups are **not linked to any ")
    lines.append("PT rate entries** in the `in_network` section. The NPIs exist in the file but have no ")
    lines.append("associated negotiated rates for PT codes. This appears to be a systematic issue with ")
    lines.append("how UHC structures their data.\n")
    
    lines.append("### Payers Not Ingested\n")
    lines.append("The following payers have not been ingested due to technical barriers:\n")
    lines.append("- **Medica** - Requires browser automation; bot protection in place\n")
    lines.append("- **Cigna** - Complex file structure; requires browser automation\n")
    lines.append("- **Humana** - Bot protection and CAPTCHA requirements\n")
    lines.append("- **Medicare (CMS)** - Published separately via CMS; different file format\n")
    lines.append("- **Medicaid (MN DHS)** - State-published data; not in standard TiC format\n")
    
    return "\n".join(lines)


def generate_payer_summary_report(conn) -> str:
    """Generate simple payer rate summary by CPT code."""
    cpt_desc = get_cpt_descriptions()
    
    # Get payers
    payers = conn.execute("SELECT DISTINCT payer_name FROM rates ORDER BY payer_name").fetchdf()['payer_name'].tolist()
    
    # Get median rates by payer and CPT code (across all NPIs)
    rates_df = conn.execute("""
        SELECT 
            payer_name,
            billing_code,
            MEDIAN(negotiated_rate) as median_rate
        FROM rates
        GROUP BY payer_name, billing_code
        ORDER BY billing_code, payer_name
    """).fetchdf()
    
    if rates_df.empty:
        return "No rate data available.\n"
    
    # Get all CPT codes, sorted numerically
    cpt_codes = sorted(rates_df['billing_code'].unique(), key=lambda x: int(x) if x.isdigit() else 0)
    
    lines = ["# Payer Rate Summary by CPT Code\n"]
    lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    lines.append("*Median rate across all providers for each payer*\n")
    
    # Build header
    payer_cols = "".join(f"{p:<18}" for p in payers)
    lines.append("```")
    lines.append(f"{'CPT':<8}{'Description':<24}{payer_cols}")
    lines.append("─" * (32 + 18 * len(payers)))
    
    for cpt in cpt_codes:
        desc = cpt_desc.get(cpt, "")[:22]
        
        rate_strs = []
        for payer in payers:
            payer_rate = rates_df[(rates_df['billing_code'] == cpt) & (rates_df['payer_name'] == payer)]
            if not payer_rate.empty:
                rate_strs.append(f"${payer_rate['median_rate'].values[0]:<17.2f}")
            else:
                rate_strs.append(f"{'-':<18}")
        
        lines.append(f"{cpt:<8}{desc:<24}{''.join(rate_strs)}")
    
    lines.append("```\n")
    
    return "\n".join(lines)


def main():
    REPORTS_DIR.mkdir(exist_ok=True)
    
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    
    # Check we have data
    stats = conn.execute("SELECT COUNT(*) as cnt, COUNT(DISTINCT payer_name) as payers FROM rates").fetchone()
    print(f"Database: {stats[0]:,} rates from {stats[1]} payers")
    
    # Generate Type 2 report
    print("Generating Type 2 (Clinic) report...")
    type2_report = generate_type2_report(conn)
    type2_path = REPORTS_DIR / "local_competitor_rates_by_clinic.md"
    with open(type2_path, "w") as f:
        f.write(type2_report)
    print(f"  Saved: {type2_path}")
    
    # Generate Type 1 report
    print("Generating Type 1 (Individual) report...")
    type1_report = generate_type1_report(conn)
    type1_path = REPORTS_DIR / "local_competitor_rates_by_individual.md"
    with open(type1_path, "w") as f:
        f.write(type1_report)
    print(f"  Saved: {type1_path}")
    
    # Generate Payer Summary report
    print("Generating Payer Summary report...")
    summary_report = generate_payer_summary_report(conn)
    summary_path = REPORTS_DIR / "median_payer_rates_by_cpt_code.md"
    with open(summary_path, "w") as f:
        f.write(summary_report)
    print(f"  Saved: {summary_path}")
    
    # Generate Coverage Summary report
    print("Generating Coverage Summary report...")
    coverage_report = generate_coverage_summary(conn)
    coverage_path = REPORTS_DIR / "underlying_data_summary.md"
    with open(coverage_path, "w") as f:
        f.write(coverage_report)
    print(f"  Saved: {coverage_path}")
    
    # Generate Renegotiation Opportunities report
    print("Generating Renegotiation Opportunities report...")
    reneg_report = generate_renegotiation_opportunities(conn)
    reneg_path = REPORTS_DIR / "renegotiation_opportunities.md"
    with open(reneg_path, "w") as f:
        f.write(reneg_report)
    print(f"  Saved: {reneg_path}")
    
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
