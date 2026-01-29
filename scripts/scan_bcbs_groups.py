#!/usr/bin/env python3
"""
Scan BCBS provider group files to find which groups contain our NPIs.

Usage:
    nohup python scripts/scan_bcbs_groups.py > logs/bcbs_group_scan.log 2>&1 &
"""

import json
import sys
from pathlib import Path
from datetime import datetime
import concurrent.futures

import httpx
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.storage import RatesDatabase

console = Console(force_terminal=True)

BASE_URL = "https://mrfdata.hmhs.com/files/720/mn/inbound/local/providergrp/new/720_pdo_prov_mrf_prvgrp_11_{:010d}.json"
START_ID = 237894
END_ID = 265242
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "bcbs_npi_to_groups.json"


def main():
    start_time = datetime.now()
    console.print("[bold]=== BCBS Provider Group Scan ===[/bold]")
    console.print(f"Started: {start_time}")
    
    # Get all our NPIs
    db = RatesDatabase()
    npis_df = db.query_df("SELECT npi, provider_name FROM nppes_providers")
    db.close()
    
    all_npis = set(npis_df["npi"].tolist())
    npi_names = dict(zip(npis_df["npi"], npis_df["provider_name"]))
    
    console.print(f"Target NPIs: {len(all_npis)}")
    console.print(f"Scanning groups {START_ID} to {END_ID} ({END_ID - START_ID:,} groups)")
    
    found = {npi: [] for npi in all_npis}
    checked = 0
    errors = 0
    
    def check_group(group_id):
        url = BASE_URL.format(group_id)
        try:
            resp = httpx.get(url, timeout=10)
            if resp.status_code == 200:
                text = resp.text
                matches = []
                for npi in all_npis:
                    if npi in text:
                        matches.append((group_id, npi))
                return matches, None
            return [], None
        except Exception as e:
            return [], str(e)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(check_group, gid): gid for gid in range(START_ID, END_ID)}
        
        for future in concurrent.futures.as_completed(futures):
            checked += 1
            results, error = future.result()
            
            if error:
                errors += 1
            
            for group_id, npi in results:
                if group_id not in found[npi]:
                    found[npi].append(group_id)
                    name = npi_names.get(npi, "Unknown")[:30]
                    console.print(f"[green]FOUND:[/green] {npi} ({name}) -> group {group_id}")
            
            if checked % 2000 == 0:
                found_count = sum(1 for v in found.values() if v)
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = checked / elapsed
                remaining = (END_ID - START_ID - checked) / rate / 60
                console.print(
                    f"  {checked:,}/{END_ID-START_ID:,} | "
                    f"{found_count}/{len(all_npis)} NPIs found | "
                    f"~{remaining:.1f}min remaining"
                )
    
    # Save results
    console.print(f"\n[bold]=== Results ===[/bold]")
    
    # Filter to only NPIs that were found
    found_npis = {npi: groups for npi, groups in found.items() if groups}
    missing_npis = [npi for npi, groups in found.items() if not groups]
    
    console.print(f"NPIs found in BCBS: {len(found_npis)}/{len(all_npis)}")
    for npi, groups in sorted(found_npis.items()):
        name = npi_names.get(npi, "Unknown")[:35]
        console.print(f"  {npi} ({name}): {len(groups)} groups")
    
    console.print(f"\nNPIs NOT in BCBS: {len(missing_npis)}")
    for npi in missing_npis[:10]:
        name = npi_names.get(npi, "Unknown")[:35]
        console.print(f"  {npi} ({name})")
    
    # Save mapping
    output = {
        "scan_date": start_time.isoformat(),
        "total_groups_scanned": END_ID - START_ID,
        "npis_found": len(found_npis),
        "npis_missing": len(missing_npis),
        "npi_to_groups": found_npis,
        "missing_npis": missing_npis,
    }
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    console.print(f"\nSaved to: {OUTPUT_FILE}")
    
    console.print(f"\n[bold]Duration: {datetime.now() - start_time}[/bold]")


if __name__ == "__main__":
    main()
