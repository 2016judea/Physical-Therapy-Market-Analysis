#!/usr/bin/env python3
"""
Ingest BCBS MN Local files using provider group mapping.

Uses data/bcbs_npi_to_groups.json to filter rates to our target NPIs.
BCBS Local files reference provider groups as floats like 720.0000237894.

Usage:
    nohup python scripts/ingest_bcbs_local.py > logs/bcbs_local.log 2>&1 &
"""

import gzip
import json
import sys
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

import httpx
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_cpt_codes
from src.storage import RatesDatabase, RateRecord

console = Console(force_terminal=True)

GROUP_MAPPING_FILE = Path(__file__).parent.parent / "data" / "bcbs_npi_to_groups.json"


def load_group_mapping() -> tuple[dict[int, set[str]], set[str]]:
    """
    Load the NPI to groups mapping and invert it.
    
    Returns:
        group_to_npis: dict mapping group_id -> set of NPIs in that group
        all_target_npis: set of all target NPIs
    """
    with open(GROUP_MAPPING_FILE) as f:
        data = json.load(f)
    
    npi_to_groups = data.get("npi_to_groups", {})
    
    # Invert: group_id -> set of NPIs
    group_to_npis: dict[int, set[str]] = {}
    all_target_npis = set()
    
    for npi, groups in npi_to_groups.items():
        all_target_npis.add(npi)
        for group_id in groups:
            if group_id not in group_to_npis:
                group_to_npis[group_id] = set()
            group_to_npis[group_id].add(npi)
    
    return group_to_npis, all_target_npis


def parse_bcbs_local_file(
    data: dict,
    payer_name: str,
    file_source: str,
    group_to_npis: dict[int, set[str]],
    target_cpts: set[str],
):
    """
    Parse BCBS Local file, matching provider_references to our known groups.
    
    BCBS Local files have provider_references as floats like 720.0000237894
    where 237894 is the group_id we scanned.
    """
    # Extract metadata
    last_updated_str = data.get("last_updated_on", "")
    try:
        last_updated = date.fromisoformat(last_updated_str) if last_updated_str else None
    except ValueError:
        last_updated = None
    
    # Build float_id -> group_id lookup from top-level provider_references
    float_to_group = {}
    for pr in data.get("provider_references", []):
        float_id = pr.get("provider_group_id")
        if float_id is not None:
            # 720.0000237894 -> 237894
            group_id = round((float_id - int(float_id)) * 10_000_000_000)
            float_to_group[float_id] = group_id
    
    records = []
    
    for item in data.get("in_network", []):
        billing_code = item.get("billing_code", "")
        billing_code_type = item.get("billing_code_type", "CPT")
        
        # Filter to target CPT codes
        if billing_code not in target_cpts:
            continue
        
        for neg_rate in item.get("negotiated_rates", []):
            # Get provider references - these are floats like 720.0000237894
            prov_refs = neg_rate.get("provider_references", [])
            
            # Find which of our target NPIs are in these references
            matched_npis = set()
            for ref in prov_refs:
                if ref in float_to_group:
                    group_id = float_to_group[ref]
                    if group_id in group_to_npis:
                        matched_npis.update(group_to_npis[group_id])
            
            if not matched_npis:
                continue
            
            for price in neg_rate.get("negotiated_prices", []):
                rate_value = price.get("negotiated_rate")
                if rate_value is None:
                    continue
                
                negotiated_type = price.get("negotiated_type", "")
                billing_class = price.get("billing_class", "")
                service_codes = price.get("service_code", [])
                pos = service_codes[0] if service_codes else None
                
                # Create a record for each matched NPI
                for npi in matched_npis:
                    records.append(RateRecord(
                        payer_name=payer_name,
                        last_updated=last_updated,
                        billing_code=billing_code,
                        billing_code_type=billing_code_type,
                        negotiated_rate=Decimal(str(rate_value)),
                        negotiated_type=negotiated_type,
                        billing_class=billing_class,
                        place_of_service=pos,
                        npi=npi,
                        tin=None,  # Not available in this format
                        file_source=file_source,
                    ))
    
    return records


def main():
    start = datetime.now()
    console.print("[bold]=== BCBS Minnesota Local File Ingestion ===[/bold]")
    console.print(f"Started: {start}")
    
    # Load group mapping
    if not GROUP_MAPPING_FILE.exists():
        console.print("[red]ERROR: Run scan_bcbs_groups.py first to create mapping[/red]")
        return
    
    group_to_npis, all_target_npis = load_group_mapping()
    console.print(f"[green]Loaded mapping: {len(group_to_npis)} groups -> {len(all_target_npis)} NPIs[/green]")
    
    if not group_to_npis:
        console.print("[red]No NPIs found in BCBS groups - nothing to ingest[/red]")
        return
    
    target_cpts = load_cpt_codes()
    console.print(f"Target CPT codes: {len(target_cpts)}")
    
    db = RatesDatabase()
    
    # Fetch index
    console.print("[blue]Fetching BCBS index...[/blue]")
    index_url = "https://mktg.bluecrossmn.com/mrf/2026/2026-01-01_Blue_Cross_and_Blue_Shield_of_Minnesota_index.json"
    resp = httpx.get(index_url, timeout=60)
    data = resp.json()
    
    # Extract Local files
    all_files = []
    for rs in data.get('reporting_structure', []):
        for inf in rs.get('in_network_files', []):
            loc = inf.get('location', '')
            desc = inf.get('description', '')
            if loc.startswith('http') and 'Local' in desc:
                all_files.append({'url': loc, 'desc': desc})
    
    # Deduplicate
    seen = set()
    unique_files = []
    for f in all_files:
        if f['url'] not in seen:
            seen.add(f['url'])
            unique_files.append(f)
    
    console.print(f"Found {len(unique_files)} Local files")
    
    # Check which are already ingested
    not_ingested = []
    for f in unique_files:
        if not db.is_file_ingested(f['url']):
            not_ingested.append(f)
    
    console.print(f"[yellow]Already ingested: {len(unique_files) - len(not_ingested)}[/yellow]")
    console.print(f"[green]Remaining to ingest: {len(not_ingested)}[/green]")
    
    if not not_ingested:
        console.print("[dim]All BCBS Local files already ingested[/dim]")
        db.close()
        return
    
    # Get sizes and sort (smallest first)
    sized_files = []
    console.print("[blue]Checking file sizes...[/blue]")
    for f in not_ingested:  # Process all files
        try:
            head = httpx.head(f['url'], timeout=30, follow_redirects=True)
            size = int(head.headers.get('content-length', 0))
            sized_files.append({**f, 'size': size})
        except Exception as e:
            console.print(f"[dim]Skipping size check: {e}[/dim]")
            sized_files.append({**f, 'size': 0})
    
    sized_files.sort(key=lambda x: x['size'])
    console.print(f"Processing {len(sized_files)} files (smallest first)")
    
    total = 0
    files_with_rates = 0
    
    for i, f in enumerate(sized_files, 1):
        size_mb = f['size'] / 1024 / 1024
        console.print(f"\n[bold]File {i}/{len(sized_files)} ({size_mb:.1f} MB)[/bold]")
        console.print(f"  {f['desc'][:60]}")
        
        log_id = db.log_ingestion_start("BCBS Minnesota", f['url'])
        
        try:
            console.print(f"  [blue]Downloading...[/blue]", end="")
            resp = httpx.get(f['url'], timeout=httpx.Timeout(30.0, read=3600.0), follow_redirects=True)
            console.print(f" {len(resp.content)/1e6:.1f} MB")
            
            console.print(f"  [blue]Decompressing...[/blue]", end="")
            content = gzip.decompress(resp.content)
            console.print(f" {len(content)/1e6:.1f} MB")
            
            console.print(f"  [blue]Parsing...[/blue]", end="")
            mrf_data = json.loads(content)
            records = parse_bcbs_local_file(
                mrf_data, 
                "BCBS Minnesota", 
                f['url'],
                group_to_npis,
                target_cpts,
            )
            console.print(f" {len(records):,} PT records for our NPIs")
            
            if records:
                console.print(f"  [blue]Storing...[/blue]", end="")
                inserted = db.insert_rates(records)
                db.log_ingestion_complete(log_id, inserted)
                console.print(f" [green]✓ {inserted:,} inserted[/green]")
                total += inserted
                files_with_rates += 1
            else:
                db.log_ingestion_complete(log_id, 0)
            
        except Exception as e:
            db.log_ingestion_error(log_id, str(e))
            console.print(f"\n  [red]Error: {e}[/red]")
    
    # Final stats
    stats = db.get_rate_stats()
    console.print(f"\n[bold green]=== Complete ===[/bold green]")
    console.print(f"BCBS Local files with our rates: {files_with_rates}/{len(sized_files)}")
    console.print(f"BCBS rates added: {total}")
    console.print(f"Total rates in DB: {stats['total_rates']:,}")
    console.print(f"Duration: {datetime.now() - start}")
    
    db.close()


if __name__ == "__main__":
    main()
