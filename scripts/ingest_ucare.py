#!/usr/bin/env python3
"""
Ingest UCare in-network rate files.

UCare publishes a TOC (table of contents) JSON that lists MRF file URLs.
They use Type 2 (organization) NPIs only - no Type 1 (individual) rates.

Usage:
    python scripts/ingest_ucare.py
"""

import gzip
import json
import sys
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_cpt_codes, load_payers_config
from src.parser import parse_tic_file_simple, get_target_npis
from src.storage import RatesDatabase

console = Console(force_terminal=True)


def get_ucare_index_url() -> str:
    """Get UCare index URL from config."""
    config = load_payers_config()
    
    for payer in config.payers:
        if payer.name == "UCare":
            return payer.index_url
    
    return ""


def fetch_in_network_files(index_url: str) -> list[dict]:
    """Fetch list of in-network files from UCare TOC."""
    console.print(f"[blue]Fetching UCare index...[/blue]")
    
    resp = httpx.get(index_url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    data = resp.json()
    
    files = []
    for rs in data.get('reporting_structure', []):
        for inf in rs.get('in_network_files', []):
            loc = inf.get('location', '')
            desc = inf.get('description', '')
            if loc.startswith('http'):
                files.append({'url': loc, 'desc': desc})
    
    # Deduplicate by URL
    seen = set()
    unique = []
    for f in files:
        if f['url'] not in seen:
            seen.add(f['url'])
            unique.append(f)
    
    return unique


def process_mrf_file(file_info: dict, db: RatesDatabase, target_npis: set[str]) -> int:
    """Download and process a single MRF file."""
    url = file_info['url']
    desc = file_info.get('desc', '')[:50]
    
    if db.is_file_ingested(url):
        console.print(f"[dim]Skipping (already ingested): {desc}[/dim]")
        return 0
    
    log_id = db.log_ingestion_start("UCare", url)
    
    try:
        console.print(f"[blue]Downloading:[/blue] {desc}...")
        resp = httpx.get(url, timeout=httpx.Timeout(30.0, read=600.0), follow_redirects=True)
        resp.raise_for_status()
        console.print(f"  Downloaded {len(resp.content) / 1e6:.1f} MB")
        
        # Decompress if gzipped
        if url.endswith('.gz'):
            content = gzip.decompress(resp.content)
        else:
            content = resp.content
        
        data = json.loads(content)
        
        # Parse rates, filtering to our target NPIs
        records = list(parse_tic_file_simple(
            data,
            payer_name="UCare",
            file_source=url,
            target_npis=target_npis,
        ))
        
        if records:
            inserted = db.insert_rates(records)
            db.log_ingestion_complete(log_id, inserted)
            console.print(f"  [green]✓ {inserted:,} rates inserted[/green]")
            return inserted
        else:
            db.log_ingestion_complete(log_id, 0)
            console.print(f"  [dim]0 rates for our NPIs[/dim]")
            return 0
            
    except Exception as e:
        db.log_ingestion_error(log_id, str(e))
        console.print(f"  [red]Error: {e}[/red]")
        return 0


def main():
    start = datetime.now()
    console.print("[bold]=== UCare Ingestion ===[/bold]")
    console.print(f"Started: {start}")
    
    # Get target NPIs from NPPES table
    target_npis = get_target_npis()
    if not target_npis:
        console.print("[red]No NPIs in nppes_providers table. Run load_mn_nppes.py first.[/red]")
        return
    
    console.print(f"Target NPIs: {len(target_npis)}")
    
    target_cpts = load_cpt_codes()
    console.print(f"Target CPT codes: {len(target_cpts)}")
    
    # Get index URL
    index_url = get_ucare_index_url()
    if not index_url:
        console.print("[red]No UCare index URL found in config/payers.yaml[/red]")
        return
    
    # Fetch file list
    files = fetch_in_network_files(index_url)
    console.print(f"Found {len(files)} in-network files")
    
    db = RatesDatabase()
    
    # Check which are already ingested
    not_ingested = [f for f in files if not db.is_file_ingested(f['url'])]
    console.print(f"Already ingested: {len(files) - len(not_ingested)}")
    console.print(f"Remaining: {len(not_ingested)}")
    
    total = 0
    for i, file_info in enumerate(not_ingested, 1):
        console.print(f"\n[bold]File {i}/{len(not_ingested)}[/bold]")
        inserted = process_mrf_file(file_info, db, target_npis)
        total += inserted
    
    # Final stats
    stats = db.get_rate_stats()
    console.print(f"\n[bold green]=== Complete ===[/bold green]")
    console.print(f"UCare rates added: {total:,}")
    console.print(f"Total rates in DB: {stats['total_rates']:,}")
    console.print(f"Duration: {datetime.now() - start}")
    
    db.close()


if __name__ == "__main__":
    main()
