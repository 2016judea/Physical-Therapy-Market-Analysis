#!/usr/bin/env python3
"""
Ingest HealthPartners in-network rate files.

HealthPartners publishes direct ZIP files per network containing JSON MRF data.
They use Type 1 (individual) NPIs only - no Type 2 (clinic) rates.

Usage:
    python scripts/ingest_healthpartners.py
"""

import gzip
import io
import json
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_cpt_codes, load_payers_config
from src.parser import parse_tic_file_simple, get_target_npis
from src.storage import RatesDatabase

console = Console(force_terminal=True)


def get_hp_file_urls() -> list[str]:
    """Get all HealthPartners file URLs from config."""
    config = load_payers_config()
    
    for payer in config.payers:
        if payer.name == "HealthPartners":
            urls = [payer.index_url]
            if payer.additional_files:
                urls.extend(payer.additional_files)
            return urls
    
    return []


def process_zip_file(url: str, db: RatesDatabase, target_npis: set[str], target_cpts: set[str]) -> int:
    """Download and process a HealthPartners ZIP file."""
    console.print(f"[blue]Downloading:[/blue] {url.split('/')[-1]}")
    
    try:
        resp = httpx.get(url, timeout=httpx.Timeout(30.0, read=600.0), follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        console.print(f"[red]Download failed: {e}[/red]")
        return 0
    
    console.print(f"  Downloaded {len(resp.content) / 1e6:.1f} MB")
    
    total_inserted = 0
    
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        json_files = [n for n in zf.namelist() if n.endswith('.json') or n.endswith('.json.gz')]
        console.print(f"  Found {len(json_files)} JSON files in ZIP")
        
        for json_file in json_files:
            file_source = f"{url}#{json_file}"
            
            if db.is_file_ingested(file_source):
                console.print(f"  [dim]Skipping (already ingested): {json_file}[/dim]")
                continue
            
            log_id = db.log_ingestion_start("HealthPartners", file_source)
            
            try:
                with zf.open(json_file) as f:
                    content = f.read()
                
                # Decompress if gzipped
                if json_file.endswith('.gz'):
                    content = gzip.decompress(content)
                
                data = json.loads(content)
                
                # Parse rates, filtering to our target NPIs
                records = list(parse_tic_file_simple(
                    data,
                    payer_name="HealthPartners",
                    file_source=file_source,
                    target_npis=target_npis,
                ))
                
                if records:
                    inserted = db.insert_rates(records)
                    db.log_ingestion_complete(log_id, inserted)
                    total_inserted += inserted
                    console.print(f"  [green]✓ {json_file}: {inserted:,} rates[/green]")
                else:
                    db.log_ingestion_complete(log_id, 0)
                    console.print(f"  [dim]{json_file}: 0 rates for our NPIs[/dim]")
                    
            except Exception as e:
                db.log_ingestion_error(log_id, str(e))
                console.print(f"  [red]Error processing {json_file}: {e}[/red]")
    
    return total_inserted


def main():
    start = datetime.now()
    console.print("[bold]=== HealthPartners Ingestion ===[/bold]")
    console.print(f"Started: {start}")
    
    # Get target NPIs from NPPES table
    target_npis = get_target_npis()
    if not target_npis:
        console.print("[red]No NPIs in nppes_providers table. Run load_mn_nppes.py first.[/red]")
        return
    
    console.print(f"Target NPIs: {len(target_npis)}")
    
    target_cpts = load_cpt_codes()
    console.print(f"Target CPT codes: {len(target_cpts)}")
    
    # Get file URLs
    urls = get_hp_file_urls()
    if not urls:
        console.print("[red]No HealthPartners URLs found in config/payers.yaml[/red]")
        return
    
    console.print(f"Networks to process: {len(urls)}")
    
    db = RatesDatabase()
    
    total = 0
    for i, url in enumerate(urls, 1):
        console.print(f"\n[bold]Network {i}/{len(urls)}[/bold]")
        inserted = process_zip_file(url, db, target_npis, target_cpts)
        total += inserted
    
    # Final stats
    stats = db.get_rate_stats()
    console.print(f"\n[bold green]=== Complete ===[/bold green]")
    console.print(f"HealthPartners rates added: {total:,}")
    console.print(f"Total rates in DB: {stats['total_rates']:,}")
    console.print(f"Duration: {datetime.now() - start}")
    
    db.close()


if __name__ == "__main__":
    main()
