#!/usr/bin/env python3
"""Full data ingestion script for MN payers."""

import json
import gzip
import zipfile
import io
import sys
from pathlib import Path

import httpx
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parser import parse_tic_file_simple
from src.storage import RatesDatabase
from src.config import load_cpt_codes

console = Console()
target_cpts = load_cpt_codes()


def process_file(db: RatesDatabase, url: str, payer_name: str, description: str = "") -> int:
    """Download, parse, and store a single MRF file."""
    
    # Check if already ingested
    if db.is_file_ingested(url):
        console.print(f"  [dim]Skipping (already ingested)[/dim]")
        return 0
    
    log_id = db.log_ingestion_start(payer_name, url)
    
    try:
        # Download with longer timeout for large files
        console.print(f"  [blue]Downloading...[/blue]", end="")
        resp = httpx.get(url, timeout=httpx.Timeout(30.0, read=3600.0), follow_redirects=True)
        size_mb = len(resp.content) / 1024 / 1024
        console.print(f" {size_mb:.1f} MB")
        
        # Decompress based on file type
        console.print(f"  [blue]Decompressing...[/blue]", end="")
        if '.json.gz' in url or url.endswith('.gz'):
            content = gzip.decompress(resp.content)
        elif url.endswith('.zip'):
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                json_files = [n for n in zf.namelist() if n.endswith('.json')]
                if not json_files:
                    raise ValueError("No JSON file in ZIP")
                content = zf.read(json_files[0])
        else:
            content = resp.content
        console.print(f" {len(content) / 1024 / 1024:.1f} MB")
        
        # Parse
        console.print(f"  [blue]Parsing...[/blue]", end="")
        mrf_data = json.loads(content)
        records = list(parse_tic_file_simple(mrf_data, payer_name, url))
        console.print(f" {len(records):,} PT records")
        
        if not records:
            db.log_ingestion_complete(log_id, 0)
            return 0
        
        # Store
        console.print(f"  [blue]Storing...[/blue]", end="")
        inserted = db.insert_rates(records)
        db.log_ingestion_complete(log_id, inserted)
        console.print(f" [green]✓ {inserted:,} inserted[/green]")
        return inserted
        
    except Exception as e:
        db.log_ingestion_error(log_id, str(e))
        console.print(f"\n  [red]Error: {e}[/red]")
        return 0


def ingest_healthpartners(db: RatesDatabase) -> int:
    """Ingest HealthPartners files (direct ZIP downloads)."""
    console.print("\n[bold cyan]═══ HealthPartners ═══[/bold cyan]")
    
    networks = [
        ("HP-Select", "HealthPartners_HP-Select"),
        ("Open-Access+Simplica", "HealthPartners_Open-Access+Simplica"),
        ("Cornerstone", "HealthPartners_Cornerstone"),
        ("Achieve-Large-Employer", "HealthPartners_Achieve-Large-Employer"),
        ("Achieve-Small-Employer+Peak", "HealthPartners_Achieve-Small-Employer+Peak"),
        ("State-of-MN", "HealthPartners_State-of-MN"),
    ]
    
    base_url = "https://mrfproddestinationdata.blob.core.windows.net/mrf-output"
    total = 0
    
    for name, file_prefix in networks:
        url = f"{base_url}/2026-01-01_{file_prefix}_in-network-rates.zip"
        console.print(f"\n[bold]{name}[/bold]")
        
        # Check if file exists
        try:
            head = httpx.head(url, timeout=30, follow_redirects=True)
            if head.status_code != 200:
                console.print(f"  [yellow]Not found (HTTP {head.status_code})[/yellow]")
                continue
        except:
            console.print(f"  [yellow]Could not check file[/yellow]")
            continue
            
        total += process_file(db, url, "HealthPartners", name)
    
    return total


def ingest_bcbs_mn(db: RatesDatabase, max_files: int = None) -> int:
    """Ingest BCBS Minnesota files from their index."""
    console.print("\n[bold cyan]═══ BCBS Minnesota ═══[/bold cyan]")
    
    # Fetch index
    console.print("[blue]Fetching index...[/blue]")
    index_url = "https://mktg.bluecrossmn.com/mrf/2026/2026-01-01_Blue_Cross_and_Blue_Shield_of_Minnesota_index.json"
    resp = httpx.get(index_url, timeout=60)
    data = resp.json()
    
    # Extract all in-network file URLs
    all_files = []
    for rs in data.get('reporting_structure', []):
        for inf in rs.get('in_network_files', []):
            loc = inf.get('location', '')
            desc = inf.get('description', '')
            if loc.startswith('http'):
                all_files.append({'url': loc, 'desc': desc})
    
    # Deduplicate by URL
    seen = set()
    unique_files = []
    for f in all_files:
        if f['url'] not in seen:
            seen.add(f['url'])
            unique_files.append(f)
    
    console.print(f"Found {len(unique_files)} unique files")
    
    # Sort: prefer "National" files (have PT codes) and smaller files first
    # Check sizes for first batch
    console.print("[blue]Checking file sizes...[/blue]")
    
    sized_files = []
    national_files = [f for f in unique_files if 'National' in f['desc']]
    
    # Sample sizes from first 20 national files
    for f in national_files[:30]:
        try:
            head = httpx.head(f['url'], timeout=30, follow_redirects=True)
            size = int(head.headers.get('content-length', 0))
            sized_files.append({**f, 'size': size})
        except:
            pass
    
    # Sort by size (smallest first for faster initial results)
    sized_files.sort(key=lambda x: x['size'])
    
    console.print(f"Processing {len(sized_files)} national files (sorted by size)")
    
    if max_files:
        sized_files = sized_files[:max_files]
        console.print(f"Limited to {max_files} files")
    
    total = 0
    for i, f in enumerate(sized_files, 1):
        size_mb = f['size'] / 1024 / 1024
        console.print(f"\n[bold]File {i}/{len(sized_files)} ({size_mb:.1f} MB)[/bold]")
        total += process_file(db, f['url'], "BCBS Minnesota", f['desc'])
        
        # Show running total
        stats = db.get_rate_stats()
        console.print(f"[dim]Running total: {stats['total_rates']:,} rates[/dim]")
    
    return total


def main():
    console.print("[bold green]╔══════════════════════════════════════╗[/bold green]")
    console.print("[bold green]║   TiC Data Pipeline - Full Ingest    ║[/bold green]")
    console.print("[bold green]╚══════════════════════════════════════╝[/bold green]")
    
    db = RatesDatabase()
    
    # Show initial stats
    stats = db.get_rate_stats()
    console.print(f"\nStarting with: {stats['total_rates']:,} existing rates")
    
    total_inserted = 0
    
    # 1. HealthPartners
    total_inserted += ingest_healthpartners(db)
    
    # 2. BCBS Minnesota (limit to manageable number for initial load)
    total_inserted += ingest_bcbs_mn(db, max_files=20)
    
    # Final stats
    console.print("\n[bold green]═══ Ingestion Complete ═══[/bold green]")
    stats = db.get_rate_stats()
    console.print(f"Total rates: {stats['total_rates']:,}")
    console.print(f"Unique payers: {stats['payers']}")
    console.print(f"Unique CPT codes: {stats['cpt_codes']}")
    console.print(f"Unique providers: {stats['providers']:,}")
    
    db.close()


if __name__ == "__main__":
    main()
