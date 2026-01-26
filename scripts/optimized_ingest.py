#!/usr/bin/env python3
"""Optimized data ingestion - processes smaller files first for quick results."""

import json
import gzip
import zipfile
import io
import sys
from pathlib import Path
from datetime import date
from decimal import Decimal

import httpx
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage import RatesDatabase, RateRecord
from src.config import load_cpt_codes

console = Console()
TARGET_CPTS = load_cpt_codes()

# Limit file size to avoid memory issues (500MB compressed max)
MAX_FILE_SIZE_MB = 500


def fast_parse_mrf(content: bytes, payer_name: str, file_source: str) -> list[RateRecord]:
    """
    Fast parsing optimized for large files.
    Only extracts PT CPT codes, skips everything else immediately.
    """
    data = json.loads(content)
    
    entity_name = data.get("reporting_entity_name", payer_name)
    last_updated_str = data.get("last_updated_on", "")
    try:
        last_updated = date.fromisoformat(last_updated_str) if last_updated_str else None
    except ValueError:
        last_updated = None
    
    # Build provider map
    provider_map = {}
    for pref in data.get("provider_references", []):
        group_id = pref.get("provider_group_id")
        if group_id is not None:
            providers = []
            for pg in pref.get("provider_groups", []):
                npis = pg.get("npi", [])
                tin_info = pg.get("tin", {})
                tin_value = tin_info.get("value") if isinstance(tin_info, dict) else None
                for npi in npis:
                    providers.append({"npi": str(npi), "tin": tin_value})
            provider_map[group_id] = providers
    
    records = []
    in_network = data.get("in_network", [])
    
    for item in in_network:
        billing_code = item.get("billing_code", "")
        
        # Skip non-PT codes immediately
        if billing_code not in TARGET_CPTS:
            continue
        
        billing_code_type = item.get("billing_code_type", "CPT")
        
        for neg_rate in item.get("negotiated_rates", []):
            prov_refs = neg_rate.get("provider_references", [])
            
            # Resolve providers
            provider_list = []
            for ref_id in prov_refs:
                if ref_id in provider_map:
                    provider_list.extend(provider_map[ref_id])
            
            if not provider_list:
                continue
            
            for price in neg_rate.get("negotiated_prices", []):
                rate_value = price.get("negotiated_rate")
                if rate_value is None:
                    continue
                
                negotiated_type = price.get("negotiated_type", "")
                billing_class = price.get("billing_class", "")
                service_codes = price.get("service_code", [])
                pos = service_codes[0] if service_codes else None
                
                for prov in provider_list:
                    records.append(RateRecord(
                        payer_name=entity_name,
                        last_updated=last_updated,
                        billing_code=billing_code,
                        billing_code_type=billing_code_type,
                        negotiated_rate=Decimal(str(rate_value)),
                        negotiated_type=negotiated_type,
                        billing_class=billing_class,
                        place_of_service=pos,
                        npi=prov["npi"],
                        tin=prov.get("tin"),
                        file_source=file_source,
                    ))
    
    return records


def process_file(db: RatesDatabase, url: str, payer_name: str, desc: str = "") -> int:
    """Download and process a single file."""
    
    if db.is_file_ingested(url):
        console.print(f"  [dim]Already ingested[/dim]")
        return 0
    
    log_id = db.log_ingestion_start(payer_name, url)
    
    try:
        # Download
        console.print(f"  Downloading...")
        resp = httpx.get(url, timeout=httpx.Timeout(30.0, read=1800.0), follow_redirects=True)
        size_mb = len(resp.content) / 1024 / 1024
        console.print(f"  Downloaded {size_mb:.1f} MB")
        
        # Decompress
        console.print(f"  Decompressing...")
        if '.json.gz' in url or url.endswith('.gz'):
            content = gzip.decompress(resp.content)
        elif url.endswith('.zip'):
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                json_files = [n for n in zf.namelist() if n.endswith('.json')]
                if not json_files:
                    raise ValueError("No JSON in ZIP")
                content = zf.read(json_files[0])
        else:
            content = resp.content
        console.print(f"  Decompressed {len(content) / 1024 / 1024:.1f} MB")
        
        # Parse
        console.print(f"  Parsing...")
        records = fast_parse_mrf(content, payer_name, url)
        console.print(f"  Found {len(records):,} PT records")
        
        if not records:
            db.log_ingestion_complete(log_id, 0)
            return 0
        
        # Store
        console.print(f"  Storing...")
        inserted = db.insert_rates(records)
        db.log_ingestion_complete(log_id, inserted)
        console.print(f"  [green]✓ Inserted {inserted:,} records[/green]")
        return inserted
        
    except Exception as e:
        db.log_ingestion_error(log_id, str(e))
        console.print(f"\n  [red]Error: {e}[/red]")
        return 0


def get_bcbs_mn_files():
    """Get BCBS MN national files sorted by size."""
    console.print("[blue]Fetching BCBS MN index...[/blue]")
    
    url = "https://mktg.bluecrossmn.com/mrf/2026/2026-01-01_Blue_Cross_and_Blue_Shield_of_Minnesota_index.json"
    resp = httpx.get(url, timeout=60)
    data = resp.json()
    
    # Get national files only (they have PT codes)
    files = []
    seen = set()
    for rs in data.get("reporting_structure", []):
        for inf in rs.get("in_network_files", []):
            loc = inf.get("location", "")
            desc = inf.get("description", "")
            if loc.startswith("http") and "National" in desc and loc not in seen:
                seen.add(loc)
                files.append({"url": loc, "desc": desc})
    
    console.print(f"Found {len(files)} national files")
    
    # Check sizes
    console.print("[blue]Checking file sizes...[/blue]")
    sized = []
    for f in files[:50]:  # Check first 50
        try:
            head = httpx.head(f["url"], timeout=30, follow_redirects=True)
            size = int(head.headers.get("content-length", 0))
            size_mb = size / 1024 / 1024
            if size_mb <= MAX_FILE_SIZE_MB:
                sized.append({**f, "size": size, "size_mb": size_mb})
        except:
            pass
    
    # Sort by size
    sized.sort(key=lambda x: x["size"])
    console.print(f"Found {len(sized)} files under {MAX_FILE_SIZE_MB}MB")
    
    return sized


def main():
    console.print("[bold green]TiC Data Ingestion - Optimized[/bold green]\n")
    
    db = RatesDatabase()
    stats = db.get_rate_stats()
    console.print(f"Starting: {stats['total_rates']:,} existing rates\n")
    
    # Get BCBS MN files (sorted by size, smallest first)
    bcbs_files = get_bcbs_mn_files()
    
    # Process files
    total = 0
    for i, f in enumerate(bcbs_files, 1):
        console.print(f"\n[bold]BCBS MN File {i}/{len(bcbs_files)} ({f['size_mb']:.1f} MB)[/bold]")
        total += process_file(db, f["url"], "BCBS Minnesota", f["desc"])
        
        # Show progress every 5 files
        if i % 5 == 0:
            stats = db.get_rate_stats()
            console.print(f"\n[cyan]Progress: {stats['total_rates']:,} total rates, {stats['providers']:,} providers[/cyan]")
    
    # Final stats
    console.print("\n[bold green]═══ Complete ═══[/bold green]")
    stats = db.get_rate_stats()
    console.print(f"Total rates: {stats['total_rates']:,}")
    console.print(f"Payers: {stats['payers']}")
    console.print(f"CPT codes: {stats['cpt_codes']}")
    console.print(f"Providers: {stats['providers']:,}")
    
    db.close()


if __name__ == "__main__":
    main()
