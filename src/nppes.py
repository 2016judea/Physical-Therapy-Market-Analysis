"""NPPES (National Provider Identifier) data integration.

Downloads and filters NPPES data to get Minnesota Physical Therapy providers.
Uses NPPES API for faster loading (vs bulk file download).
"""

import csv
import io
import zipfile
from pathlib import Path
from typing import Optional
import time

import httpx
from rich.console import Console
from rich.progress import Progress

from .storage import RatesDatabase
from .config import get_data_dir

console = Console()

# Physical Therapy taxonomy codes
PT_TAXONOMY_CODES = {
    "225100000X": "Physical Therapist",
    "225200000X": "Physical Therapy Assistant", 
}

# NPPES API endpoint
NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"

# NPPES monthly file URL (updated monthly) - fallback
NPPES_DOWNLOAD_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_2026.zip"


def fetch_providers_via_api(
    state: str = "MN",
    taxonomy_codes: list[str] = None,
) -> list[dict]:
    """
    Fetch PT providers from NPPES API by ZIP code prefix.
    
    The state filter doesn't work reliably, so we query by MN ZIP prefixes.
    """
    taxonomy_codes = taxonomy_codes or list(PT_TAXONOMY_CODES.keys())
    providers = []
    seen_npis = set()
    
    # Minnesota ZIP code prefixes
    mn_zip_prefixes = [
        "550", "551", "553", "554", "556", "557", "558", "559",
        "560", "561", "562", "563", "564", "565", "566", "567"
    ]
    
    console.print(f"[bold]Fetching {state} PT providers via NPPES API...[/bold]")
    console.print(f"  Querying {len(mn_zip_prefixes)} ZIP prefixes x {len(taxonomy_codes)} taxonomies")
    
    with httpx.Client(timeout=30) as client:
        for taxonomy in taxonomy_codes:
            desc = PT_TAXONOMY_CODES.get(taxonomy, "Physical Therapist")
            console.print(f"\n  Taxonomy: {taxonomy} ({desc})")
            
            for zip_prefix in mn_zip_prefixes:
                skip = 0
                limit = 200
                
                while True:
                    params = {
                        "version": "2.1",
                        "postal_code": f"{zip_prefix}*",
                        "taxonomy_description": desc,
                        "limit": limit,
                        "skip": skip,
                    }
                    
                    try:
                        resp = client.get(NPPES_API_URL, params=params)
                        resp.raise_for_status()
                        data = resp.json()
                    except Exception as e:
                        console.print(f"    [red]API error for {zip_prefix}: {e}[/red]")
                        break
                    
                    results = data.get("results", [])
                    if not results:
                        break
                    
                    for r in results:
                        npi = r.get("number", "")
                        if not npi or npi in seen_npis:
                            continue
                        seen_npis.add(npi)
                        
                        basic = r.get("basic", {})
                        
                        if r.get("enumeration_type") == "NPI-1":
                            name = f"{basic.get('first_name', '')} {basic.get('last_name', '')}".strip()
                            prov_type = "Individual"
                        else:
                            name = basic.get("organization_name", "")
                            prov_type = "Organization"
                        
                        addresses = r.get("addresses", [])
                        primary = next((a for a in addresses if a.get("address_purpose") == "LOCATION"), {})
                        
                        taxonomies = r.get("taxonomies", [])
                        primary_tax = next((t for t in taxonomies if t.get("primary")), taxonomies[0] if taxonomies else {})
                        
                        providers.append({
                            "npi": npi,
                            "provider_name": name,
                            "provider_type": prov_type,
                            "taxonomy_code": primary_tax.get("code", taxonomy),
                            "taxonomy_desc": primary_tax.get("desc", ""),
                            "address_line1": primary.get("address_1", ""),
                            "city": primary.get("city", ""),
                            "state": primary.get("state", state),
                            "zip": primary.get("postal_code", "")[:5] if primary.get("postal_code") else "",
                            "phone": primary.get("telephone_number", ""),
                        })
                    
                    if len(results) < limit:
                        break
                    skip += limit
                    time.sleep(0.2)
                
                console.print(f"    ZIP {zip_prefix}*: {len(seen_npis)} total so far", end="\r")
            
            console.print(f"    Collected {len(seen_npis)} unique NPIs")
    
    console.print(f"\n[green]Total: {len(providers)} MN PT providers[/green]")
    return providers


def download_nppes_file(output_path: Path) -> Path:
    """Download the NPPES full file."""
    console.print(f"[bold]Downloading NPPES file (~1GB)...[/bold]")
    console.print(f"URL: {NPPES_DOWNLOAD_URL}")
    
    with httpx.Client(timeout=600, follow_redirects=True) as client:
        with client.stream("GET", NPPES_DOWNLOAD_URL) as resp:
            resp.raise_for_status()
            total_size = int(resp.headers.get("content-length", 0))
            
            with Progress() as progress:
                task = progress.add_task("Downloading...", total=total_size)
                
                with open(output_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        progress.update(task, advance=len(chunk))
    
    console.print(f"[green]Downloaded to {output_path}[/green]")
    return output_path


def extract_mn_pt_providers(
    zip_path: Path,
    target_states: list[str] = None,
    target_taxonomies: list[str] = None,
) -> list[dict]:
    """
    Extract MN Physical Therapy providers from NPPES ZIP file.
    
    Args:
        zip_path: Path to NPPES ZIP file
        target_states: List of state codes to filter (default: ["MN"])
        target_taxonomies: List of taxonomy codes (default: PT codes)
    
    Returns:
        List of provider dicts with NPI, name, address, taxonomy info
    """
    target_states = target_states or ["MN"]
    target_taxonomies = target_taxonomies or list(PT_TAXONOMY_CODES.keys())
    
    console.print(f"[bold]Extracting providers for states: {target_states}[/bold]")
    console.print(f"Taxonomy codes: {target_taxonomies}")
    
    providers = []
    
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the main NPI file (CSV)
        csv_files = [n for n in zf.namelist() if n.endswith(".csv") and "npidata" in n.lower()]
        if not csv_files:
            raise ValueError("No NPI data CSV found in ZIP file")
        
        csv_filename = csv_files[0]
        console.print(f"Processing: {csv_filename}")
        
        with zf.open(csv_filename) as f:
            # Read as text
            text_wrapper = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
            reader = csv.DictReader(text_wrapper)
            
            row_count = 0
            with Progress() as progress:
                task = progress.add_task("Scanning providers...", total=None)
                
                for row in reader:
                    row_count += 1
                    if row_count % 100000 == 0:
                        progress.update(task, description=f"Scanned {row_count:,} rows, found {len(providers):,} matches")
                    
                    # Check state (primary practice location)
                    state = row.get("Provider Business Practice Location Address State Name", "")
                    if state not in target_states:
                        continue
                    
                    # Check taxonomy codes (up to 15 taxonomy columns)
                    has_pt_taxonomy = False
                    taxonomy_code = None
                    taxonomy_desc = None
                    
                    for i in range(1, 16):
                        tax_col = f"Healthcare Provider Taxonomy Code_{i}"
                        tax_code = row.get(tax_col, "")
                        if tax_code in target_taxonomies:
                            has_pt_taxonomy = True
                            taxonomy_code = tax_code
                            taxonomy_desc = PT_TAXONOMY_CODES.get(tax_code, "")
                            break
                    
                    if not has_pt_taxonomy:
                        continue
                    
                    # Extract provider info
                    npi = row.get("NPI", "")
                    if not npi:
                        continue
                    
                    # Build name (individual or organization)
                    entity_type = row.get("Entity Type Code", "")
                    if entity_type == "1":  # Individual
                        first = row.get("Provider First Name", "")
                        last = row.get("Provider Last Name (Legal Name)", "")
                        name = f"{first} {last}".strip()
                    else:  # Organization
                        name = row.get("Provider Organization Name (Legal Business Name)", "")
                    
                    providers.append({
                        "npi": npi,
                        "provider_name": name,
                        "provider_type": "Individual" if entity_type == "1" else "Organization",
                        "taxonomy_code": taxonomy_code,
                        "taxonomy_desc": taxonomy_desc,
                        "address_line1": row.get("Provider First Line Business Practice Location Address", ""),
                        "city": row.get("Provider Business Practice Location Address City Name", ""),
                        "state": state,
                        "zip": row.get("Provider Business Practice Location Address Postal Code", "")[:5],
                        "phone": row.get("Provider Business Practice Location Address Telephone Number", ""),
                    })
    
    console.print(f"[green]Found {len(providers):,} PT providers in {target_states}[/green]")
    return providers


def load_providers_to_db(providers: list[dict], db: RatesDatabase = None):
    """Load providers into the nppes_providers table."""
    db = db or RatesDatabase()
    
    console.print(f"[bold]Loading {len(providers):,} providers into database...[/bold]")
    
    # Clear existing data
    db.conn.execute("DELETE FROM nppes_providers")
    
    # Insert in batches
    batch_size = 5000
    for i in range(0, len(providers), batch_size):
        batch = providers[i:i + batch_size]
        values = [
            (
                p["npi"],
                p["provider_name"],
                p["provider_type"],
                p["taxonomy_code"],
                p["taxonomy_desc"],
                p["address_line1"],
                p["city"],
                p["state"],
                p["zip"],
                p["phone"],
            )
            for p in batch
        ]
        
        db.conn.executemany(
            """
            INSERT INTO nppes_providers (
                npi, provider_name, provider_type, taxonomy_code, taxonomy_desc,
                address_line1, city, state, zip, phone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
    
    console.print(f"[green]Loaded {len(providers):,} providers[/green]")


def setup_nppes(
    states: list[str] = None,
    force_download: bool = False,
    use_api: bool = True,
) -> int:
    """
    Load MN PT providers into database.
    
    Args:
        states: Target states (default: ["MN"])
        force_download: Re-download bulk file even if cached
        use_api: Use NPPES API (faster) instead of bulk file
    
    Returns:
        Number of providers loaded
    """
    states = states or ["MN"]
    
    if use_api:
        # Use API - faster for single state
        all_providers = []
        for state in states:
            providers = fetch_providers_via_api(state=state)
            all_providers.extend(providers)
        
        # Load to database
        db = RatesDatabase()
        load_providers_to_db(all_providers, db)
        db.close()
        return len(all_providers)
    
    # Fallback: bulk file download
    data_dir = get_data_dir()
    zip_path = data_dir / "nppes_full.zip"
    
    # Download if needed
    if not zip_path.exists() or force_download:
        download_nppes_file(zip_path)
    else:
        console.print(f"[dim]Using cached NPPES file: {zip_path}[/dim]")
    
    # Extract providers
    providers = extract_mn_pt_providers(zip_path, target_states=states)
    
    # Load to database
    db = RatesDatabase()
    load_providers_to_db(providers, db)
    db.close()
    
    return len(providers)


def get_mn_npi_set(db: RatesDatabase = None) -> set[str]:
    """Get set of MN PT provider NPIs from database."""
    db = db or RatesDatabase()
    result = db.query_df("SELECT npi FROM nppes_providers")
    return set(result["npi"].tolist())


if __name__ == "__main__":
    setup_nppes()
