#!/usr/bin/env python3
"""
NPPES loader for PT providers.

Loads physical therapists from the NPPES API based on configured zip prefixes.
Uses user_config.json if available, otherwise defaults to all MN zips.

Usage:
    python scripts/load_mn_nppes.py
    python scripts/load_mn_nppes.py --zips 551,553,554
"""

import argparse
import json
import httpx
import time
from pathlib import Path
from src.storage import RatesDatabase
from src.config import get_data_dir

NPPES_API = "https://npiregistry.cms.hhs.gov/api/"

# Default: all Minnesota zip prefixes
DEFAULT_MN_ZIPS = [
    "550", "551", "553", "554", "556", "557", "558", "559",
    "560", "561", "562", "563", "564", "565", "566", "567"
]

TAXONOMIES = [
    ("225100000X", "Physical Therapist"),
    ("225200000X", "Physical Therapy Assistant"),
]


def get_zip_prefixes() -> list[str]:
    """Get zip prefixes from user config or defaults."""
    config_file = get_data_dir() / "user_config.json"
    if config_file.exists():
        with open(config_file) as f:
            config = json.load(f)
        zips = config.get("zip_prefixes", [])
        if zips:
            return zips
    return DEFAULT_MN_ZIPS


def fetch_all(zip_prefixes: list[str] = None):
    """Fetch PT providers for the given zip prefixes."""
    if zip_prefixes is None:
        zip_prefixes = get_zip_prefixes()
    
    providers = {}  # npi -> provider dict
    
    print(f"Searching zip prefixes: {', '.join(zip_prefixes)}")
    
    with httpx.Client(timeout=60) as client:
        for tax_code, tax_desc in TAXONOMIES:
            print(f"\nFetching {tax_desc}...")
            
            for zip_prefix in zip_prefixes:
                skip = 0
                while True:
                    params = {
                        "version": "2.1",
                        "postal_code": f"{zip_prefix}*",
                        "taxonomy_description": tax_desc,
                        "limit": 200,
                        "skip": skip,
                    }
                    
                    try:
                        resp = client.get(NPPES_API, params=params)
                        data = resp.json()
                    except Exception as e:
                        print(f"  Error {zip_prefix}: {e}")
                        break
                    
                    results = data.get("results", [])
                    if not results:
                        break
                    
                    for r in results:
                        npi = r.get("number", "")
                        if npi in providers:
                            continue
                        
                        basic = r.get("basic", {})
                        if r.get("enumeration_type") == "NPI-1":
                            name = f"{basic.get('first_name', '')} {basic.get('last_name', '')}".strip()
                            ptype = "Individual"
                        else:
                            name = basic.get("organization_name", "")
                            ptype = "Organization"
                        
                        addrs = r.get("addresses", [])
                        loc = next((a for a in addrs if a.get("address_purpose") == "LOCATION"), {})
                        
                        taxes = r.get("taxonomies", [])
                        ptax = next((t for t in taxes if t.get("primary")), taxes[0] if taxes else {})
                        
                        providers[npi] = {
                            "npi": npi,
                            "provider_name": name,
                            "provider_type": ptype,
                            "taxonomy_code": ptax.get("code", tax_code),
                            "taxonomy_desc": ptax.get("desc", ""),
                            "address_line1": loc.get("address_1", ""),
                            "city": loc.get("city", ""),
                            "state": loc.get("state", "MN"),
                            "zip": (loc.get("postal_code", "") or "")[:5],
                            "phone": loc.get("telephone_number", ""),
                        }
                    
                    print(f"  ZIP {zip_prefix}: {len(providers)} total", end="\r")
                    
                    if len(results) < 200:
                        break
                    skip += 200
                    time.sleep(0.15)
            
            print(f"  Collected {len(providers)} providers")
    
    return list(providers.values())


def load_to_db(providers):
    """Load providers into database."""
    db = RatesDatabase()
    
    # Clear existing
    db.conn.execute("DELETE FROM nppes_providers")
    
    # Insert
    values = [
        (p["npi"], p["provider_name"], p["provider_type"], p["taxonomy_code"],
         p["taxonomy_desc"], p["address_line1"], p["city"], p["state"], 
         p["zip"], p["phone"])
        for p in providers
    ]
    
    db.conn.executemany(
        """INSERT INTO nppes_providers 
           (npi, provider_name, provider_type, taxonomy_code, taxonomy_desc,
            address_line1, city, state, zip, phone)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        values
    )
    
    db.close()
    print(f"\nLoaded {len(providers)} providers to database")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load PT providers from NPPES")
    parser.add_argument(
        "--zips", 
        type=str, 
        help="Comma-separated zip prefixes (e.g., 551,553,554)"
    )
    args = parser.parse_args()
    
    zip_prefixes = None
    if args.zips:
        zip_prefixes = [z.strip() for z in args.zips.split(",")]
    
    print("Loading PT providers from NPPES API...")
    providers = fetch_all(zip_prefixes)
    load_to_db(providers)
    print("Done!")
