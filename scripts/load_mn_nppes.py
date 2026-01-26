#!/usr/bin/env python3
"""Quick NPPES loader for MN PT providers."""

import httpx
import time
from src.storage import RatesDatabase

NPPES_API = "https://npiregistry.cms.hhs.gov/api/"
MN_ZIPS = ["550", "551", "553", "554", "556", "557", "558", "559",
           "560", "561", "562", "563", "564", "565", "566", "567"]
TAXONOMIES = [
    ("225100000X", "Physical Therapist"),
    ("225200000X", "Physical Therapy Assistant"),
]


def fetch_all():
    """Fetch all MN PT providers."""
    providers = {}  # npi -> provider dict
    
    with httpx.Client(timeout=60) as client:
        for tax_code, tax_desc in TAXONOMIES:
            print(f"\nFetching {tax_desc}...")
            
            for zip_prefix in MN_ZIPS:
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
    print("Loading MN PT providers from NPPES API...")
    providers = fetch_all()
    load_to_db(providers)
    print("Done!")
