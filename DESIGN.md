# TiC Reimbursement Data Pipeline - Design Document

## Goal
Build a local data pipeline to aggregate payer reimbursement data from Transparency in Coverage (TiC) machine-readable files, filtered to PT-relevant CPT codes, for contract negotiation analysis.

---

## Architecture Overview

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐     ┌───────────────┐
│  Payer Index    │────▶│  Downloader  │────▶│  Stream Parser  │────▶│  DuckDB/      │
│  Files (JSON)   │     │  (chunked)   │     │  (filter CPTs)  │     │  Parquet      │
└─────────────────┘     └──────────────┘     └─────────────────┘     └───────────────┘
                                                                            │
                                                                            ▼
                                                                     ┌───────────────┐
                                                                     │  Analysis     │
                                                                     │  (SQL/Python) │
                                                                     └───────────────┘
```

---

## Components

### 1. Payer Registry (`payers.yaml`)
A config file listing payers to scrape:

```yaml
payers:
  - name: "UnitedHealthcare"
    index_url: "https://transparency-in-coverage.uhc.com/..."
    
  - name: "Aetna"
    index_url: "https://health1.aetna.com/..."
    
  - name: "Cigna"
    index_url: "https://www.cigna.com/..."
```

**Action item:** We'll need to manually locate index URLs for target payers.

---

### 2. CPT Filter List (`cpt_codes.yaml`)
PT-relevant codes to extract:

```yaml
cpt_codes:
  # Therapeutic exercises
  - "97110"  # Therapeutic exercises
  - "97112"  # Neuromuscular re-education
  - "97116"  # Gait training
  - "97140"  # Manual therapy
  - "97530"  # Therapeutic activities
  - "97535"  # Self-care/home management training
  - "97542"  # Wheelchair management
  - "97750"  # Physical performance test
  - "97760"  # Orthotic training
  - "97761"  # Prosthetic training
  
  # Evaluations
  - "97161"  # PT eval, low complexity
  - "97162"  # PT eval, moderate complexity
  - "97163"  # PT eval, high complexity
  - "97164"  # PT re-evaluation
  
  # Modalities
  - "97010"  # Hot/cold packs
  - "97012"  # Mechanical traction
  - "97014"  # Electrical stimulation (unattended)
  - "97016"  # Vasopneumatic devices
  - "97018"  # Paraffin bath
  - "97022"  # Whirlpool
  - "97032"  # Electrical stimulation (attended)
  - "97033"  # Iontophoresis
  - "97034"  # Contrast baths
  - "97035"  # Ultrasound
  - "97036"  # Hubbard tank
  
  # Dry needling (if you use it)
  - "20560"  # Dry needling, 1-2 muscles
  - "20561"  # Dry needling, 3+ muscles
```

---

### 3. Downloader (`src/downloader.py`)

**Challenges:**
- Index files point to thousands of sub-files (often 1 per provider or EIN)
- Individual files can be 1-50GB compressed
- Files are gzipped JSON

**Strategy:**
```
1. Fetch index file → get list of in-network file URLs
2. For each file URL:
   a. Stream download (don't load into memory)
   b. Decompress on-the-fly (gzip stream)
   c. Pass to parser immediately (no intermediate storage of full file)
   d. Track progress in SQLite for resumability
```

**Key decisions:**
- [ ] **Option A:** Download full files, then parse (needs ~500GB+ temp space)
- [x] **Option B:** Stream + parse in single pass (recommended - lower disk, slower)

---

### 4. Stream Parser (`src/parser.py`)

TiC in-network files have this structure:
```json
{
  "reporting_entity_name": "Payer Name",
  "reporting_entity_type": "health insurance issuer",
  "last_updated_on": "2024-01-01",
  "in_network": [
    {
      "negotiation_arrangement": "ffs",
      "billing_code_type": "CPT",
      "billing_code": "97110",
      "negotiated_rates": [
        {
          "negotiated_prices": [
            {
              "negotiated_rate": 45.00,
              "negotiated_type": "negotiated",
              "billing_class": "professional",
              "service_code": ["11"],  # place of service
              "expiration_date": "9999-12-31"
            }
          ],
          "provider_references": [123, 456]  # indexes into provider array
        }
      ]
    }
  ],
  "provider_references": [
    {
      "provider_group_id": 123,
      "provider_groups": [
        {
          "npi": ["1234567890"],
          "tin": {"type": "ein", "value": "12-3456789"}
        }
      ]
    }
  ]
}
```

**Parser approach:**
1. Use `ijson` for streaming JSON parse (never loads full file)
2. Filter to only CPT codes in our list
3. Resolve provider references to NPIs
4. Emit normalized rows

---

### 5. Storage Layer

**Recommended: DuckDB + Parquet**

Why DuckDB over SQLite:
- Columnar storage = fast aggregations
- Native Parquet support
- Handles billions of rows locally
- SQL interface for analysis

**Schema:**
```sql
CREATE TABLE rates (
    payer_name VARCHAR,
    last_updated DATE,
    billing_code VARCHAR,        -- CPT code
    billing_code_type VARCHAR,   -- CPT, HCPCS
    negotiated_rate DECIMAL(10,2),
    negotiated_type VARCHAR,     -- negotiated, derived, fee schedule
    billing_class VARCHAR,       -- professional, institutional
    place_of_service VARCHAR,
    npi VARCHAR,
    tin VARCHAR,
    provider_name VARCHAR,       -- enriched later via NPPES
    provider_state VARCHAR,      -- enriched later via NPPES
    provider_zip VARCHAR,        -- enriched later via NPPES
    file_source VARCHAR,
    ingested_at TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX idx_cpt ON rates(billing_code);
CREATE INDEX idx_npi ON rates(npi);
CREATE INDEX idx_payer ON rates(payer_name);
CREATE INDEX idx_state ON rates(provider_state);
```

---

### 6. NPI Enrichment (`src/enrich.py`)

The MRF files only contain NPI numbers. To get provider names/locations:

1. Download NPPES monthly file (~8GB CSV)
   - https://download.cms.gov/nppes/NPI_Files.html
2. Load into DuckDB
3. Join to enrich rates with:
   - Provider name
   - Practice address (city, state, zip)
   - Taxonomy (to confirm PT)

---

## Directory Structure

```
mason_pt_data/
├── DESIGN.md
├── README.md
├── config/
│   ├── payers.yaml          # Payer index URLs
│   └── cpt_codes.yaml       # PT CPT codes to filter
├── src/
│   ├── __init__.py
│   ├── downloader.py        # Fetch + stream files
│   ├── parser.py            # Stream parse JSON
│   ├── storage.py           # DuckDB operations
│   ├── enrich.py            # NPPES enrichment
│   └── cli.py               # CLI entrypoint
├── data/
│   ├── raw/                 # (optional) cached raw files
│   ├── rates.duckdb         # Main database
│   └── nppes/               # NPPES lookup data
├── notebooks/
│   └── analysis.ipynb       # Jupyter for exploration
├── tests/
├── pyproject.toml
└── requirements.txt
```

---

## CLI Interface

```bash
# Add a payer
tic add-payer "UnitedHealthcare" "https://..."

# Run ingestion for all payers
tic ingest --payers all

# Run ingestion for specific payer
tic ingest --payer "UnitedHealthcare"

# Enrich with NPPES data
tic enrich

# Query example
tic query "SELECT * FROM rates WHERE billing_code = '97110' LIMIT 10"
```

---

## Data Volume Estimates (Minnesota Only)

| Metric | Estimate |
|--------|----------|
| Payers to ingest | 5-10 major payers |
| Raw file size per payer | 50-500 GB (national) |
| Filtered to PT CPTs + MN | ~100-500 MB per payer |
| Total DuckDB size | ~2-5 GB |
| Rows (rates) | 1-10 million |

**Geographic filter:** Minnesota providers only (via NPPES lookup or provider_reference addresses in MRF files)

---

## Implementation Phases

### Phase 1: Core Pipeline (MVP)
- [ ] Payer config + CPT filter config
- [ ] Streaming downloader (single payer)
- [ ] Streaming JSON parser with CPT filter
- [ ] DuckDB storage
- [ ] Basic CLI

### Phase 2: Scale + Robustness
- [ ] Resumable downloads (track progress)
- [ ] Multiple payer support
- [ ] Error handling + retries
- [ ] Logging + monitoring

### Phase 3: Enrichment
- [ ] NPPES data download + load
- [ ] Join NPI → provider name/location
- [ ] Add geographic filtering

### Phase 4: Analysis Layer
- [ ] Jupyter notebook templates
- [ ] Pre-built queries (median by CPT, payer comparison, geographic variance)
- [ ] Export to CSV/Excel for sharing

---

## Open Questions for You

1. **Target payers:** Which specific payers do you want to start with? (UHC, BCBS of [state], Aetna, Cigna?)

2. **Geographic scope:** National, or specific states/regions?

3. **Additional CPT codes:** Any codes I missed that you commonly bill?

4. **Historical data:** Do you want to track rate changes over time (monthly snapshots)?

5. **Workers' comp:** TiC doesn't cover workers' comp. Do you want to integrate other data sources for that?

---

## Tech Stack

- **Python 3.11+**
- **ijson** - streaming JSON parser
- **httpx** - async HTTP client
- **duckdb** - analytical database
- **typer** - CLI framework
- **pydantic** - config validation
- **rich** - terminal UI/progress bars

---

## Next Steps

Once you approve this design, I'll implement Phase 1 in this order:
1. Project scaffolding (pyproject.toml, directory structure)
2. Config files (payers.yaml, cpt_codes.yaml)
3. Streaming parser (most complex piece)
4. DuckDB storage layer
5. Downloader with streaming
6. CLI wrapper
