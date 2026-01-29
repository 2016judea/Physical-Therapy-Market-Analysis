# Physical Therapy Market Rate Analysis

Competitive rate intelligence for PT contract negotiations using Transparency in Coverage data.

---

## Overview

This tool ingests negotiated rate data from health insurance payers and generates competitive analysis reports for physical therapy providers in your local market.

**What it does:**
- Extracts PT-specific rates from payer Transparency in Coverage (TiC) files
- Compares your practice's rates against local competitors
- Identifies renegotiation opportunities where rates are below market median
- Generates markdown reports for easy sharing

---

## Architecture

```mermaid
flowchart LR
    subgraph Input
        A[("Payer TiC Files")]
        B[("NPPES Provider Data")]
    end
    
    subgraph Processing
        C["Ingestion Scripts"]
        D[("DuckDB")]
    end
    
    subgraph Output
        E["Competitive Reports"]
    end
    
    A --> C
    B --> C
    C --> D
    D --> E
```

---

## Data Flow

```mermaid
flowchart TD
    subgraph Payers
        HP["HealthPartners"]
        UC["UCare"]
        BCBS["BCBS Minnesota"]
    end
    
    subgraph Database
        DB[("DuckDB")]
    end
    
    subgraph Reports
        R1["Clinic Comparison"]
        R2["Individual PT Comparison"]
        R3["Payer Rate Summary"]
        R4["Data Coverage"]
        R5["Renegotiation Opps"]
    end
    
    HP --> DB
    UC --> DB
    BCBS --> DB
    DB --> R1
    DB --> R2
    DB --> R3
    DB --> R4
    DB --> R5
```

---

## Quick Start

```bash
# Clone and setup
git clone https://github.com/2016judea/Physical-Therapy-Market-Analysis.git
cd Physical-Therapy-Market-Analysis

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install package
pip install -e .

# Interactive setup - configure your NPIs and location
tic init

# Run full ingestion pipeline
tic ingest

# Generate competitive analysis reports
tic report

# Check status anytime
tic status
```

---

## Project Structure

```
pt_rate_analysis/
├── README.md
├── AGENTS.md                 # AI agent instructions
├── pyproject.toml
│
├── config/
│   ├── cpt_codes.yaml        # PT CPT codes to extract
│   └── payers.yaml           # Payer configurations
│
├── scripts/
│   ├── generate_competitive_report.py   # Main report generator
│   ├── ingest_healthpartners.py         # HealthPartners data ingestion
│   ├── ingest_ucare.py                  # UCare data ingestion
│   ├── ingest_bcbs_local.py             # BCBS data ingestion
│   ├── scan_bcbs_groups.py              # BCBS provider group scanner
│   └── load_mn_nppes.py                 # Load provider NPIs
│
├── src/
│   ├── config.py             # Configuration loader
│   ├── parser.py             # TiC file parser
│   └── storage.py            # DuckDB storage layer
│
├── data/                     # (gitignored)
│   ├── rates.duckdb          # Main database
│   └── bcbs_npi_to_groups.json
│
├── reports/                  # (gitignored) Generated reports
└── logs/                     # (gitignored) Ingestion logs
```

---

## Reports Generated

| Report | Description |
|--------|-------------|
| `local_competitor_rates_by_clinic.md` | Primary clinic vs competitor clinics (Type 2 NPIs) |
| `local_competitor_rates_by_individual.md` | Primary individuals vs individual PTs (Type 1 NPIs) |
| `median_payer_rates_by_cpt_code.md` | Rate summary across all payers |
| `underlying_data_summary.md` | Data coverage and limitations |
| `renegotiation_opportunities.md` | Rates below market median |

---

## Payer Coverage

| Payer | Status | Notes |
|-------|--------|-------|
| BCBS Minnesota | ✅ Supported | Complex ingestion via provider group mapping |
| HealthPartners | ✅ Supported | Direct ZIP downloads, Type 1 NPIs only |
| UCare | ✅ Supported | TOC index file, Type 2 NPIs only |
| UnitedHealthcare | ⚠️ No usable data | NPIs exist in files but not linked to PT rate entries |
| Aetna | ❌ Not supported | National payer with HealthSparq portal; complex file structure |
| Medica | ❌ Not supported | HealthSparq portal with bot protection |
| Cigna | ❌ Not supported | Browser automation required; CAPTCHA protection |
| Humana | ❌ Not supported | Bot protection and CAPTCHA requirements |
| Medicare/Medicaid | ❌ Not supported | CMS/DHS publish separately in different formats |

---

## Configuration

Run `tic init` to configure your practice NPIs and geographic area interactively. This stores your settings in `data/user_config.json`:

- **Clinic NPI** - Your practice's Type 2 (organization) NPI
- **Individual NPIs** - Type 1 NPIs for individual providers
- **Zip Prefixes** - 3-digit prefixes to filter local competitors (e.g., `551` for Saint Paul, `554` for Minneapolis)
- **Practice Name** - Used in report headers

---

## Database Schema

```mermaid
erDiagram
    rates {
        int id PK
        string payer_name
        date last_updated
        string billing_code
        decimal negotiated_rate
        string npi FK
        string tin
        string file_source
        timestamp ingested_at
    }
    
    nppes_providers {
        string npi PK
        string provider_name
        string provider_type
        string city
        string state
        string zip
    }
    
    ingestion_log {
        int id PK
        string payer_name
        string file_url
        string status
        int records_inserted
        timestamp completed_at
    }
    
    rates }o--|| nppes_providers : "npi"
```

---

## CLI Reference

The `tic` command provides a unified interface for the entire pipeline:

| Command | Description |
|---------|-------------|
| `tic init` | Interactive setup - configure NPIs and zip prefixes |
| `tic ingest` | Run full data ingestion for all payers |
| `tic ingest -p healthpartners` | Ingest specific payer only |
| `tic ingest --skip-bcbs` | Skip BCBS (faster, excludes slow 2-4hr ingestion) |
| `tic report` | Generate competitive analysis reports |
| `tic status` | Show database statistics and configuration |
| `tic reset` | Delete all data and start fresh |

### Example Session

```bash
$ tic init
PT Rate Analysis - Initial Setup

Primary clinic NPI (Type 2): 1234567890
Enter individual provider NPIs (empty line to finish):
  Add NPI: 1111111111
  Add NPI: 2222222222
  Add NPI: 
Zip prefixes (comma-separated) [551]: 551, 553
Practice name (for reports) [Our Practice]: ABC Physical Therapy

✓ Configuration saved!
Load local provider data from NPPES now? [Y/n]: y
```

---

## Manual Data Ingestion

If you prefer running scripts directly instead of the CLI:

### Step 1: Load Provider NPIs
```bash
python scripts/load_mn_nppes.py
```
Fetches physical therapists from the NPPES API for your configured zip prefixes and loads them into the database.

### Step 2: Ingest Payer Data

#### HealthPartners (fastest)
```bash
python scripts/ingest_healthpartners.py
```
Downloads ZIP files directly from HealthPartners. Type 1 NPIs only.

#### UCare (fast)
```bash
python scripts/ingest_ucare.py
```
Fetches TOC index and downloads MRF files. Type 2 NPIs only.

#### BCBS Minnesota (slow, ~2-4 hours)
```bash
# One-time: scan provider groups to map NPIs to group IDs
python scripts/scan_bcbs_groups.py

# Ingest rates from Local files
nohup python scripts/ingest_bcbs_local.py > logs/bcbs_local.log 2>&1 &
tail -f logs/bcbs_local.log
```
BCBS requires a two-phase approach: first scanning provider group files to find which groups contain your NPIs, then ingesting rates from "Local" network files.

### Step 3: Generate Reports
```bash
python scripts/generate_competitive_report.py
```

---

## Sample Output

### Clinic Comparison (Type 2 NPIs)
```
CPT     Description           Your Rate  Rank   Lowest                   Highest
────────────────────────────────────────────────────────────────────────────────
97110   Therapeutic exercises $XX.XX     3/12   $XX.XX (Competitor A)    $XX.XX (Competitor B)
97140   Manual therapy        $XX.XX     1/12   $XX.XX (Competitor C)    $XX.XX (Your Clinic)
```

### Individual PT Comparison (Type 1 NPIs)
```
CPT     Description           Indiv A    Rank   Indiv B    Rank   Lowest         Highest
─────────────────────────────────────────────────────────────────────────────────────────
97161   PT eval low           $XX.XX     5/42   $XX.XX     8/42   $XX.XX         $XX.XX
97530   Therapeutic activities$XX.XX     2/42   $XX.XX     3/42   $XX.XX         $XX.XX
```

### Payer Rate Summary
```
CPT     Description           BCBS Minnesota  HealthPartners  UCare
───────────────────────────────────────────────────────────────────
97110   Therapeutic exercises $XX.XX          $XX.XX          $XX.XX
97140   Manual therapy        $XX.XX          $XX.XX          $XX.XX
97161   PT eval low           $XX.XX          $XX.XX          $XX.XX
```

### Renegotiation Opportunities
```
Payer              CPT     Description            Your Rate   Payer Median   % Below
────────────────────────────────────────────────────────────────────────────────────
BCBS Minnesota     97110   Therapeutic exercises  $XX.XX      $XX.XX         -X.X%
HealthPartners     97161   PT eval low            $XX.XX      $XX.XX         -X.X%
```

---

## Tech Stack

- **Python 3.11+** - Core language
- **DuckDB** - Fast analytical database
- **httpx** - HTTP client for API calls
- **Rich** - Terminal output formatting
- **ijson** - Streaming JSON parser for large files
