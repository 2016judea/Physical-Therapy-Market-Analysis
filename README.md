# Physical Therapy Payer Reimbursement Analysis

Aggregate payer reimbursement data from Transparency in Coverage (TiC) machine-readable files for PT contract negotiation analysis.

**Geographic scope:** Minnesota only

## Quick Start

```bash
# Install dependencies
pip install -e .

# Check configured payers
tic payers

# Check configured CPT codes
tic cpts

# Ingest data from all enabled payers
tic ingest

# Check database status
tic status

# View rate summary
tic summary

# Compare payers for a specific CPT
tic compare-payers 97110

# Run custom SQL query
tic query "SELECT * FROM rates WHERE billing_code = '97110' LIMIT 10"
```

## Configuration

- `config/payers.yaml` - Payer index URLs (need to be verified/updated)
- `config/cpt_codes.yaml` - PT CPT codes to extract

## Data Location

- `data/rates.duckdb` - Main database
- `data/raw/` - Cached raw files (optional)

## Architecture

See [DESIGN.md](DESIGN.md) for full architecture documentation.

```
Payer Index → Downloader → Stream Parser → DuckDB
                              ↓
                      (filter CPTs + MN)
```

## Key Commands

| Command                    | Description                  |
| -------------------------- | ---------------------------- |
| `tic status`               | Show database statistics     |
| `tic payers`               | List configured payers       |
| `tic cpts`                 | List PT CPT codes            |
| `tic ingest`               | Download and parse MRF files |
| `tic summary`              | Rate statistics by CPT       |
| `tic compare-payers <cpt>` | Compare payers for a CPT     |
| `tic query "<sql>"`        | Run custom SQL               |

## Next Steps

1. Verify payer index URLs in `config/payers.yaml`
2. Run `tic ingest --payer "UnitedHealthcare" --max-files 1` to test
3. Enrich with NPPES data for provider names/locations
