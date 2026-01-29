# Agent Instructions

## Project Overview

This is a PT (physical therapy) rate analysis tool for the Saint Paul metro area. It ingests Transparency in Coverage (TiC) data from health insurance payers and generates competitive analysis reports.

## Key Commands

```bash
# Activate virtual environment
source .venv/bin/activate

# CLI commands (preferred)
tic init          # Interactive setup for NPIs and location
tic ingest        # Run full data ingestion pipeline
tic report        # Generate competitive analysis reports
tic status        # Show database stats

# Manual scripts (alternative)
python scripts/load_mn_nppes.py
python scripts/ingest_healthpartners.py
python scripts/ingest_ucare.py
python scripts/ingest_bcbs_local.py
python scripts/generate_competitive_report.py

# Check database directly
python -c "from src.storage import RatesDatabase; db = RatesDatabase(); print(db.get_rate_stats()); db.close()"
```

## Important Files

- `scripts/generate_competitive_report.py` - Main report generator (5 reports)
- `scripts/ingest_bcbs_local.py` - BCBS MN data ingestion
- `src/storage.py` - DuckDB database layer (RatesDatabase, RateRecord)
- `src/config.py` - Configuration loader (load_cpt_codes, get_target_states)
- `config/cpt_codes.yaml` - PT CPT codes to extract

## Database Schema

**rates** table:
- `payer_name`, `billing_code`, `negotiated_rate`, `npi`, `tin`
- `billing_code_type`, `negotiated_type`, `billing_class`, `place_of_service`
- `last_updated`, `file_source`, `ingested_at`

**nppes_providers** table:
- `npi`, `provider_name`, `provider_type` (1=individual, 2=organization)
- `city`, `state`, `zip`, `taxonomy_code`

**ingestion_log** table:
- Tracks which files have been processed

## Key NPIs

Configure target NPIs in `scripts/generate_competitive_report.py`:
- `PRIMARY_TYPE2_NPI` - Primary clinic (Type 2)
- `PRIMARY_TYPE1_NPI_A` - Primary individual A (Type 1)
- `PRIMARY_TYPE1_NPI_B` - Primary individual B (Type 1)

## Payer-Specific Notes

- **HealthPartners**: Bills by Type 1 NPI only
- **UCare**: Bills by Type 2 NPI only
- **BCBS MN**: Uses both Type 1 and Type 2 NPIs; requires provider group mapping
- **UHC**: NPIs exist in files but are not linked to PT rate entries (no data available)

## Report Output

Reports are generated as markdown files in `reports/`:
- `local_competitor_rates_by_clinic.md`
- `local_competitor_rates_by_individual.md`
- `median_payer_rates_by_cpt_code.md`
- `underlying_data_summary.md`
- `renegotiation_opportunities.md`
