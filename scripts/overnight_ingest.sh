#!/bin/bash
# Overnight ingestion script for all enabled payers
# Run with: nohup ./scripts/overnight_ingest.sh > logs/overnight_$(date +%Y%m%d).log 2>&1 &

cd "$(dirname "$0")/.."
source .venv/bin/activate

mkdir -p logs

echo "======================================"
echo "Starting overnight ingestion: $(date)"
echo "======================================"

# Ingest all enabled payers
python -m src.cli ingest

echo ""
echo "======================================"
echo "Ingestion complete: $(date)"
echo "======================================"

# Show final stats
python -m src.cli status

# Generate reports
echo ""
echo "Generating reports..."
python -m src.cli reports --output "reports/$(date +%Y-%m-%d)"

echo ""
echo "Done! Check reports/ for output."
