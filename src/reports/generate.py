#!/usr/bin/env python3
"""Generate market rate reports."""

from pathlib import Path
from datetime import datetime

from .market_benchmark import MarketBenchmarkReport
from .fair_market_value import FairMarketValueReport
from .rate_dimensions import RateDimensionsReport, RateTimeSeriesReport
from .pdf_generator import generate_market_rate_report, generate_rate_reference_card


def generate_reports(output_dir: Path, generate_pdfs: bool = True):
    """
    Generate market rate reports.
    
    Args:
        output_dir: Base output directory
        generate_pdfs: Whether to generate PDF reports alongside images
    
    Returns:
        Tuple of (image_dirs, pdf_paths)
    """
    output_dir = Path(output_dir)
    
    image_dirs = []
    pdf_paths = []
    
    # 1. Market Rate Report (main analysis)
    print("\n📊 Generating Market Rate Report...")
    report = MarketBenchmarkReport()
    image_dir = output_dir / "market_rates"
    report.generate(image_dir)
    image_dirs.append(image_dir)
    print(f"   ✓ Images: {image_dir}")
    
    if generate_pdfs:
        pdf_path = generate_market_rate_report(output_dir, image_dir)
        pdf_paths.append(pdf_path)
        print(f"   ✓ PDF: {pdf_path}")
    
    # 2. Rate Reference Card (quick lookup)
    print("\n📋 Generating Rate Reference Card...")
    report = FairMarketValueReport()
    image_dir = output_dir / "reference_card"
    report.generate(image_dir)
    image_dirs.append(image_dir)
    print(f"   ✓ Images: {image_dir}")
    
    if generate_pdfs:
        pdf_path = generate_rate_reference_card(output_dir, image_dir)
        pdf_paths.append(pdf_path)
        print(f"   ✓ PDF: {pdf_path}")
    
    # 3. Rate Dimensions (by negotiated_type, billing_class, place_of_service)
    print("\n📈 Generating Rate Dimensions Report...")
    report = RateDimensionsReport()
    image_dir = output_dir / "rate_dimensions"
    report.generate(image_dir)
    image_dirs.append(image_dir)
    print(f"   ✓ Images: {image_dir}")
    
    # 4. Rate Time Series
    print("\n📅 Generating Rate Time Series Report...")
    report = RateTimeSeriesReport()
    image_dir = output_dir / "time_series"
    report.generate(image_dir)
    image_dirs.append(image_dir)
    print(f"   ✓ Images: {image_dir}")
    
    print("\n" + "=" * 40)
    print("GENERATED:")
    for d in image_dirs:
        print(f"  • {d}/")
    for p in pdf_paths:
        print(f"  • {p}")
    print("=" * 40)
    
    return image_dirs, pdf_paths


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    
    output = Path("reports") / datetime.now().strftime("%Y-%m-%d")
    generate_reports(output)
