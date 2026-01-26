"""PDF Report Generator for MN Payer Rate Analysis."""

from pathlib import Path
from datetime import datetime

from fpdf import FPDF

from ..storage import RatesDatabase


class PTReportPDF(FPDF):
    """Custom PDF class for PT reports."""
    
    def __init__(self, title: str):
        super().__init__()
        self.report_title = title
        self.set_auto_page_break(auto=True, margin=15)
        
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, self.report_title, align="L")
        self.cell(0, 8, datetime.now().strftime("%B %d, %Y"), align="R", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(37, 99, 235)
        self.line(10, 16, 200, 16)
        self.ln(3)
        
    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 8, f"Page {self.page_no()}/{{nb}}", align="C")
        
    def add_section(self, title: str):
        self.ln(5)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(37, 99, 235)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)
        
    def add_text(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5, text)
        self.ln(2)
        
    def add_image_full_width(self, image_path: str):
        if Path(image_path).exists():
            self.image(image_path, x=10, w=190)
            self.ln(3)


def generate_market_rate_report(
    output_dir: Path,
    image_dir: Path,
    db: RatesDatabase = None
) -> Path:
    """Generate MN Payer Rate Analysis PDF."""
    db = db or RatesDatabase()
    
    # Get summary stats
    stats = db.query_df("""
        SELECT 
            COUNT(*) as total_rates,
            COUNT(DISTINCT payer_name) as num_payers,
            COUNT(DISTINCT npi) as num_providers
        FROM rates
    """)
    
    payer_list = db.query_df("SELECT DISTINCT payer_name FROM rates ORDER BY payer_name")
    payers = ", ".join(payer_list["payer_name"].tolist())
    
    pdf = PTReportPDF("MN Payer Rate Analysis")
    pdf.alias_nb_pages()
    
    # Page 1: Overview
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(37, 99, 235)
    pdf.cell(0, 12, "Minnesota Payer Rate Analysis", new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 6, f"Payers: {payers}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"{stats['total_rates'].iloc[0]:,} rates from {stats['num_providers'].iloc[0]:,} providers", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)
    
    dashboard_path = image_dir / "00_summary_dashboard.png"
    if dashboard_path.exists():
        pdf.add_image_full_width(str(dashboard_path))
    
    # Page 2: Rate heatmap
    pdf.add_page()
    pdf.add_section("Rates by Payer and CPT Code")
    
    heatmap_path = image_dir / "01_rate_ranges.png"
    if heatmap_path.exists():
        pdf.add_image_full_width(str(heatmap_path))
    
    # Page 3: Distributions
    pdf.add_page()
    pdf.add_section("Rate Distributions by Payer")
    
    dist_path = image_dir / "02_payer_distributions.png"
    if dist_path.exists():
        pdf.add_image_full_width(str(dist_path))
    
    # Page 4: Percentile table
    pdf.add_page()
    pdf.add_section("Detailed Percentiles")
    
    table_path = image_dir / "03_payer_comparison.png"
    if table_path.exists():
        pdf.add_image_full_width(str(table_path))
    
    # Save
    output_path = output_dir / "MN_Payer_Rate_Analysis.pdf"
    pdf.output(str(output_path))
    return output_path


def generate_rate_reference_card(
    output_dir: Path,
    image_dir: Path,
    db: RatesDatabase = None
) -> Path:
    """Generate Rate Reference Card PDF."""
    db = db or RatesDatabase()
    
    total_rates = db.query_df("SELECT COUNT(*) as n FROM rates")["n"].iloc[0]
    
    pdf = PTReportPDF("Rate Reference Card")
    pdf.alias_nb_pages()
    
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(37, 99, 235)
    pdf.cell(0, 10, "PT Rate Reference Card", new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 5, f"Based on {total_rates:,} MN payer rates", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    
    rate_card_path = image_dir / "01_rate_card.png"
    if rate_card_path.exists():
        pdf.add_image_full_width(str(rate_card_path))
    
    pdf.add_page()
    pdf.add_section("Rate Ranges")
    
    ranges_path = image_dir / "02_market_ranges.png"
    if ranges_path.exists():
        pdf.add_image_full_width(str(ranges_path))
    
    pdf.add_page()
    pdf.add_section("Summary")
    
    summary_path = image_dir / "00_case_manager_summary.png"
    if summary_path.exists():
        pdf.add_image_full_width(str(summary_path))
    
    output_path = output_dir / "Rate_Reference_Card.pdf"
    pdf.output(str(output_path))
    return output_path
