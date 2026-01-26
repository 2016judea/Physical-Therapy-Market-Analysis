"""Base class for reports."""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from ..storage import RatesDatabase


class BaseReport(ABC):
    """Base class for all reports."""
    
    # Common PT CPT codes grouped by category (short descriptions for reports)
    CPT_CATEGORIES = {
        "Evaluations": {
            "97161": "Eval Low",
            "97162": "Eval Moderate", 
            "97163": "Eval High",
            "97164": "Re-eval",
        },
        "Therapeutic Procedures": {
            "97110": "Ther Exercise",
            "97112": "Neuro Re-ed",
            "97116": "Gait Training",
            "97140": "Manual Therapy",
            "97530": "Ther Activities",
            "97535": "Self-Care",
            "97542": "Wheelchair",
        },
        "Modalities": {
            "97010": "Hot/Cold",
            "97032": "E-Stim",
            "97035": "Ultrasound",
        },
        "Specialty": {
            "97113": "Aquatic",
            "97150": "Group",
            "20560": "Dry Needle 1-2",
            "20561": "Dry Needle 3+",
        },
    }
    
    # High-volume codes typically used in analysis
    KEY_CODES = ["97110", "97140", "97530", "97161", "97162", "97163"]
    
    def __init__(self, db: Optional[RatesDatabase] = None):
        self.db = db or RatesDatabase()
        self.generated_at = datetime.now()
        
        # Set up matplotlib style
        plt.style.use('seaborn-v0_8-whitegrid')
        sns.set_palette("husl")
        
        # Color scheme
        self.colors = {
            "primary": "#2563eb",      # Blue
            "secondary": "#10b981",    # Green
            "accent": "#f59e0b",       # Amber
            "danger": "#ef4444",       # Red
            "neutral": "#6b7280",      # Gray
            "your_rate": "#7c3aed",    # Purple (for user's rate)
        }
    
    @property
    @abstractmethod
    def title(self) -> str:
        """Report title."""
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        """Report description."""
        pass
    
    @abstractmethod
    def generate(self, output_dir: Path, **kwargs) -> Path:
        """Generate the report and return path to output."""
        pass
    
    def get_market_stats(self, cpt_codes: list[str] = None) -> pd.DataFrame:
        """Get market statistics for CPT codes."""
        codes = cpt_codes or self.KEY_CODES
        code_list = "', '".join(codes)
        
        query = f"""
            SELECT 
                billing_code as cpt_code,
                COUNT(*) as sample_size,
                COUNT(DISTINCT npi) as provider_count,
                ROUND(MIN(negotiated_rate), 2) as min_rate,
                ROUND(PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p10,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as median,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p75,
                ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p90,
                ROUND(MAX(negotiated_rate), 2) as max_rate,
                ROUND(AVG(negotiated_rate), 2) as mean
            FROM rates
            WHERE billing_code IN ('{code_list}')
              AND negotiated_rate > 0
            GROUP BY billing_code
            ORDER BY billing_code
        """
        return self.db.query_df(query)
    
    def get_payer_comparison(self, cpt_code: str) -> pd.DataFrame:
        """Get payer-by-payer comparison for a CPT code."""
        query = f"""
            SELECT 
                payer_name,
                COUNT(*) as sample_size,
                COUNT(DISTINCT npi) as providers,
                ROUND(MIN(negotiated_rate), 2) as min_rate,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as median,
                ROUND(MAX(negotiated_rate), 2) as max_rate
            FROM rates
            WHERE billing_code = '{cpt_code}'
              AND negotiated_rate > 0
            GROUP BY payer_name
            ORDER BY median DESC
        """
        return self.db.query_df(query)
    
    def get_rate_distribution(self, cpt_code: str) -> pd.DataFrame:
        """Get rate distribution for a CPT code."""
        query = f"""
            SELECT negotiated_rate as rate
            FROM rates
            WHERE billing_code = '{cpt_code}'
              AND negotiated_rate > 0
        """
        return self.db.query_df(query)
    
    def add_cpt_description(self, df: pd.DataFrame, code_col: str = "cpt_code") -> pd.DataFrame:
        """Add CPT descriptions to a dataframe."""
        desc_map = {}
        for category, codes in self.CPT_CATEGORIES.items():
            desc_map.update(codes)
        
        df = df.copy()
        df["description"] = df[code_col].map(desc_map).fillna("Unknown")
        return df
    
    def save_figure(self, fig: plt.Figure, output_dir: Path, name: str) -> Path:
        """Save figure to output directory."""
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"{name}.png"
        fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return path
    
    def create_header(self, ax: plt.Axes, title: str, subtitle: str = None):
        """Add a styled header to an axes."""
        ax.set_title(title, fontsize=16, fontweight="bold", loc="left", pad=20)
        if subtitle:
            ax.text(0, 1.02, subtitle, transform=ax.transAxes, 
                   fontsize=10, color=self.colors["neutral"])
    
    def generate_pdf(self, output_dir: Path, **kwargs) -> Optional[Path]:
        """Generate PDF report. Override in subclasses that support PDF output."""
        return None
