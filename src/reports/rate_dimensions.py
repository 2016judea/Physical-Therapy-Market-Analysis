"""
Rate Dimension Reports

Analyze rates by negotiated_type, billing_class, and place_of_service.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from .base import BaseReport


class RateDimensionsReport(BaseReport):
    """Rate analysis by negotiated_type, billing_class, place_of_service."""
    
    # Place of service code descriptions
    POS_CODES = {
        "01": "Pharmacy",
        "02": "Telehealth",
        "03": "School",
        "04": "Homeless Shelter",
        "05": "IHS Facility",
        "11": "Office",
        "12": "Home",
        "19": "Off-Campus Outpatient",
        "22": "Outpatient Hospital",
        "31": "SNF",
        "32": "Nursing Facility",
        "34": "Hospice",
        "99": "Other",
    }
    
    @property
    def title(self) -> str:
        return "Rate Dimensions Analysis"
    
    @property
    def description(self) -> str:
        return "Rate breakdown by negotiation type, billing class, and place of service"
    
    def generate(self, output_dir: Path) -> Path:
        """Generate the rate dimensions report images."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        self._generate_by_negotiated_type(output_dir)
        self._generate_by_billing_class(output_dir)
        self._generate_by_place_of_service(output_dir)
        
        return output_dir
    
    def _get_percentiles_by_dimension(self, dimension: str, codes: list[str] = None) -> pd.DataFrame:
        """Get p25, p50, p75 by a dimension (negotiated_type, billing_class, place_of_service)."""
        codes = codes or self.KEY_CODES
        code_list = "', '".join(codes)
        
        query = f"""
            SELECT 
                {dimension},
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p50,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p75,
                COUNT(*) as n
            FROM rates
            WHERE billing_code IN ('{code_list}')
              AND {dimension} IS NOT NULL
            GROUP BY {dimension}
            ORDER BY p50 DESC
        """
        return self.db.query_df(query)
    
    def _get_percentiles_by_dimension_and_payer(self, dimension: str, codes: list[str] = None) -> pd.DataFrame:
        """Get p25, p50, p75 by dimension and payer."""
        codes = codes or self.KEY_CODES
        code_list = "', '".join(codes)
        
        query = f"""
            SELECT 
                payer_name,
                {dimension},
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p50,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p75,
                COUNT(*) as n
            FROM rates
            WHERE billing_code IN ('{code_list}')
              AND {dimension} IS NOT NULL
            GROUP BY payer_name, {dimension}
            ORDER BY payer_name, p50 DESC
        """
        return self.db.query_df(query)
    
    def _generate_by_negotiated_type(self, output_dir: Path):
        """Generate charts breaking down rates by negotiated_type."""
        # Overall chart
        overall = self._get_percentiles_by_dimension("negotiated_type")
        by_payer = self._get_percentiles_by_dimension_and_payer("negotiated_type")
        
        payers = by_payer["payer_name"].unique()
        n_payers = len(payers)
        
        fig, axes = plt.subplots(1 + n_payers, 1, figsize=(12, 4 + 3 * n_payers))
        if n_payers == 0:
            axes = [axes]
        
        # Overall
        ax = axes[0]
        self._plot_percentile_bars(ax, overall, "negotiated_type", "All Payers - By Negotiation Type")
        
        # By payer
        for i, payer in enumerate(payers):
            ax = axes[i + 1]
            payer_data = by_payer[by_payer["payer_name"] == payer]
            self._plot_percentile_bars(ax, payer_data, "negotiated_type", f"{payer} - By Negotiation Type")
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "00_by_negotiated_type")
    
    def _generate_by_billing_class(self, output_dir: Path):
        """Generate charts breaking down rates by billing_class."""
        overall = self._get_percentiles_by_dimension("billing_class")
        by_payer = self._get_percentiles_by_dimension_and_payer("billing_class")
        
        payers = by_payer["payer_name"].unique()
        n_payers = len(payers)
        
        fig, axes = plt.subplots(1 + n_payers, 1, figsize=(12, 4 + 3 * n_payers))
        if n_payers == 0:
            axes = [axes]
        
        # Overall
        ax = axes[0]
        self._plot_percentile_bars(ax, overall, "billing_class", "All Payers - By Billing Class")
        
        # By payer
        for i, payer in enumerate(payers):
            ax = axes[i + 1]
            payer_data = by_payer[by_payer["payer_name"] == payer]
            self._plot_percentile_bars(ax, payer_data, "billing_class", f"{payer} - By Billing Class")
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "01_by_billing_class")
    
    def _generate_by_place_of_service(self, output_dir: Path):
        """Generate charts breaking down rates by place_of_service."""
        overall = self._get_percentiles_by_dimension("place_of_service")
        by_payer = self._get_percentiles_by_dimension_and_payer("place_of_service")
        
        # Add POS descriptions
        overall["pos_label"] = overall["place_of_service"].apply(
            lambda x: f"{x} - {self.POS_CODES.get(str(x), 'Other')}"
        )
        by_payer["pos_label"] = by_payer["place_of_service"].apply(
            lambda x: f"{x} - {self.POS_CODES.get(str(x), 'Other')}"
        )
        
        payers = by_payer["payer_name"].unique()
        n_payers = len(payers)
        
        fig, axes = plt.subplots(1 + n_payers, 1, figsize=(12, 4 + 3 * n_payers))
        if n_payers == 0:
            axes = [axes]
        
        # Overall
        ax = axes[0]
        self._plot_percentile_bars(ax, overall, "pos_label", "All Payers - By Place of Service")
        
        # By payer
        for i, payer in enumerate(payers):
            ax = axes[i + 1]
            payer_data = by_payer[by_payer["payer_name"] == payer]
            self._plot_percentile_bars(ax, payer_data, "pos_label", f"{payer} - By Place of Service")
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "02_by_place_of_service")
    
    def _plot_percentile_bars(self, ax: plt.Axes, data: pd.DataFrame, label_col: str, title: str):
        """Plot horizontal bar chart with p25-p75 range and p50 marker."""
        if data.empty:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(title, fontweight="bold")
            return
        
        y_pos = np.arange(len(data))
        labels = data[label_col].tolist()
        
        # Draw IQR bars
        for i, (_, row) in enumerate(data.iterrows()):
            ax.barh(i, row["p75"] - row["p25"], left=row["p25"],
                   color=self.colors["primary"], alpha=0.6, height=0.6)
            # P50 marker
            ax.scatter(row["p50"], i, color="white", s=60, zorder=5,
                      edgecolor=self.colors["primary"], linewidth=2)
            # Value label
            ax.text(row["p75"] + 2, i, f"${row['p50']:.0f}", va="center", fontsize=9)
        
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Rate ($)")
        ax.set_title(title, fontweight="bold")
        ax.xaxis.grid(True, linestyle="--", alpha=0.5)
        ax.set_axisbelow(True)


class RateTimeSeriesReport(BaseReport):
    """Rate trends over time based on last_updated date."""
    
    @property
    def title(self) -> str:
        return "Rate Trends Over Time"
    
    @property
    def description(self) -> str:
        return "How rates have changed based on MRF publication dates"
    
    def generate(self, output_dir: Path) -> Path:
        """Generate the time series report images."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        self._generate_overall_trend(output_dir)
        self._generate_trend_by_payer(output_dir)
        self._generate_trend_by_code(output_dir)
        
        return output_dir
    
    def _generate_overall_trend(self, output_dir: Path):
        """Overall rate trend across all payers."""
        data = self.db.query_df("""
            SELECT 
                DATE_TRUNC('month', last_updated) as month,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p50,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p75,
                COUNT(*) as n
            FROM rates
            WHERE last_updated IS NOT NULL
            GROUP BY DATE_TRUNC('month', last_updated)
            ORDER BY month
        """)
        
        if data.empty or len(data) < 2:
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, "Insufficient time series data\n(need multiple months)", 
                   ha="center", va="center", transform=ax.transAxes, fontsize=14)
            ax.set_title("Rate Trends Over Time", fontweight="bold")
            self.save_figure(fig, output_dir, "00_overall_trend")
            return
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.fill_between(data["month"], data["p25"], data["p75"], 
                       alpha=0.3, color=self.colors["primary"], label="25th-75th percentile")
        ax.plot(data["month"], data["p50"], color=self.colors["primary"], 
               linewidth=2, marker="o", label="Median")
        
        ax.set_xlabel("Month")
        ax.set_ylabel("Rate ($)")
        ax.set_title("Overall Rate Trends (All Payers, Key CPT Codes)", fontsize=14, fontweight="bold")
        ax.legend(loc="upper left")
        ax.yaxis.grid(True, linestyle="--", alpha=0.5)
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        self.save_figure(fig, output_dir, "00_overall_trend")
    
    def _generate_trend_by_payer(self, output_dir: Path):
        """Rate trends broken down by payer."""
        data = self.db.query_df("""
            SELECT 
                payer_name,
                DATE_TRUNC('month', last_updated) as month,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p50,
                COUNT(*) as n
            FROM rates
            WHERE last_updated IS NOT NULL
            GROUP BY payer_name, DATE_TRUNC('month', last_updated)
            ORDER BY payer_name, month
        """)
        
        if data.empty:
            return
        
        payers = data["payer_name"].unique()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        colors = plt.cm.tab10(np.linspace(0, 1, len(payers)))
        for i, payer in enumerate(payers):
            payer_data = data[data["payer_name"] == payer]
            ax.plot(payer_data["month"], payer_data["p50"], 
                   linewidth=2, marker="o", label=payer, color=colors[i])
        
        ax.set_xlabel("Month")
        ax.set_ylabel("Median Rate ($)")
        ax.set_title("Median Rate Trends by Payer", fontsize=14, fontweight="bold")
        ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
        ax.yaxis.grid(True, linestyle="--", alpha=0.5)
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        self.save_figure(fig, output_dir, "01_trend_by_payer")
    
    def _generate_trend_by_code(self, output_dir: Path):
        """Rate trends for key CPT codes."""
        code_list = "', '".join(self.KEY_CODES)
        
        data = self.db.query_df(f"""
            SELECT 
                billing_code,
                DATE_TRUNC('month', last_updated) as month,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p50,
                COUNT(*) as n
            FROM rates
            WHERE last_updated IS NOT NULL
              AND billing_code IN ('{code_list}')
            GROUP BY billing_code, DATE_TRUNC('month', last_updated)
            ORDER BY billing_code, month
        """)
        
        if data.empty:
            return
        
        data = self.add_cpt_description(data, "billing_code")
        
        codes = data["billing_code"].unique()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        colors = plt.cm.tab10(np.linspace(0, 1, len(codes)))
        for i, code in enumerate(codes):
            code_data = data[data["billing_code"] == code]
            desc = code_data["description"].iloc[0] if not code_data.empty else ""
            ax.plot(code_data["month"], code_data["p50"], 
                   linewidth=2, marker="o", label=f"{code} {desc}", color=colors[i])
        
        ax.set_xlabel("Month")
        ax.set_ylabel("Median Rate ($)")
        ax.set_title("Median Rate Trends by CPT Code", fontsize=14, fontweight="bold")
        ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1))
        ax.yaxis.grid(True, linestyle="--", alpha=0.5)
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        self.save_figure(fig, output_dir, "02_trend_by_code")
