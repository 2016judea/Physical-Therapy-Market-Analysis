"""
Market Rate Report - Payer Analysis for Minnesota

Shows rate distributions and comparisons across MN insurance payers.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from .base import BaseReport


class MarketBenchmarkReport(BaseReport):
    """Market rate analysis by payer."""
    
    @property
    def title(self) -> str:
        return "MN Payer Rate Analysis"
    
    @property
    def description(self) -> str:
        return "Rate comparison across Minnesota insurance payers"
    
    def generate(self, output_dir: Path) -> Path:
        """Generate the market rate report images."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        self._generate_payer_overview(output_dir)
        self._generate_payer_comparison_by_code(output_dir)
        self._generate_rate_distributions(output_dir)
        self._generate_percentile_table(output_dir)
        
        return output_dir
    
    def _generate_payer_overview(self, output_dir: Path):
        """Overview of all payers - median rates comparison."""
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Get median rates by payer and code category
        eval_codes = ["97161", "97162", "97163"]
        tx_codes = ["97110", "97140", "97530"]
        
        payer_stats = self.db.query_df("""
            SELECT 
                payer_name,
                ROUND(AVG(CASE WHEN billing_code IN ('97161','97162','97163') 
                    THEN negotiated_rate END), 2) as eval_median,
                ROUND(AVG(CASE WHEN billing_code IN ('97110','97140','97530') 
                    THEN negotiated_rate END), 2) as tx_median,
                COUNT(DISTINCT npi) as providers
            FROM rates
            GROUP BY payer_name
            ORDER BY tx_median DESC
        """)
        
        x = np.arange(len(payer_stats))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, payer_stats["eval_median"], width, 
                      label="Evaluations (97161-63)", color=self.colors["primary"])
        bars2 = ax.bar(x + width/2, payer_stats["tx_median"], width,
                      label="Treatment Units (97110, 97140, 97530)", color=self.colors["secondary"])
        
        ax.set_xticks(x)
        ax.set_xticklabels(payer_stats["payer_name"], rotation=15, ha="right")
        ax.set_ylabel("Average Rate ($)")
        ax.set_title("MN Payer Rate Comparison", fontsize=14, fontweight="bold")
        ax.legend(loc="upper right")
        ax.yaxis.grid(True, linestyle="--", alpha=0.5)
        
        # Add value labels
        for bar in bars1:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                   f"${bar.get_height():.0f}", ha="center", fontsize=9)
        for bar in bars2:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                   f"${bar.get_height():.0f}", ha="center", fontsize=9)
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "00_summary_dashboard")
    
    def _generate_payer_comparison_by_code(self, output_dir: Path):
        """Heatmap showing rates by payer and CPT code."""
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Pivot: payers as rows, codes as columns
        pivot_data = self.db.query_df("""
            SELECT 
                payer_name,
                billing_code,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY negotiated_rate), 2) as median
            FROM rates
            WHERE billing_code IN ('97110','97140','97530','97161','97162','97163')
            GROUP BY payer_name, billing_code
        """)
        
        pivot = pivot_data.pivot(index="payer_name", columns="billing_code", values="median")
        pivot = pivot[["97161", "97162", "97163", "97110", "97140", "97530"]]  # Order columns
        
        # Rename columns to include descriptions
        code_labels = {
            "97161": "97161\nEval Low",
            "97162": "97162\nEval Mod",
            "97163": "97163\nEval High",
            "97110": "97110\nTher Ex",
            "97140": "97140\nManual",
            "97530": "97530\nTher Act",
        }
        pivot = pivot.rename(columns=code_labels)
        
        sns.heatmap(pivot, annot=True, fmt=".0f", cmap="Blues", ax=ax,
                   cbar_kws={"label": "Median Rate ($)"})
        ax.set_title("Median Rates by Payer and CPT Code", fontsize=14, fontweight="bold")
        ax.set_xlabel("")
        ax.set_ylabel("")
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "01_rate_ranges")
    
    def _generate_rate_distributions(self, output_dir: Path):
        """Box plots showing rate distribution by payer for key codes."""
        fig, axes = plt.subplots(2, 3, figsize=(14, 10))
        axes = axes.flatten()
        
        codes = ["97110", "97140", "97530", "97161", "97162", "97163"]
        code_names = {
            "97110": "Ther Exercise",
            "97140": "Manual Therapy", 
            "97530": "Ther Activities",
            "97161": "Eval Low",
            "97162": "Eval Moderate",
            "97163": "Eval High",
        }
        
        for i, code in enumerate(codes):
            ax = axes[i]
            
            data = self.db.query_df(f"""
                SELECT payer_name, negotiated_rate as rate
                FROM rates WHERE billing_code = '{code}'
            """)
            
            # Order by median
            order = data.groupby("payer_name")["rate"].median().sort_values(ascending=False).index
            
            sns.boxplot(data=data, x="payer_name", y="rate", order=order,
                       hue="payer_name", palette="Blues", ax=ax, showfliers=False, legend=False)
            ax.set_title(f"{code}: {code_names[code]}", fontweight="bold")
            ax.set_xlabel("")
            ax.set_ylabel("Rate ($)")
            ax.tick_params(axis="x", rotation=30)
        
        plt.suptitle("Rate Distribution by Payer", fontsize=14, fontweight="bold", y=1.02)
        plt.tight_layout()
        self.save_figure(fig, output_dir, "02_payer_distributions")
    
    def _generate_percentile_table(self, output_dir: Path):
        """Table showing percentiles by payer for key codes."""
        fig, ax = plt.subplots(figsize=(14, 10))
        ax.axis("off")
        
        # Get percentiles by payer for 97110 (highest volume code)
        stats = self.db.query_df("""
            SELECT 
                payer_name,
                billing_code,
                ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p25,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY negotiated_rate), 2) as median,
                ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY negotiated_rate), 2) as p75,
                COUNT(*) as n
            FROM rates
            WHERE billing_code IN ('97110', '97140', '97161')
            GROUP BY payer_name, billing_code
            ORDER BY payer_name, billing_code
        """)
        
        # CPT code descriptions
        code_desc = {
            "97110": "Ther Ex",
            "97140": "Manual",
            "97161": "Eval Low",
        }
        
        # Create table data
        table_data = []
        for _, row in stats.iterrows():
            code = row["billing_code"]
            desc = code_desc.get(code, "")
            table_data.append([
                row["payer_name"],
                f"{code} {desc}",
                f"${row['p25']:.2f}",
                f"${row['median']:.2f}",
                f"${row['p75']:.2f}",
                str(row["n"]),
            ])
        
        columns = ["Payer", "CPT Code", "25th", "Median", "75th", "N"]
        
        table = ax.table(
            cellText=table_data,
            colLabels=columns,
            cellLoc="center",
            loc="center",
            colWidths=[0.22, 0.18, 0.12, 0.12, 0.12, 0.10]
        )
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.6)
        
        for i in range(len(columns)):
            table[(0, i)].set_facecolor(self.colors["primary"])
            table[(0, i)].set_text_props(color="white", fontweight="bold")
        
        ax.set_title("Rate Percentiles by Payer (Key Codes)", fontsize=14, fontweight="bold", y=0.92)
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "03_payer_comparison")
