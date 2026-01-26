"""
Rate Reference Card Report

A simple, printable reference showing market rates by CPT code.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from .base import BaseReport


class FairMarketValueReport(BaseReport):
    """Rate reference card for quick lookups."""
    
    @property
    def title(self) -> str:
        return "Rate Reference Card"
    
    @property
    def description(self) -> str:
        return "Quick reference for market rates by CPT code"
    
    def generate(self, output_dir: Path) -> Path:
        """Generate the rate reference card images."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        self._generate_summary(output_dir)
        self._generate_rate_card(output_dir)
        self._generate_market_ranges(output_dir)
        
        return output_dir
    
    def _generate_summary(self, output_dir: Path):
        """Generate a summary view of key codes."""
        fig = plt.figure(figsize=(11, 8.5))
        gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)
        
        stats = self.get_market_stats(self.KEY_CODES)
        stats = self.add_cpt_description(stats)
        
        fig.suptitle("PT Rate Quick Reference", fontsize=16, fontweight="bold", y=0.96)
        
        # Evaluations
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.axis("off")
        eval_stats = stats[stats["cpt_code"].isin(["97161", "97162", "97163"])]
        text = "EVALUATIONS\n" + "-" * 30 + "\n"
        for _, row in eval_stats.iterrows():
            text += f"\n{row['cpt_code']} {row['description']}\n"
            text += f"  ${row['p25']:.0f} - ${row['p75']:.0f} (med ${row['median']:.0f})\n"
        ax1.text(0.1, 0.9, text, transform=ax1.transAxes, fontsize=11,
                fontfamily="monospace", verticalalignment="top",
                bbox=dict(boxstyle="round", facecolor="#eff6ff", edgecolor=self.colors["primary"]))
        
        # Treatment
        ax2 = fig.add_subplot(gs[0, 1])
        ax2.axis("off")
        tx_stats = stats[stats["cpt_code"].isin(["97110", "97140", "97530"])]
        text = "TREATMENT (per unit)\n" + "-" * 30 + "\n"
        for _, row in tx_stats.iterrows():
            text += f"\n{row['cpt_code']} {row['description']}\n"
            text += f"  ${row['p25']:.0f} - ${row['p75']:.0f} (med ${row['median']:.0f})\n"
        ax2.text(0.1, 0.9, text, transform=ax2.transAxes, fontsize=11,
                fontfamily="monospace", verticalalignment="top",
                bbox=dict(boxstyle="round", facecolor="#f0fdf4", edgecolor=self.colors["secondary"]))
        
        # Bar chart
        ax3 = fig.add_subplot(gs[1, :])
        x = np.arange(len(stats))
        ax3.bar(x, stats["p75"] - stats["p25"], bottom=stats["p25"],
               color=self.colors["primary"], alpha=0.6, edgecolor="white")
        ax3.scatter(x, stats["median"], color="white", s=80, zorder=5,
                   edgecolor=self.colors["primary"], linewidth=2)
        ax3.set_xticks(x)
        ax3.set_xticklabels(stats["cpt_code"])
        ax3.set_ylabel("Rate ($)")
        ax3.set_title("25th-75th Percentile Range (dot = median)", fontweight="bold")
        ax3.yaxis.grid(True, linestyle="--", alpha=0.5)
        
        self.save_figure(fig, output_dir, "00_case_manager_summary")
    
    def _generate_rate_card(self, output_dir: Path):
        """Generate a clean rate reference card."""
        fig, ax = plt.subplots(figsize=(10, 12))
        ax.axis("off")
        
        all_codes = []
        for codes in self.CPT_CATEGORIES.values():
            all_codes.extend(codes.keys())
        
        stats = self.get_market_stats(all_codes)
        stats = self.add_cpt_description(stats)
        
        ax.text(0.5, 0.97, "PT Rate Reference Card", fontsize=18, fontweight="bold",
               ha="center", transform=ax.transAxes, color=self.colors["primary"])
        
        y_pos = 0.90
        for category, codes in self.CPT_CATEGORIES.items():
            ax.text(0.05, y_pos, category, fontsize=12, fontweight="bold",
                   transform=ax.transAxes, color=self.colors["primary"])
            ax.plot([0.05, 0.95], [y_pos - 0.01, y_pos - 0.01],
                   color=self.colors["primary"], linewidth=1, transform=ax.transAxes)
            y_pos -= 0.03
            
            # Headers
            ax.text(0.05, y_pos, "CPT", fontsize=9, fontweight="bold", transform=ax.transAxes)
            ax.text(0.12, y_pos, "Description", fontsize=9, fontweight="bold", transform=ax.transAxes)
            ax.text(0.55, y_pos, "25th-75th", fontsize=9, fontweight="bold", transform=ax.transAxes, ha="center")
            ax.text(0.75, y_pos, "Median", fontsize=9, fontweight="bold", transform=ax.transAxes, ha="center")
            y_pos -= 0.02
            
            for code, desc in codes.items():
                row = stats[stats["cpt_code"] == code]
                if row.empty:
                    continue
                row = row.iloc[0]
                
                ax.text(0.05, y_pos, code, fontsize=9, transform=ax.transAxes)
                ax.text(0.12, y_pos, desc[:28], fontsize=9, transform=ax.transAxes)
                ax.text(0.55, y_pos, f"${row['p25']:.0f}-${row['p75']:.0f}",
                       fontsize=9, transform=ax.transAxes, ha="center")
                ax.text(0.75, y_pos, f"${row['median']:.0f}",
                       fontsize=9, fontweight="bold", transform=ax.transAxes, ha="center",
                       color=self.colors["primary"])
                y_pos -= 0.02
            
            y_pos -= 0.01
        
        ax.text(0.5, 0.02, "Source: Transparency in Coverage filings",
               fontsize=8, ha="center", transform=ax.transAxes, color=self.colors["neutral"], style="italic")
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "01_rate_card")
    
    def _generate_market_ranges(self, output_dir: Path):
        """Generate visual market range chart."""
        fig, ax = plt.subplots(figsize=(12, 8))
        
        stats = self.get_market_stats(self.KEY_CODES)
        stats = self.add_cpt_description(stats)
        stats = stats.sort_values("median", ascending=True)
        
        y_pos = np.arange(len(stats))
        
        for i, (_, row) in enumerate(stats.iterrows()):
            # IQR bar
            ax.plot([row["p25"], row["p75"]], [i, i],
                   color=self.colors["primary"], linewidth=10, alpha=0.7,
                   solid_capstyle="round")
            # Median dot
            ax.scatter(row["median"], i, color="white", s=80, zorder=5,
                      edgecolor=self.colors["primary"], linewidth=2)
        
        labels = stats["cpt_code"] + " - " + stats["description"]
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=10)
        ax.set_xlabel("Rate ($)", fontsize=12)
        ax.set_title("Market Rate Ranges (25th-75th percentile)", fontsize=14, fontweight="bold")
        
        ax.xaxis.grid(True, linestyle="--", alpha=0.7)
        ax.set_axisbelow(True)
        
        plt.tight_layout()
        self.save_figure(fig, output_dir, "02_market_ranges")
