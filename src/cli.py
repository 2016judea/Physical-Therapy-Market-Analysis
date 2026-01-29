"""
CLI for PT Rate Analysis tool.

Usage:
    tic init          # Interactive setup for NPIs and location
    tic ingest        # Run full data ingestion pipeline
    tic report        # Generate competitive analysis reports
    tic status        # Show database stats
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm

from .config import get_data_dir, load_cpt_codes
from .storage import RatesDatabase

app = typer.Typer(
    name="tic",
    help="PT Rate Analysis - Transparency in Coverage data pipeline",
    no_args_is_help=True,
)
console = Console()

CONFIG_FILE = get_data_dir() / "user_config.json"
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"


def load_user_config() -> dict:
    """Load user configuration."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return json.load(f)
    return {}


def save_user_config(config: dict):
    """Save user configuration."""
    get_data_dir().mkdir(exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


@app.command()
def init():
    """
    Interactive setup - configure your NPIs and geographic area.
    """
    console.print("\n[bold]PT Rate Analysis - Initial Setup[/bold]\n")
    
    existing = load_user_config()
    
    # Clinic NPI (Type 2)
    console.print("[dim]Type 2 NPIs are for organizations/clinics[/dim]")
    default_clinic = existing.get("clinic_npi", "")
    clinic_npi = Prompt.ask(
        "Primary clinic NPI (Type 2)",
        default=default_clinic if default_clinic else None,
    )
    
    # Individual NPIs (Type 1)
    console.print("\n[dim]Type 1 NPIs are for individual providers[/dim]")
    existing_individuals = existing.get("individual_npis", [])
    
    individual_npis = []
    console.print("Enter individual provider NPIs (empty line to finish):")
    
    # Show existing as defaults
    for i, npi in enumerate(existing_individuals):
        keep = Confirm.ask(f"  Keep {npi}?", default=True)
        if keep:
            individual_npis.append(npi)
    
    # Add new ones
    while True:
        new_npi = Prompt.ask("  Add NPI (or press Enter to finish)", default="")
        if not new_npi:
            break
        if len(new_npi) == 10 and new_npi.isdigit():
            individual_npis.append(new_npi)
        else:
            console.print("[yellow]NPIs should be 10 digits[/yellow]")
    
    # Geographic filter
    console.print("\n[dim]Enter 3-digit zip prefixes to filter providers (e.g., 551 for Saint Paul)[/dim]")
    existing_zips = existing.get("zip_prefixes", [])
    default_zips = ", ".join(existing_zips) if existing_zips else "551"
    
    zip_input = Prompt.ask("Zip prefixes (comma-separated)", default=default_zips)
    zip_prefixes = [z.strip() for z in zip_input.split(",") if z.strip()]
    
    # Practice name (for reports)
    default_name = existing.get("practice_name", "Our Practice")
    practice_name = Prompt.ask("Practice name (for reports)", default=default_name)
    
    # Save config
    config = {
        "clinic_npi": clinic_npi,
        "individual_npis": individual_npis,
        "zip_prefixes": zip_prefixes,
        "practice_name": practice_name,
    }
    save_user_config(config)
    
    console.print("\n[green]✓ Configuration saved![/green]")
    console.print(f"  Clinic NPI: {clinic_npi}")
    console.print(f"  Individual NPIs: {', '.join(individual_npis) if individual_npis else 'None'}")
    console.print(f"  Zip prefixes: {', '.join(zip_prefixes)}")
    console.print(f"  Practice name: {practice_name}")
    
    # Offer to load NPPES data
    console.print()
    if Confirm.ask("Load local provider data from NPPES now?", default=True):
        load_nppes(zip_prefixes)


def load_nppes(zip_prefixes: list[str]):
    """Load NPPES data for the configured zip prefixes."""
    console.print("\n[bold]Loading NPPES provider data...[/bold]")
    
    # Run the NPPES loader with configured zips
    script = SCRIPTS_DIR / "load_mn_nppes.py"
    
    # We need to modify the script to accept zip prefixes, or use a simpler approach
    # For now, run the existing script
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(SCRIPTS_DIR.parent),
        capture_output=False,
    )
    
    if result.returncode == 0:
        console.print("[green]✓ NPPES data loaded[/green]")
    else:
        console.print("[red]Error loading NPPES data[/red]")


@app.command()
def ingest(
    payer: Optional[str] = typer.Option(
        None, "--payer", "-p",
        help="Specific payer to ingest (healthpartners, ucare, bcbs)"
    ),
    skip_bcbs: bool = typer.Option(
        False, "--skip-bcbs",
        help="Skip BCBS (slow, 2-4 hours)"
    ),
):
    """
    Run data ingestion pipeline for all payers.
    """
    config = load_user_config()
    if not config:
        console.print("[yellow]No configuration found. Run 'tic init' first.[/yellow]")
        raise typer.Exit(1)
    
    console.print("\n[bold]PT Rate Analysis - Data Ingestion[/bold]\n")
    
    # Check NPPES data exists
    db = RatesDatabase()
    nppes_count = db.query("SELECT COUNT(*) FROM nppes_providers")[0][0]
    db.close()
    
    if nppes_count == 0:
        console.print("[yellow]No provider data loaded. Loading from NPPES...[/yellow]")
        load_nppes(config.get("zip_prefixes", ["551"]))
    else:
        console.print(f"[dim]Provider database: {nppes_count:,} NPIs[/dim]")
    
    payers_to_run = []
    
    if payer:
        payer_lower = payer.lower()
        if payer_lower in ("hp", "healthpartners"):
            payers_to_run = [("HealthPartners", "ingest_healthpartners.py")]
        elif payer_lower == "ucare":
            payers_to_run = [("UCare", "ingest_ucare.py")]
        elif payer_lower == "bcbs":
            payers_to_run = [("BCBS Minnesota", "ingest_bcbs_local.py")]
        else:
            console.print(f"[red]Unknown payer: {payer}[/red]")
            console.print("Valid options: healthpartners, ucare, bcbs")
            raise typer.Exit(1)
    else:
        payers_to_run = [
            ("HealthPartners", "ingest_healthpartners.py"),
            ("UCare", "ingest_ucare.py"),
        ]
        if not skip_bcbs:
            payers_to_run.append(("BCBS Minnesota", "ingest_bcbs_local.py"))
    
    for payer_name, script_name in payers_to_run:
        console.print(f"\n[bold]Ingesting {payer_name}...[/bold]")
        
        script = SCRIPTS_DIR / script_name
        
        # BCBS needs group scan first
        if script_name == "ingest_bcbs_local.py":
            mapping_file = get_data_dir() / "bcbs_npi_to_groups.json"
            if not mapping_file.exists():
                console.print("[yellow]Running BCBS group scan first (one-time setup)...[/yellow]")
                scan_script = SCRIPTS_DIR / "scan_bcbs_groups.py"
                subprocess.run(
                    [sys.executable, str(scan_script)],
                    cwd=str(SCRIPTS_DIR.parent),
                )
        
        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(SCRIPTS_DIR.parent),
            capture_output=False,
        )
        
        if result.returncode != 0:
            console.print(f"[red]Error ingesting {payer_name}[/red]")
    
    # Show final stats
    status()


@app.command()
def report():
    """
    Generate competitive analysis reports.
    """
    config = load_user_config()
    
    console.print("\n[bold]PT Rate Analysis - Generating Reports[/bold]\n")
    
    # Check we have data
    db = RatesDatabase()
    stats = db.get_rate_stats()
    db.close()
    
    if stats["total_rates"] == 0:
        console.print("[yellow]No rate data in database. Run 'tic ingest' first.[/yellow]")
        raise typer.Exit(1)
    
    # Update the report generator with user's NPIs
    if config:
        _update_report_npis(config)
    
    script = SCRIPTS_DIR / "generate_competitive_report.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(SCRIPTS_DIR.parent),
        capture_output=False,
    )
    
    if result.returncode == 0:
        reports_dir = SCRIPTS_DIR.parent / "reports"
        console.print(f"\n[green]✓ Reports generated in {reports_dir}[/green]")
        
        # List generated reports
        for f in sorted(reports_dir.glob("*.md")):
            console.print(f"  • {f.name}")


def _update_report_npis(config: dict):
    """Update the report generator with user's configured NPIs."""
    report_script = SCRIPTS_DIR / "generate_competitive_report.py"
    content = report_script.read_text()
    
    # Update clinic NPI
    if config.get("clinic_npi"):
        import re
        content = re.sub(
            r'PRIMARY_TYPE2_NPI = "[^"]*"',
            f'PRIMARY_TYPE2_NPI = "{config["clinic_npi"]}"',
            content
        )
    
    # Update individual NPIs
    individual_npis = config.get("individual_npis", [])
    if len(individual_npis) >= 1:
        import re
        content = re.sub(
            r'PRIMARY_TYPE1_NPI_A = "[^"]*"',
            f'PRIMARY_TYPE1_NPI_A = "{individual_npis[0]}"',
            content
        )
    if len(individual_npis) >= 2:
        content = re.sub(
            r'PRIMARY_TYPE1_NPI_B = "[^"]*"',
            f'PRIMARY_TYPE1_NPI_B = "{individual_npis[1]}"',
            content
        )
    
    report_script.write_text(content)


@app.command()
def status():
    """
    Show database statistics.
    """
    console.print("\n[bold]PT Rate Analysis - Status[/bold]\n")
    
    config = load_user_config()
    
    # User config
    if config:
        table = Table(title="Configuration")
        table.add_column("Setting", style="cyan")
        table.add_column("Value")
        
        table.add_row("Practice", config.get("practice_name", "-"))
        table.add_row("Clinic NPI", config.get("clinic_npi", "-"))
        table.add_row("Individual NPIs", ", ".join(config.get("individual_npis", [])) or "-")
        table.add_row("Zip Prefixes", ", ".join(config.get("zip_prefixes", [])) or "-")
        
        console.print(table)
        console.print()
    else:
        console.print("[yellow]No configuration. Run 'tic init' to set up.[/yellow]\n")
    
    # Database stats
    try:
        db = RatesDatabase()
        stats = db.get_rate_stats()
        
        # Provider count
        nppes_count = db.query("SELECT COUNT(*) FROM nppes_providers")[0][0]
        
        # Payer breakdown
        payer_stats = db.query_df("""
            SELECT payer_name, COUNT(*) as rates, COUNT(DISTINCT npi) as npis
            FROM rates
            GROUP BY payer_name
            ORDER BY payer_name
        """)
        
        db.close()
        
        table = Table(title="Database")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        
        table.add_row("Total Rates", f"{stats['total_rates']:,}")
        table.add_row("Payers", str(stats['payers']))
        table.add_row("CPT Codes", str(stats['cpt_codes']))
        table.add_row("Providers with Rates", str(stats['providers']))
        table.add_row("NPPES Providers", f"{nppes_count:,}")
        
        console.print(table)
        
        if not payer_stats.empty:
            console.print()
            payer_table = Table(title="Rates by Payer")
            payer_table.add_column("Payer", style="cyan")
            payer_table.add_column("Rates", justify="right")
            payer_table.add_column("NPIs", justify="right")
            
            for _, row in payer_stats.iterrows():
                payer_table.add_row(
                    row['payer_name'],
                    f"{row['rates']:,}",
                    str(row['npis'])
                )
            
            console.print(payer_table)
            
    except Exception as e:
        console.print(f"[red]Database error: {e}[/red]")
        console.print("[dim]Run 'tic init' to initialize.[/dim]")


@app.command()
def reset(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation")
):
    """
    Reset all data and configuration.
    """
    if not confirm:
        if not Confirm.ask("[red]Delete all data and configuration?[/red]", default=False):
            raise typer.Abort()
    
    data_dir = get_data_dir()
    
    # Remove database
    db_path = data_dir / "rates.duckdb"
    if db_path.exists():
        db_path.unlink()
        console.print("  Deleted rates.duckdb")
    
    # Remove config
    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()
        console.print("  Deleted user_config.json")
    
    # Remove BCBS mapping
    bcbs_map = data_dir / "bcbs_npi_to_groups.json"
    if bcbs_map.exists():
        bcbs_map.unlink()
        console.print("  Deleted bcbs_npi_to_groups.json")
    
    console.print("\n[green]✓ Reset complete[/green]")


if __name__ == "__main__":
    app()
