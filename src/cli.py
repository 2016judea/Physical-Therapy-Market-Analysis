"""Command-line interface for TiC data pipeline."""

from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from .config import get_enabled_payers, load_payers_config, load_cpt_codes
from .downloader import TiCDownloader, download_payer
from .storage import RatesDatabase
from .nppes import setup_nppes

app = typer.Typer(
    name="tic",
    help="TiC Reimbursement Data Pipeline - Aggregate payer rate data for PT analysis",
)
console = Console()


@app.command()
def status():
    """Show current database status and statistics."""
    db = RatesDatabase()
    stats = db.get_rate_stats()
    db.close()

    table = Table(title="Database Status")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total rate records", f"{stats['total_rates']:,}")
    table.add_row("Unique payers", str(stats["payers"]))
    table.add_row("Unique CPT codes", str(stats["cpt_codes"]))
    table.add_row("Unique providers (NPI)", f"{stats['providers']:,}")

    console.print(table)


@app.command()
def payers():
    """List configured payers and their status."""
    config = load_payers_config()

    table = Table(title="Configured Payers")
    table.add_column("Name", style="cyan")
    table.add_column("Enabled", style="green")
    table.add_column("Index URL", style="dim", max_width=60)
    table.add_column("Notes", style="dim")

    for p in config.payers:
        enabled = "✓" if p.enabled else "✗"
        url = p.index_url[:57] + "..." if len(p.index_url) > 60 else p.index_url
        table.add_row(p.name, enabled, url or "(none)", p.notes or "")

    console.print(table)


@app.command()
def cpts():
    """List configured CPT codes to extract."""
    codes = load_cpt_codes()

    console.print(f"[bold]Configured CPT codes ({len(codes)} total):[/bold]\n")
    # Print in columns
    codes_list = sorted(codes)
    for i in range(0, len(codes_list), 6):
        row = codes_list[i : i + 6]
        console.print("  " + "  ".join(row))


@app.command()
def ingest(
    payer_name: Optional[str] = typer.Option(
        None, "--payer", "-p", help="Specific payer to ingest (default: all enabled)"
    ),
    max_files: Optional[int] = typer.Option(
        None, "--max-files", "-n", help="Maximum number of files to process per payer"
    ),
    skip_existing: bool = typer.Option(
        True, "--skip-existing/--reprocess", help="Skip already-ingested files"
    ),
):
    """Ingest rate data from payer MRF files."""
    payers_to_process = get_enabled_payers()

    if payer_name:
        payers_to_process = [p for p in payers_to_process if p.name.lower() == payer_name.lower()]
        if not payers_to_process:
            console.print(f"[red]Payer '{payer_name}' not found or not enabled[/red]")
            raise typer.Exit(1)

    if not payers_to_process:
        console.print("[yellow]No enabled payers with valid index URLs found[/yellow]")
        raise typer.Exit(1)

    console.print(f"[bold]Processing {len(payers_to_process)} payer(s)...[/bold]\n")

    total_records = 0
    for payer in payers_to_process:
        console.print(f"\n[bold blue]═══ {payer.name} ═══[/bold blue]")
        records = download_payer(payer, max_files, skip_existing)
        total_records += records

    console.print(f"\n[bold green]Done! Total records ingested: {total_records:,}[/bold green]")


@app.command()
def query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    limit: int = typer.Option(100, "--limit", "-l", help="Maximum rows to display"),
    format: str = typer.Option("table", "--format", "-f", help="Output format: table, csv, json"),
):
    """Execute a SQL query against the rates database."""
    db = RatesDatabase()

    try:
        result = db.query_df(sql)
    except Exception as e:
        console.print(f"[red]Query error: {e}[/red]")
        db.close()
        raise typer.Exit(1)

    db.close()

    if result.empty:
        console.print("[yellow]No results[/yellow]")
        return

    if len(result) > limit:
        console.print(f"[dim]Showing first {limit} of {len(result)} rows[/dim]\n")
        result = result.head(limit)

    if format == "csv":
        print(result.to_csv(index=False))
    elif format == "json":
        print(result.to_json(orient="records", indent=2))
    else:
        # Table format using rich
        table = Table()
        for col in result.columns:
            table.add_column(str(col))

        for _, row in result.iterrows():
            table.add_row(*[str(v) for v in row])

        console.print(table)


@app.command()
def summary(
    cpt: Optional[str] = typer.Option(None, "--cpt", "-c", help="Filter by CPT code"),
):
    """Show summary statistics for rates in the database."""
    db = RatesDatabase()

    where_clause = f"WHERE billing_code = '{cpt}'" if cpt else ""

    # Overall stats
    query = f"""
        SELECT
            billing_code,
            COUNT(*) as rate_count,
            COUNT(DISTINCT npi) as provider_count,
            ROUND(MIN(negotiated_rate), 2) as min_rate,
            ROUND(AVG(negotiated_rate), 2) as avg_rate,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY negotiated_rate), 2) as median_rate,
            ROUND(MAX(negotiated_rate), 2) as max_rate
        FROM rates
        {where_clause}
        GROUP BY billing_code
        ORDER BY billing_code
    """

    result = db.query_df(query)
    db.close()

    if result.empty:
        console.print("[yellow]No data found[/yellow]")
        return

    table = Table(title="Rate Summary by CPT Code")
    table.add_column("CPT", style="cyan")
    table.add_column("Rates", justify="right")
    table.add_column("Providers", justify="right")
    table.add_column("Min", justify="right", style="red")
    table.add_column("Avg", justify="right")
    table.add_column("Median", justify="right", style="green")
    table.add_column("Max", justify="right", style="red")

    for _, row in result.iterrows():
        table.add_row(
            str(row["billing_code"]),
            f"{row['rate_count']:,}",
            f"{row['provider_count']:,}",
            f"${row['min_rate']:.2f}",
            f"${row['avg_rate']:.2f}",
            f"${row['median_rate']:.2f}",
            f"${row['max_rate']:.2f}",
        )

    console.print(table)


@app.command("compare-payers")
def compare_payers(
    cpt: str = typer.Argument(..., help="CPT code to compare"),
):
    """Compare rates across payers for a specific CPT code."""
    db = RatesDatabase()

    query = f"""
        SELECT
            payer_name,
            COUNT(*) as rate_count,
            ROUND(MIN(negotiated_rate), 2) as min_rate,
            ROUND(AVG(negotiated_rate), 2) as avg_rate,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY negotiated_rate), 2) as median_rate,
            ROUND(MAX(negotiated_rate), 2) as max_rate
        FROM rates
        WHERE billing_code = '{cpt}'
        GROUP BY payer_name
        ORDER BY median_rate DESC
    """

    result = db.query_df(query)
    db.close()

    if result.empty:
        console.print(f"[yellow]No data found for CPT {cpt}[/yellow]")
        return

    table = Table(title=f"Payer Comparison for CPT {cpt}")
    table.add_column("Payer", style="cyan")
    table.add_column("Rates", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Avg", justify="right")
    table.add_column("Median", justify="right", style="green")
    table.add_column("Max", justify="right")

    for _, row in result.iterrows():
        table.add_row(
            str(row["payer_name"]),
            f"{row['rate_count']:,}",
            f"${row['min_rate']:.2f}",
            f"${row['avg_rate']:.2f}",
            f"${row['median_rate']:.2f}",
            f"${row['max_rate']:.2f}",
        )

    console.print(table)


@app.command("load-nppes")
def load_nppes(
    force: bool = typer.Option(
        False, "--force", "-f", help="Re-download NPPES file even if cached"
    ),
):
    """Download NPPES data and load MN PT providers for filtering."""
    console.print("[bold]Loading NPPES data for Minnesota PT providers...[/bold]")
    console.print("This will download ~1GB and may take a few minutes.\n")
    
    count = setup_nppes(states=["MN"], force_download=force)
    
    console.print(f"\n[bold green]Loaded {count:,} MN PT providers[/bold green]")
    console.print("Future ingestions will filter to only these NPIs.")


@app.command()
def reports(
    output_dir: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output directory (default: reports/YYYY-MM-DD)"
    ),
    no_pdf: bool = typer.Option(
        False, "--no-pdf", help="Skip PDF generation, only create images"
    ),
):
    """Generate market rate reports (images and PDFs)."""
    from .reports.generate import generate_reports
    
    if output_dir is None:
        output_dir = Path("reports") / datetime.now().strftime("%Y-%m-%d")
    
    console.print(f"[bold]Generating reports to {output_dir}...[/bold]")
    
    images, pdfs = generate_reports(
        output_dir=output_dir,
        generate_pdfs=not no_pdf,
    )
    
    console.print(f"\n[bold green]Done! Generated {len(pdfs)} PDFs[/bold green]")


if __name__ == "__main__":
    app()
