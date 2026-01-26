"""Download and process TiC MRF files from payer index URLs."""

import gzip
import json
import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urlparse

import httpx
from rich.console import Console
from rich.progress import Progress, TaskID

from .config import PayerConfig, get_data_dir, load_cpt_codes
from .parser import TiCParser, parse_tic_file_simple
from .storage import RateRecord, RatesDatabase

console = Console()


@dataclass
class IndexEntry:
    """Entry from a TiC index file pointing to an in-network file."""

    description: str
    location: str
    file_type: str = "in-network"


class TiCDownloader:
    """Downloads and processes TiC MRF files from payers."""

    def __init__(
        self,
        payer: PayerConfig,
        db: RatesDatabase,
        cache_dir: Optional[Path] = None,
    ):
        self.payer = payer
        self.db = db
        self.cache_dir = cache_dir or (get_data_dir() / "raw" / self._safe_name(payer.name))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.target_cpts = load_cpt_codes()

        self.client = httpx.Client(
            timeout=httpx.Timeout(30.0, read=300.0),
            follow_redirects=True,
            headers={"User-Agent": "TiC-Data-Pipeline/0.1"},
        )

    def _safe_name(self, name: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]", "_", name.lower())

    def fetch_index(self) -> list[IndexEntry]:
        """Fetch and parse the payer's index/table-of-contents file."""
        console.print(f"[blue]Fetching index for {self.payer.name}...[/blue]")

        try:
            resp = self.client.get(self.payer.index_url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            console.print(f"[red]Failed to fetch index: {e}[/red]")
            return []

        # Index files can be JSON or gzipped JSON
        content = resp.content
        if self.payer.index_url.endswith(".gz") or resp.headers.get(
            "content-encoding"
        ) == "gzip":
            try:
                content = gzip.decompress(content)
            except gzip.BadGzipFile:
                pass

        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            console.print(f"[red]Failed to parse index JSON: {e}[/red]")
            return []

        return self._parse_index(data)

    def _parse_index(self, data: dict) -> list[IndexEntry]:
        """Parse index file to extract in-network file URLs."""
        entries = []

        # Common index structures:
        # 1. {"reporting_structure": [{"in_network_files": [{"location": "..."}]}]}
        # 2. {"in_network_files": [{"location": "..."}]}
        # 3. Direct list of file references

        # Try structure 1: reporting_structure
        for rs in data.get("reporting_structure", []):
            for inf in rs.get("in_network_files", []):
                location = inf.get("location", "")
                if location:
                    entries.append(
                        IndexEntry(
                            description=inf.get("description", ""),
                            location=location,
                        )
                    )

        # Try structure 2: direct in_network_files
        if not entries:
            for inf in data.get("in_network_files", []):
                location = inf.get("location", "")
                if location:
                    entries.append(
                        IndexEntry(
                            description=inf.get("description", ""),
                            location=location,
                        )
                    )

        console.print(f"[green]Found {len(entries)} in-network files[/green]")
        return entries

    def download_and_parse(
        self,
        entry: IndexEntry,
        skip_existing: bool = True,
    ) -> int:
        """Download a single in-network file and parse it into the database."""
        if skip_existing and self.db.is_file_ingested(entry.location):
            console.print(f"[yellow]Skipping already ingested: {entry.location}[/yellow]")
            return 0

        log_id = self.db.log_ingestion_start(self.payer.name, entry.location)

        try:
            records = list(self._stream_and_parse(entry.location))
            inserted = self.db.insert_rates(records)
            self.db.log_ingestion_complete(log_id, inserted)
            console.print(
                f"[green]Inserted {inserted} records from {entry.description or entry.location}[/green]"
            )
            return inserted

        except Exception as e:
            self.db.log_ingestion_error(log_id, str(e))
            console.print(f"[red]Error processing {entry.location}: {e}[/red]")
            return 0

    def _stream_and_parse(self, url: str) -> Generator[RateRecord, None, None]:
        """Stream download and parse a file, yielding records."""
        console.print(f"[blue]Downloading: {url}[/blue]")

        with self.client.stream("GET", url) as resp:
            resp.raise_for_status()

            # Collect streamed content
            chunks = []
            total_size = int(resp.headers.get("content-length", 0))

            with Progress() as progress:
                task = progress.add_task(
                    f"[cyan]Downloading...", total=total_size or None
                )

                for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                    chunks.append(chunk)
                    progress.update(task, advance=len(chunk))

            content = b"".join(chunks)

        # Decompress if needed (check URL path, ignoring query params)
        url_path = url.split("?")[0]
        if url_path.endswith(".gz"):
            console.print("[blue]Decompressing...[/blue]")
            content = gzip.decompress(content)

        # Parse JSON
        console.print("[blue]Parsing JSON...[/blue]")
        data = json.loads(content)

        # Yield records
        console.print("[blue]Extracting rates...[/blue]")
        yield from parse_tic_file_simple(data, self.payer.name, url)

    def process_all(
        self,
        max_files: Optional[int] = None,
        skip_existing: bool = True,
    ) -> int:
        """Process all files from this payer's index."""
        entries = self.fetch_index()

        if max_files:
            entries = entries[:max_files]

        total_records = 0
        for i, entry in enumerate(entries, 1):
            console.print(f"\n[bold]Processing file {i}/{len(entries)}[/bold]")
            total_records += self.download_and_parse(entry, skip_existing)

        return total_records

    def close(self):
        self.client.close()


def download_payer(
    payer: PayerConfig,
    max_files: Optional[int] = None,
    skip_existing: bool = True,
) -> int:
    """Convenience function to download all files for a payer."""
    db = RatesDatabase()
    downloader = TiCDownloader(payer, db)

    try:
        return downloader.process_all(max_files, skip_existing)
    finally:
        downloader.close()
        db.close()
