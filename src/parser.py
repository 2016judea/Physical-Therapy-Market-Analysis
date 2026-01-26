"""Streaming JSON parser for TiC in-network files."""

import gzip
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from io import BytesIO
from typing import BinaryIO, Generator, Optional

import ijson

from .config import load_cpt_codes, get_target_states
from .storage import RateRecord, RatesDatabase


@dataclass
class ParserContext:
    """Holds state during parsing."""

    payer_name: str = ""
    last_updated: Optional[date] = None
    file_source: str = ""
    provider_map: dict[int, list[dict]] = field(default_factory=dict)
    target_cpts: set[str] = field(default_factory=set)
    target_states: set[str] = field(default_factory=set)
    target_npis: Optional[set[str]] = None  # If set, filter to these NPIs only
    records_parsed: int = 0
    records_filtered: int = 0


def get_target_npis() -> Optional[set[str]]:
    """Get set of target NPIs from NPPES table. Returns None if table is empty."""
    db = RatesDatabase()
    result = db.query_df("SELECT npi FROM nppes_providers")
    db.close()
    if result.empty:
        return None
    return set(result["npi"].tolist())


def parse_tic_stream(
    stream: BinaryIO,
    payer_name: str,
    file_source: str,
    decompress: bool = True,
) -> Generator[RateRecord, None, None]:
    """
    Stream-parse a TiC in-network JSON file and yield RateRecords.

    Args:
        stream: Binary stream of the (possibly gzipped) JSON file
        payer_name: Name of the payer for this file
        file_source: URL or path of the source file
        decompress: Whether to decompress gzip stream

    Yields:
        RateRecord for each rate matching our CPT filter
    """
    ctx = ParserContext(
        payer_name=payer_name,
        file_source=file_source,
        target_cpts=load_cpt_codes(),
        target_states=get_target_states(),
    )

    if decompress:
        stream = gzip.GzipFile(fileobj=stream)

    # First pass: build provider reference map
    # TiC files have provider_references at the end, but we need them to resolve NPIs
    # For very large files, we may need a two-pass approach or store references separately
    # For now, we'll parse provider_references first if they appear before in_network

    parser = ijson.parse(stream, use_float=True)

    # Track nested structure
    current_path: list[str] = []
    current_rate_item: dict = {}
    current_negotiated_prices: list[dict] = []
    current_provider_refs: list[int] = []
    in_rate_item = False
    in_negotiated_prices = False

    for prefix, event, value in parser:
        # Extract metadata
        if prefix == "reporting_entity_name" and event == "string":
            if not ctx.payer_name:
                ctx.payer_name = value

        elif prefix == "last_updated_on" and event == "string":
            try:
                ctx.last_updated = date.fromisoformat(value)
            except ValueError:
                pass

        # Build provider reference map
        elif prefix.startswith("provider_references.item"):
            yield from _handle_provider_reference(prefix, event, value, ctx)

        # Parse in_network rates
        elif prefix.startswith("in_network.item"):
            yield from _handle_in_network_item(prefix, event, value, ctx)


def _handle_provider_reference(
    prefix: str, event: str, value, ctx: ParserContext
) -> Generator[RateRecord, None, None]:
    """Parse provider_references section to build NPI lookup map."""
    # provider_references structure:
    # [{"provider_group_id": 123, "provider_groups": [{"npi": ["123"], "tin": {...}}]}]

    # This is simplified - full implementation needs to track nested state
    # For MVP, we'll handle this in the in_network parsing where we can resolve later
    yield from []


def _handle_in_network_item(
    prefix: str, event: str, value, ctx: ParserContext
) -> Generator[RateRecord, None, None]:
    """Parse in_network items and yield matching RateRecords."""
    # Simplified parsing - tracks key fields
    # Full structure:
    # {
    #   "negotiation_arrangement": "ffs",
    #   "billing_code_type": "CPT",
    #   "billing_code": "97110",
    #   "negotiated_rates": [{
    #     "negotiated_prices": [{
    #       "negotiated_rate": 45.00,
    #       "negotiated_type": "negotiated",
    #       "billing_class": "professional",
    #       "service_code": ["11"]
    #     }],
    #     "provider_references": [123, 456]
    #   }]
    # }

    yield from []


def parse_tic_file_simple(
    data: dict,
    payer_name: str,
    file_source: str,
    target_npis: Optional[set[str]] = None,
) -> Generator[RateRecord, None, None]:
    """
    Parse a fully-loaded TiC JSON dict.
    Use this for smaller files or testing.
    For large files, use parse_tic_stream.
    
    Args:
        data: Parsed JSON dict
        payer_name: Payer name for records
        file_source: Source file URL/path
        target_npis: If provided, only include rates for these NPIs (MN filter)
    """
    target_cpts = load_cpt_codes()
    target_states = get_target_states()
    
    # Load target NPIs if not provided (from NPPES table)
    if target_npis is None:
        target_npis = get_target_npis()

    # Extract metadata
    entity_name = data.get("reporting_entity_name", payer_name)
    last_updated_str = data.get("last_updated_on", "")
    try:
        last_updated = date.fromisoformat(last_updated_str) if last_updated_str else None
    except ValueError:
        last_updated = None

    # Build provider reference map: provider_group_id -> list of {npi, tin}
    provider_map: dict[int, list[dict]] = {}
    for pref in data.get("provider_references", []):
        group_id = pref.get("provider_group_id")
        if group_id is not None:
            providers = []
            for pg in pref.get("provider_groups", []):
                npis = pg.get("npi", [])
                tin_info = pg.get("tin", {})
                tin_value = tin_info.get("value") if isinstance(tin_info, dict) else None
                for npi in npis:
                    providers.append({"npi": str(npi), "tin": tin_value})
            provider_map[group_id] = providers

    # Parse in_network rates
    for item in data.get("in_network", []):
        billing_code = item.get("billing_code", "")
        billing_code_type = item.get("billing_code_type", "CPT")

        # Filter to target CPT codes
        if billing_code not in target_cpts:
            continue

        for neg_rate in item.get("negotiated_rates", []):
            # Get provider references for this rate
            prov_refs = neg_rate.get("provider_references", [])

            # Resolve to actual NPIs
            provider_list = []
            for ref_id in prov_refs:
                if ref_id in provider_map:
                    provider_list.extend(provider_map[ref_id])

            # If no provider refs resolved, skip (can't filter by state)
            if not provider_list:
                continue

            for price in neg_rate.get("negotiated_prices", []):
                rate_value = price.get("negotiated_rate")
                if rate_value is None:
                    continue

                negotiated_type = price.get("negotiated_type", "")
                billing_class = price.get("billing_class", "")
                service_codes = price.get("service_code", [])
                pos = service_codes[0] if service_codes else None

                # Yield one record per provider (filtered by NPI if target set exists)
                for prov in provider_list:
                    npi = prov["npi"]
                    
                    # Filter by target NPIs (MN providers) if available
                    if target_npis is not None and npi not in target_npis:
                        continue
                    
                    yield RateRecord(
                        payer_name=entity_name,
                        last_updated=last_updated,
                        billing_code=billing_code,
                        billing_code_type=billing_code_type,
                        negotiated_rate=Decimal(str(rate_value)),
                        negotiated_type=negotiated_type,
                        billing_class=billing_class,
                        place_of_service=pos,
                        npi=npi,
                        tin=prov.get("tin"),
                        file_source=file_source,
                    )


class TiCParser:
    """High-level parser interface."""

    def __init__(self, payer_name: str):
        self.payer_name = payer_name
        self.target_cpts = load_cpt_codes()
        self.target_states = get_target_states()
        self.stats = {"files_parsed": 0, "records_yielded": 0, "records_filtered": 0}

    def parse_file(self, filepath: str) -> Generator[RateRecord, None, None]:
        """Parse a local file (gzipped or plain JSON)."""
        import json

        if filepath.endswith(".gz"):
            with gzip.open(filepath, "rt") as f:
                data = json.load(f)
        else:
            with open(filepath) as f:
                data = json.load(f)

        self.stats["files_parsed"] += 1

        for record in parse_tic_file_simple(data, self.payer_name, filepath):
            self.stats["records_yielded"] += 1
            yield record

    def parse_stream(
        self, stream: BinaryIO, file_source: str, decompress: bool = True
    ) -> Generator[RateRecord, None, None]:
        """Parse a stream (for downloading without saving to disk)."""
        for record in parse_tic_stream(
            stream, self.payer_name, file_source, decompress
        ):
            self.stats["records_yielded"] += 1
            yield record
