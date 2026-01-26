"""DuckDB storage layer for rate data."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import duckdb

from .config import get_data_dir


@dataclass
class RateRecord:
    payer_name: str
    last_updated: date
    billing_code: str
    billing_code_type: str
    negotiated_rate: Decimal
    negotiated_type: str
    billing_class: str
    place_of_service: Optional[str]
    npi: str
    tin: Optional[str]
    provider_name: Optional[str] = None
    provider_state: Optional[str] = None
    provider_city: Optional[str] = None
    provider_zip: Optional[str] = None
    file_source: Optional[str] = None


class RatesDatabase:
    def __init__(self, db_path: Optional[Path] = None):
        if db_path is None:
            db_path = get_data_dir() / "rates.duckdb"
        self.db_path = db_path
        self.conn = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS rates_id_seq START 1
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rates (
                id INTEGER DEFAULT nextval('rates_id_seq') PRIMARY KEY,
                payer_name VARCHAR NOT NULL,
                last_updated DATE,
                billing_code VARCHAR NOT NULL,
                billing_code_type VARCHAR DEFAULT 'CPT',
                negotiated_rate DECIMAL(10,2) NOT NULL,
                negotiated_type VARCHAR,
                billing_class VARCHAR,
                place_of_service VARCHAR,
                npi VARCHAR NOT NULL,
                tin VARCHAR,
                provider_name VARCHAR,
                provider_state VARCHAR,
                provider_city VARCHAR,
                provider_zip VARCHAR,
                file_source VARCHAR,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS ingestion_log_id_seq START 1
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id INTEGER DEFAULT nextval('ingestion_log_id_seq') PRIMARY KEY,
                payer_name VARCHAR NOT NULL,
                file_url VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                records_inserted INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message VARCHAR
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nppes_providers (
                npi VARCHAR PRIMARY KEY,
                provider_name VARCHAR,
                provider_type VARCHAR,
                taxonomy_code VARCHAR,
                taxonomy_desc VARCHAR,
                address_line1 VARCHAR,
                city VARCHAR,
                state VARCHAR,
                zip VARCHAR,
                phone VARCHAR
            )
        """)

        # Create indexes if they don't exist
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rates_cpt ON rates(billing_code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rates_npi ON rates(npi)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rates_payer ON rates(payer_name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rates_state ON rates(provider_state)")

    def insert_rates(self, records: list[RateRecord], batch_size: int = 10000):
        if not records:
            return 0

        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            values = [
                (
                    r.payer_name,
                    r.last_updated,
                    r.billing_code,
                    r.billing_code_type,
                    float(r.negotiated_rate),
                    r.negotiated_type,
                    r.billing_class,
                    r.place_of_service,
                    r.npi,
                    r.tin,
                    r.provider_name,
                    r.provider_state,
                    r.provider_city,
                    r.provider_zip,
                    r.file_source,
                )
                for r in batch
            ]

            self.conn.executemany(
                """
                INSERT INTO rates (
                    payer_name, last_updated, billing_code, billing_code_type,
                    negotiated_rate, negotiated_type, billing_class, place_of_service,
                    npi, tin, provider_name, provider_state, provider_city,
                    provider_zip, file_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            total_inserted += len(batch)

        return total_inserted

    def log_ingestion_start(self, payer_name: str, file_url: str) -> int:
        result = self.conn.execute(
            """
            INSERT INTO ingestion_log (payer_name, file_url, status, started_at)
            VALUES (?, ?, 'running', CURRENT_TIMESTAMP)
            RETURNING id
            """,
            [payer_name, file_url],
        ).fetchone()
        return result[0]

    def log_ingestion_complete(self, log_id: int, records_inserted: int):
        self.conn.execute(
            """
            UPDATE ingestion_log
            SET status = 'complete', records_inserted = ?, completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            [records_inserted, log_id],
        )

    def log_ingestion_error(self, log_id: int, error: str):
        self.conn.execute(
            """
            UPDATE ingestion_log
            SET status = 'error', error_message = ?, completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            [error, log_id],
        )

    def is_file_ingested(self, file_url: str) -> bool:
        result = self.conn.execute(
            "SELECT 1 FROM ingestion_log WHERE file_url = ? AND status = 'complete'",
            [file_url],
        ).fetchone()
        return result is not None

    def get_rate_stats(self) -> dict:
        stats = {}

        result = self.conn.execute("SELECT COUNT(*) FROM rates").fetchone()
        stats["total_rates"] = result[0]

        result = self.conn.execute("SELECT COUNT(DISTINCT payer_name) FROM rates").fetchone()
        stats["payers"] = result[0]

        result = self.conn.execute("SELECT COUNT(DISTINCT billing_code) FROM rates").fetchone()
        stats["cpt_codes"] = result[0]

        result = self.conn.execute("SELECT COUNT(DISTINCT npi) FROM rates").fetchone()
        stats["providers"] = result[0]

        return stats

    def query(self, sql: str):
        return self.conn.execute(sql).fetchall()

    def query_df(self, sql: str):
        return self.conn.execute(sql).fetchdf()

    def close(self):
        self.conn.close()
