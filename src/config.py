"""Configuration loading and validation."""

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel


class PayerConfig(BaseModel):
    name: str
    index_url: str
    enabled: bool = True
    notes: Optional[str] = None
    additional_files: Optional[list[str]] = None


class GeographyConfig(BaseModel):
    states: list[str] = ["MN"]
    zip_prefixes: Optional[list[str]] = None


class PayersConfig(BaseModel):
    payers: list[PayerConfig]
    geography: GeographyConfig = GeographyConfig()


class CPTConfig(BaseModel):
    cpt_codes: list[str]


def get_config_dir() -> Path:
    return Path(__file__).parent.parent / "config"


def get_data_dir() -> Path:
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir


def load_payers_config() -> PayersConfig:
    config_path = get_config_dir() / "payers.yaml"
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return PayersConfig(**data)


def load_cpt_codes() -> set[str]:
    config_path = get_config_dir() / "cpt_codes.yaml"
    with open(config_path) as f:
        data = yaml.safe_load(f)
    config = CPTConfig(**data)
    return set(config.cpt_codes)


def get_enabled_payers() -> list[PayerConfig]:
    config = load_payers_config()
    return [p for p in config.payers if p.enabled and p.index_url]


def get_target_states() -> set[str]:
    config = load_payers_config()
    return set(config.geography.states)
