"""
etl/config.py
─────────────
Single source of truth for all configuration.
Reads from environment variables (set via .env + docker-compose).
"""

import os
from dataclasses import dataclass, field

# ── Target BINs (from ТЗ) ─────────────────────────────────────────────────────
TARGET_BINS: list[str] = [
    "000740001307", "020240002363", "020440003656", "030440003698",
    "050740004819", "051040005150", "100140011059", "120940001946",
    "140340016539", "150540000186", "171041003124", "210240019348",
    "210240033968", "210941010761", "230740013340", "231040023028",
    "780140000023", "900640000128", "940740000911", "940940000384",
    "960440000220", "970940001378", "971040001050", "980440001034",
    "981140001551", "990340005977", "990740002243",
]

DATA_YEARS: list[int] = [2024, 2025, 2026]
DATE_FROM = "2024-01-01"
DATE_TO   = "2026-12-31"


@dataclass(frozen=True)
class Config:
    # OWS API
    ows_token:       str = field(default_factory=lambda: os.environ["OWS_TOKEN"])
    ows_base_url:    str = field(default_factory=lambda: os.environ.get("OWS_BASE_URL", "https://ows.goszakup.gov.kz"))
    ows_rps:         float = field(default_factory=lambda: float(os.environ.get("OWS_RATE_LIMIT_RPS", "5")))

    # AI / LLM
    ai_api_key:      str = field(default_factory=lambda: os.environ.get("AI_API_KEY", ""))
    ai_base_url:     str = field(default_factory=lambda: os.environ.get("AI_BASE_URL", "https://nitec-ai.kz/api"))
    ai_model:        str = field(default_factory=lambda: os.environ.get("AI_MODEL", "openai/gpt-oss-120b"))

    # Postgres
    db_url:          str = field(default_factory=lambda: os.environ["DATABASE_URL"])

    # Redis
    redis_url:       str = field(default_factory=lambda: os.environ.get("REDIS_URL", "redis://redis:6379/0"))

    # App
    log_level:       str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))
    env:             str = field(default_factory=lambda: os.environ.get("APP_ENV", "development"))

    @property
    def is_dev(self) -> bool:
        return self.env == "development"


def get_config() -> Config:
    return Config()
