"""
etl/config.py
─────────────
Single source of truth for all configuration.
Reads from environment variables (set via .env + docker-compose).
"""

import os
from dataclasses import dataclass, field

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_TARGET_BINS: list[str] = [
    "000740001307", "020240002363", "020440003656", "030440003698",
    "050740004819", "051040005150", "100140011059", "120940001946",
    "140340016539", "150540000186", "171041003124", "210240019348",
    "210240033968", "210941010761", "230740013340", "231040023028",
    "780140000023", "900640000128", "940740000911", "940940000384",
    "960440000220", "970940001378", "971040001050", "980440001034",
    "981140001551", "990340005977", "990740002243",
]

DEFAULT_DATA_YEAR_FROM = 2024
DEFAULT_DATA_YEAR_TO = 2026
DEFAULT_RESUME_BACKSTEP = 20000
DEFAULT_ETL_CONCURRENCY = 2


def _parse_target_bins(raw: str) -> tuple[str, ...]:
    bins = [b.strip() for b in raw.split(",") if b.strip()]
    for b in bins:
        if len(b) != 12 or not b.isdigit():
            raise ValueError(f"Invalid BIN in TARGET_BINS: {b!r} (expected 12 digits)")
    # Preserve order, drop duplicates.
    seen: set[str] = set()
    out: list[str] = []
    for b in bins:
        if b not in seen:
            seen.add(b)
            out.append(b)
    return tuple(out)


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
    ai_max_tokens:   int = field(default_factory=lambda: int(os.environ.get("AI_MAX_TOKENS", "4096")))
    ai_temperature:  float = field(default_factory=lambda: float(os.environ.get("AI_TEMPERATURE", "0.1")))

    # Postgres
    db_url:          str = field(default_factory=lambda: os.environ["DATABASE_URL"])

    # Redis
    redis_url:       str = field(default_factory=lambda: os.environ.get("REDIS_URL", "redis://localhost:6379/0"))

    # App
    log_level:       str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))
    env:             str = field(default_factory=lambda: os.environ.get("APP_ENV", "development"))

    # Scope
    target_bins:     tuple[str, ...] = field(
        default_factory=lambda: _parse_target_bins(os.environ.get("TARGET_BINS", "")) or tuple(DEFAULT_TARGET_BINS)
    )
    data_year_from:  int = field(default_factory=lambda: int(os.environ.get("DATA_YEAR_FROM", str(DEFAULT_DATA_YEAR_FROM))))
    data_year_to:    int = field(default_factory=lambda: int(os.environ.get("DATA_YEAR_TO", str(DEFAULT_DATA_YEAR_TO))))
    resume_backstep: int = field(default_factory=lambda: int(os.environ.get("RESUME_BACKSTEP", str(DEFAULT_RESUME_BACKSTEP))))
    etl_concurrency: int = field(default_factory=lambda: int(os.environ.get("ETL_CONCURRENCY", str(DEFAULT_ETL_CONCURRENCY))))

    def __post_init__(self) -> None:
        if self.data_year_from > self.data_year_to:
            raise ValueError(f"DATA_YEAR_FROM ({self.data_year_from}) must be <= DATA_YEAR_TO ({self.data_year_to})")
        if self.resume_backstep < 0:
            raise ValueError("RESUME_BACKSTEP must be >= 0")
        if self.etl_concurrency < 1:
            raise ValueError("ETL_CONCURRENCY must be >= 1")

    @property
    def is_dev(self) -> bool:
        return self.env == "development"

    @property
    def date_from(self) -> str:
        return f"{self.data_year_from:04d}-01-01"

    @property
    def date_to(self) -> str:
        return f"{self.data_year_to:04d}-12-31"

    @property
    def data_years(self) -> list[int]:
        return list(range(self.data_year_from, self.data_year_to + 1))


def get_config() -> Config:
    return Config()
