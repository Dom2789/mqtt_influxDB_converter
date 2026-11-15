from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .errors import ConfigError
from .model import SensorSpec

# [[sensor]] cast must be one of these.
VALID_CASTS = ("float", "int", "str", "bool")

# Environment variables that must all be present and non-empty.
CREDENTIALS = ("INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET")


@dataclass(frozen=True, slots=True)
class MqttConfig:
    """The [mqtt] table."""
    host: str
    port: int = 1883


@dataclass(frozen=True, slots=True)
class InfluxWriteConfig:
    """Credentials from the environment plus the optional [influx] table.

    Only batch_size and flush_interval_ms may appear in config.toml; the four
    credentials come from the environment and are a ConfigError in the file.
    """
    url: str
    token: str
    org: str
    bucket: str
    batch_size: int = 500
    flush_interval_ms: int = 1000


@dataclass(frozen=True, slots=True)
class AppConfig:
    mqtt: MqttConfig
    influx: InfluxWriteConfig
    sensors: tuple[SensorSpec, ...]
    log_level: str = "INFO"


def build_config(raw: Mapping, env: Mapping[str, str]) -> AppConfig:
    """Validate an already-parsed TOML dict plus credentials into an AppConfig.

    Every problem is a ConfigError naming the field (and the sensor, if any).
    """
    ...


def load_config(path: str | Path, env_file: str | Path | None = None) -> AppConfig:
    """Read config.toml and the credentials, then build_config().

    Credentials: dotenv_values(env_file) | os.environ — the process
    environment wins. Without env_file, os.environ alone.
    """
    ...
