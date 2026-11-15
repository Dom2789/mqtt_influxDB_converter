import tomllib

from os import environ
from dotenv import dotenv_values

from collections.abc import Mapping, Set
from dataclasses import dataclass, fields
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

def unknown_keys(keys_config:Set[str], keys_dataclass: Set[str]) -> Set[str]:
    return keys_config - keys_dataclass

def build_config(raw: Mapping, env: Mapping[str, str]) -> AppConfig:
    """Validate an already-parsed TOML dict plus credentials into an AppConfig.

    Every problem is a ConfigError naming the field (and the sensor, if any).
    """
    # mqtt section
    raw_mqtt = raw.get("mqtt", {})
    if raw_mqtt == {}:
        raise ConfigError("Config-File has no mqtt-block")
    if "host" not in raw_mqtt:
        raise ConfigError("[mqtt]: missing required key 'host'")
    valid = {f.name for f in fields(MqttConfig)}
    if unknown := unknown_keys(raw_mqtt.keys(), valid):
        raise ConfigError(f"[mqtt]: unknown key(s) {sorted(unknown)}, valid: {sorted(valid)}")
    if "port" in raw_mqtt.keys():
        mqtt = MqttConfig(raw_mqtt["host"], raw_mqtt["port"])
    else:
        mqtt = MqttConfig(raw_mqtt["host"])

    # influx section
    raw_influx = raw.get("influx", {})

    valid = {k for k in CREDENTIALS}
    if unknown := unknown_keys(env.keys(), valid):
        raise ConfigError(f"influx.env: unknown key(s) {sorted(unknown)}, valid: {sorted(valid)}")

    for key, value in env.items():
        if not value:
            raise ConfigError(f"influx.env file has empty field: {key}")
        else:
            raw_influx[key] = value

    influx = None

    # sensors section
    sensors = None

    print(AppConfig(mqtt, influx, sensors))
    return AppConfig(mqtt, influx, sensors)


def load_config(path: str | Path, env_file: str | Path | None = None) -> AppConfig:
    """Read config.toml and the credentials, then build_config().

    Credentials: dotenv_values(env_file) | os.environ — the process
    environment wins. Without env_file, os.environ alone.
    """
    try:
        with open(path, "rb") as f:  # tomllib requires binary mode
            raw = tomllib.load(f)
    except OSError as e:
        raise ConfigError(f"{path}: cannot read config file: {e.strerror}") from None
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{path}: invalid TOML: {e}") from None

    file_env = dotenv_values(env_file) if env_file is not None else {}
    env = {k: environ.get(k, file_env.get(k)) for k in CREDENTIALS}

    print(raw)

    build_config(raw, env)


