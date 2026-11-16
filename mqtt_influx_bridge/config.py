import tomllib

from os import environ
from pprint import pprint

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

def build_mqtt(raw:Mapping) -> MqttConfig:
    if "host" not in raw:
        raise ConfigError("[mqtt]: missing required key 'host'")
    valid = {f.name for f in fields(MqttConfig)}
    if unknown := unknown_keys(raw.keys(), valid):
        raise ConfigError(f"[mqtt]: unknown key(s) {sorted(unknown)}, valid: {sorted(valid)}")
    if "port" in raw.keys():
        mqtt = MqttConfig(raw["host"], raw["port"])
    else:
        mqtt = MqttConfig(raw["host"])

    return mqtt


def build_influx(raw: Mapping, env: Mapping[str, str]) -> InfluxWriteConfig:
    valid = {"batch_size", "flush_interval_ms"}
    if unknown := unknown_keys(raw.keys(), valid):
        raise ConfigError(f"[influx]: unknown key(s) {sorted(unknown)}, valid: {sorted(valid)}")

    creds = {}
    for var in CREDENTIALS:
        if not env.get(var):  # missing, None, or ""
            raise ConfigError(f"missing or empty credential: {var}")
        creds[var.removeprefix("INFLUX_").lower()] = env[var]

    return InfluxWriteConfig(**creds, **raw)


def build_sensors(raw:tuple[SensorSpec, ...]) -> tuple[SensorSpec, ...]:
    sensors = tuple()
    return sensors


def build_config(raw: Mapping, env: Mapping[str, str]) -> AppConfig:
    """Validate an already-parsed TOML dict plus credentials into an AppConfig.

    Every problem is a ConfigError naming the field (and the sensor, if any).
    """
    # mqtt section
    raw_mqtt = raw.get("mqtt", {})
    if raw_mqtt == {}:
        raise ConfigError("Config-File has no mqtt-block")
    mqtt = build_mqtt(raw_mqtt)

    # influx section
    raw_influx = raw.get("influx", {})
    influx = build_influx(raw_influx, env)

    # sensors section
    raw_sensors = raw.get("sensor")
    sensors = build_sensors(raw_sensors)

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

    pprint(raw)

    build_config(raw, env)


