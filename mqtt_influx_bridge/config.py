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

# Top-level log_level must be one of these (the names logging understands).
VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

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


def check_topic_filter(topic: str, where: str) -> None:
    """MQTT filter rules: '+' and '#' fill a whole level, '#' only as the last."""
    levels = topic.split("/")
    for i, level in enumerate(levels):
        if level == "#" and i == len(levels) - 1:
            continue
        if level == "+":
            continue
        if not topic or "#" in level or "+" in level:
            raise ConfigError(f"{where}: invalid topic filter {topic!r}")


def build_sensor(raw: Mapping) -> SensorSpec:
    where = f"[[sensor]] {raw.get('name', '?')!r}"

    valid = {f.name for f in fields(SensorSpec)}
    if unknown := unknown_keys(raw.keys(), valid):
        raise ConfigError(f"{where}: unknown key(s) {sorted(unknown)}, valid: {sorted(valid)}")
    for key in ("name", "topic", "measurement", "fields"):
        if key not in raw:
            raise ConfigError(f"{where}: missing required key {key!r}") from None

    check_topic_filter(raw["topic"], where)

    fields_ = raw["fields"]
    if not isinstance(fields_, Mapping) or not fields_:
        raise ConfigError(f"{where}: fields must be a non-empty table")
    for name, key in fields_.items():
        if not isinstance(key, str):
            raise ConfigError(f"{where}: fields.{name} must be a string, got {key!r}")

    cast = raw.get("cast", "float")
    if cast not in VALID_CASTS:
        raise ConfigError(f"{where}: unknown cast {cast!r}, valid: {list(VALID_CASTS)}")

    for name, value in raw.get("tags", {}).items():
        if not isinstance(value, str):
            raise ConfigError(f"{where}: tags.{name} must be a string, got {value!r}")

    # Without '#' the filter fixes the number of levels, so an index past
    # the end can be caught now; with '#' only the real topic can tell.
    levels = raw["topic"].split("/")
    for name, index in raw.get("tags_from_topic", {}).items():
        # bool is a subclass of int, so True would otherwise pass as 1
        if not isinstance(index, int) or isinstance(index, bool) or index < 0:
            raise ConfigError(f"{where}: tags_from_topic.{name} must be an int >= 0, got {index!r}")
        if "#" not in levels and index >= len(levels):
            raise ConfigError(
                f"{where}: tags_from_topic.{name} = {index} is past the last level "
                f"of {raw['topic']!r} ({len(levels)} levels)"
            )

    return SensorSpec(**raw)


def build_sensors(raw: list[Mapping]) -> tuple[SensorSpec, ...]:
    return tuple(build_sensor(sensor) for sensor in raw)


def build_config(raw: Mapping, env: Mapping[str, str]) -> AppConfig:
    """Validate an already-parsed TOML dict plus credentials into an AppConfig.

    Every problem is a ConfigError naming the field (and the sensor, if any).
    """
    valid = {"mqtt", "influx", "sensor", "log_level"}
    if unknown := unknown_keys(raw.keys(), valid):
        raise ConfigError(f"unknown top-level key(s) {sorted(unknown)}, valid: {sorted(valid)}")

    # mqtt section
    raw_mqtt = raw.get("mqtt", {})
    if raw_mqtt == {}:
        raise ConfigError("Config-File has no mqtt-block")
    mqtt = build_mqtt(raw_mqtt)

    # influx section
    raw_influx = raw.get("influx", {})
    influx = build_influx(raw_influx, env)

    # sensors section
    raw_sensors = raw.get("sensor", {})
    if raw_sensors == {}:
        raise ConfigError("Config-File has no sensor-blocks")
    sensors = build_sensors(raw_sensors)

    # log_level, optional
    log_level = raw.get("log_level", "INFO")
    if log_level not in VALID_LOG_LEVELS:
        raise ConfigError(f"unknown log_level {log_level!r}, valid: {list(VALID_LOG_LEVELS)}")

    return AppConfig(mqtt, influx, sensors, log_level)


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

    return build_config(raw, env)


