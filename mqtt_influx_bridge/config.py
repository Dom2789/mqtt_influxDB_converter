"""TOML -> frozen dataclasses, validated eagerly at startup.

Secrets are deliberately *not* in config.toml: InfluxDB credentials come from the
process environment (which the systemd EnvironmentFile populates) or from an
explicit env file, so config.toml stays safe to commit.
"""

import os
from dataclasses import dataclass, field as dc_field
from collections.abc import Mapping, Sequence
from os import environ
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from . import decoders
from ._lib.ConfigToml import ConfigToml
from .routing import InvalidTopicFilter, max_safe_topic_index, validate_topic_filter

INFLUX_KEYS = ("INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET")


class ConfigError(Exception):
    """Configuration is unusable. Always names the offending field."""


@dataclass(frozen=True, slots=True)
class InfluxCredentials:
    url: str
    token: str
    org: str
    bucket: str

    @classmethod
    def from_env(cls, path: str | os.PathLike[str] | None = None) -> "InfluxCredentials":
        """Load credentials, with the process environment taking precedence.

        That precedence is what the systemd deployment relies on: EnvironmentFile
        injects the values into the environment, so no file read is needed there.
        """
        if path is None:
            env: Mapping[str, str | None] = environ
            source = "the process environment"
        else:
            if not Path(path).is_file():
                raise ConfigError(f"env file not found: {path}")
            env = dotenv_values(path) | environ
            source = f"{path} or the process environment"

        missing = [k for k in INFLUX_KEYS if not env.get(k)]
        if missing:
            raise ConfigError(
                f"missing InfluxDB credential(s) {', '.join(missing)} in {source}"
            )
        return cls(
            url=str(env["INFLUX_URL"]),
            token=str(env["INFLUX_TOKEN"]),
            org=str(env["INFLUX_ORG"]),
            bucket=str(env["INFLUX_BUCKET"]),
        )


@dataclass(frozen=True, slots=True)
class MqttConfig:
    host: str
    port: int = 1883
    client_id: str = "mqtt-influx-bridge"
    keepalive: int = 60
    qos: int = 1
    reconnect_min_delay_s: int = 1
    reconnect_max_delay_s: int = 120
    username: str | None = None


@dataclass(frozen=True, slots=True)
class InfluxWriteConfig:
    batch_size: int = 500
    flush_interval_ms: int = 1000
    max_retries: int = 5


@dataclass(frozen=True, slots=True)
class QueueConfig:
    maxsize: int = 10_000


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    level: str = "INFO"
    target: str = "stdout"
    path: str | None = None
    name: str | None = None
    add_date_to_name: bool = False


@dataclass(frozen=True, slots=True)
class FileSinkConfig:
    enabled: bool = False
    path: str = "."
    file_name: str = "readings.log"


@dataclass(frozen=True, slots=True)
class SensorSpec:
    name: str
    topic: str
    decoder: str
    measurement: str
    fields: Mapping[str, str]
    tags: Mapping[str, str] = dc_field(default_factory=dict)
    tags_from_topic: Mapping[str, int] = dc_field(default_factory=dict)
    decoder_options: Mapping[str, Any] = dc_field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AppConfig:
    mqtt: MqttConfig
    sensors: tuple[SensorSpec, ...]
    influx_write: InfluxWriteConfig = InfluxWriteConfig()
    queue: QueueConfig = QueueConfig()
    logging: LoggingConfig = LoggingConfig()
    file_sink: FileSinkConfig = FileSinkConfig()


def _section(raw: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = raw.get(name, {})
    if not isinstance(value, Mapping):
        raise ConfigError(f"[{name}] must be a table, got {type(value).__name__}")
    return value


def _reject_unknown(section: str, got: Mapping[str, Any], known: Sequence[str]) -> None:
    unknown = sorted(set(got) - set(known))
    if unknown:
        raise ConfigError(
            f"[{section}]: unknown key(s) {', '.join(unknown)}; "
            f"valid keys: {', '.join(sorted(known))}"
        )


def _int(section: str, key: str, value: Any, lo: int, hi: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"[{section}].{key} must be an integer, got {value!r}")
    if not lo <= value <= hi:
        raise ConfigError(f"[{section}].{key} must be in [{lo}, {hi}], got {value}")
    return value


def _mqtt(raw: Mapping[str, Any]) -> MqttConfig:
    if "mqtt" not in raw:
        raise ConfigError("[mqtt] section is required")
    section = _section(raw, "mqtt")
    known = (
        "host", "port", "client_id", "keepalive", "qos",
        "reconnect_min_delay_s", "reconnect_max_delay_s", "username",
    )
    _reject_unknown("mqtt", section, known)

    host = section.get("host")
    if not isinstance(host, str) or not host:
        raise ConfigError("[mqtt].host is required and must be a non-empty string")

    defaults = MqttConfig(host=host)
    port = _int("mqtt", "port", section.get("port", defaults.port), 1, 65535)
    qos = _int("mqtt", "qos", section.get("qos", defaults.qos), 0, 2)
    keepalive = _int(
        "mqtt", "keepalive", section.get("keepalive", defaults.keepalive), 1, 65535
    )
    rmin = _int(
        "mqtt", "reconnect_min_delay_s",
        section.get("reconnect_min_delay_s", defaults.reconnect_min_delay_s), 1, 3600,
    )
    rmax = _int(
        "mqtt", "reconnect_max_delay_s",
        section.get("reconnect_max_delay_s", defaults.reconnect_max_delay_s), 1, 86400,
    )
    if rmin > rmax:
        raise ConfigError(
            f"[mqtt].reconnect_min_delay_s ({rmin}) must not exceed "
            f"reconnect_max_delay_s ({rmax})"
        )

    username = section.get("username")
    if username is not None and not isinstance(username, str):
        raise ConfigError("[mqtt].username must be a string")

    return MqttConfig(
        host=host,
        port=port,
        client_id=str(section.get("client_id", defaults.client_id)),
        keepalive=keepalive,
        qos=qos,
        reconnect_min_delay_s=rmin,
        reconnect_max_delay_s=rmax,
        username=username,
    )


def _influx_write(raw: Mapping[str, Any]) -> InfluxWriteConfig:
    section = _section(raw, "influx")
    # checked before _reject_unknown so a leaked secret gets the specific message
    for leaked in ("token", "url", "org", "bucket"):
        if leaked in section:
            raise ConfigError(
                f"[influx].{leaked} must not live in config.toml — InfluxDB "
                f"credentials come from the environment or --env-file"
            )
    known = ("batch_size", "flush_interval_ms", "max_retries")
    _reject_unknown("influx", section, known)
    d = InfluxWriteConfig()
    return InfluxWriteConfig(
        batch_size=_int(
            "influx", "batch_size", section.get("batch_size", d.batch_size), 1, 100_000
        ),
        flush_interval_ms=_int(
            "influx", "flush_interval_ms",
            section.get("flush_interval_ms", d.flush_interval_ms), 10, 3_600_000,
        ),
        max_retries=_int(
            "influx", "max_retries", section.get("max_retries", d.max_retries), 0, 100
        ),
    )


def _queue(raw: Mapping[str, Any]) -> QueueConfig:
    section = _section(raw, "queue")
    _reject_unknown("queue", section, ("maxsize",))
    d = QueueConfig()
    return QueueConfig(
        maxsize=_int("queue", "maxsize", section.get("maxsize", d.maxsize), 1, 10_000_000)
    )


def _logging(raw: Mapping[str, Any]) -> LoggingConfig:
    section = _section(raw, "logging")
    known = ("level", "target", "path", "name", "add_date_to_name")
    _reject_unknown("logging", section, known)
    d = LoggingConfig()
    target = str(section.get("target", d.target))
    if target not in ("stdout", "file"):
        raise ConfigError(f'[logging].target must be "stdout" or "file", got {target!r}')
    level = str(section.get("level", d.level)).upper()
    import logging as _logging_mod

    if level not in _logging_mod.getLevelNamesMapping():
        raise ConfigError(f"[logging].level {level!r} is not a valid level name")
    path = section.get("path", d.path)
    name = section.get("name", d.name)
    if target == "file" and not (path and name):
        raise ConfigError('[logging]: target = "file" requires both path and name')
    return LoggingConfig(
        level=level,
        target=target,
        path=str(path) if path else None,
        name=str(name) if name else None,
        add_date_to_name=bool(section.get("add_date_to_name", d.add_date_to_name)),
    )


def _file_sink(raw: Mapping[str, Any]) -> FileSinkConfig:
    section = _section(raw, "file_sink")
    known = ("enabled", "path", "file_name")
    _reject_unknown("file_sink", section, known)
    d = FileSinkConfig()
    enabled = bool(section.get("enabled", d.enabled))
    path = str(section.get("path", d.path))
    file_name = str(section.get("file_name", d.file_name))
    if enabled and not Path(path).is_dir():
        raise ConfigError(
            f"[file_sink].path {path!r} is not an existing directory "
            f"(the systemd unit also needs it in ReadWritePaths)"
        )
    return FileSinkConfig(enabled=enabled, path=path, file_name=file_name)


def _normalise_fields(name: str, block: Mapping[str, Any]) -> dict[str, str]:
    """Accept either `field = "x"` (one field) or `fields = { x = "src" }`."""
    has_field, has_fields = "field" in block, "fields" in block
    if has_field and has_fields:
        raise ConfigError(f"sensor {name!r}: use either field or fields, not both")

    if has_field:
        single = block["field"]
        if not isinstance(single, str) or not single:
            raise ConfigError(f"sensor {name!r}: field must be a non-empty string")
        return {single: single}

    if not has_fields:
        raise ConfigError(f"sensor {name!r}: either field or fields is required")

    fields = block["fields"]
    if not isinstance(fields, Mapping) or not fields:
        raise ConfigError(f"sensor {name!r}: fields must be a non-empty table")
    out: dict[str, str] = {}
    for key, source in fields.items():
        if not isinstance(source, str) or not source:
            raise ConfigError(
                f"sensor {name!r}: fields.{key} must be a non-empty string naming "
                f"the source key"
            )
        out[str(key)] = source
    return out


def _sensor(block: Mapping[str, Any], index: int) -> SensorSpec:
    label = f"[[sensor]] #{index + 1}"
    raw_name = block.get("name")
    if not isinstance(raw_name, str) or not raw_name:
        raise ConfigError(f"{label}: name is required and must be a non-empty string")
    name = raw_name

    known = (
        "name", "topic", "decoder", "measurement", "field", "fields",
        "tags", "tags_from_topic", "decoder_options",
    )
    _reject_unknown(f"sensor {name!r}", block, known)

    topic = block.get("topic")
    if not isinstance(topic, str):
        raise ConfigError(f"sensor {name!r}: topic is required and must be a string")
    try:
        validate_topic_filter(topic)
    except InvalidTopicFilter as e:
        raise ConfigError(f"sensor {name!r}: {e}") from e

    decoder = block.get("decoder")
    if not isinstance(decoder, str):
        raise ConfigError(f"sensor {name!r}: decoder is required and must be a string")
    if decoder not in decoders.available():
        raise ConfigError(
            f"sensor {name!r}: unknown decoder {decoder!r}; "
            f"available: {', '.join(decoders.available())}"
        )

    measurement = block.get("measurement")
    if not isinstance(measurement, str) or not measurement:
        raise ConfigError(
            f"sensor {name!r}: measurement is required and must be a non-empty string"
        )

    fields = _normalise_fields(name, block)
    if decoder == "scalar" and len(fields) != 1:
        raise ConfigError(
            f"sensor {name!r}: the scalar decoder produces exactly one field, "
            f"but {len(fields)} were declared ({', '.join(sorted(fields))})"
        )

    tags_raw = block.get("tags", {})
    if not isinstance(tags_raw, Mapping):
        raise ConfigError(f"sensor {name!r}: tags must be a table")
    tags: dict[str, str] = {}
    for key, value in tags_raw.items():
        if not isinstance(value, str):
            raise ConfigError(
                f"sensor {name!r}: tags.{key} must be a string — InfluxDB tag "
                f"values are always strings, got {type(value).__name__}"
            )
        tags[str(key)] = value

    tft_raw = block.get("tags_from_topic", {})
    if not isinstance(tft_raw, Mapping):
        raise ConfigError(f"sensor {name!r}: tags_from_topic must be a table")
    safe_max = max_safe_topic_index(topic)
    tags_from_topic: dict[str, int] = {}
    for key, value in tft_raw.items():
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(
                f"sensor {name!r}: tags_from_topic.{key} must be an integer "
                f"topic-level index, got {value!r}"
            )
        if value < 0:
            raise ConfigError(
                f"sensor {name!r}: tags_from_topic.{key} must be >= 0, got {value}"
            )
        if safe_max is not None and value > safe_max:
            raise ConfigError(
                f"sensor {name!r}: tags_from_topic.{key} = {value} is out of range "
                f"for topic {topic!r}, which has {safe_max + 1} level(s)"
            )
        tags_from_topic[str(key)] = value

    if overlap := sorted(set(tags) & set(tags_from_topic)):
        raise ConfigError(
            f"sensor {name!r}: tag(s) {', '.join(overlap)} defined in both tags "
            f"and tags_from_topic"
        )

    options = block.get("decoder_options", {})
    if not isinstance(options, Mapping):
        raise ConfigError(f"sensor {name!r}: decoder_options must be a table")

    return SensorSpec(
        name=name,
        topic=topic,
        decoder=decoder,
        measurement=measurement,
        fields=fields,
        tags=tags,
        tags_from_topic=tags_from_topic,
        decoder_options=dict(options),
    )


def _sensors(raw: Mapping[str, Any]) -> tuple[SensorSpec, ...]:
    blocks = raw.get("sensor", ())
    if isinstance(blocks, Mapping):
        raise ConfigError(
            "sensors must be declared as [[sensor]] array-of-tables, not [sensor]"
        )
    if not isinstance(blocks, (list, tuple)) or not blocks:
        raise ConfigError("at least one [[sensor]] block is required")

    specs: list[SensorSpec] = []
    seen: dict[str, int] = {}
    for i, block in enumerate(blocks):
        if not isinstance(block, Mapping):
            raise ConfigError(f"[[sensor]] #{i + 1} must be a table")
        spec = _sensor(block, i)
        if spec.name in seen:
            raise ConfigError(
                f"duplicate sensor name {spec.name!r} "
                f"(blocks #{seen[spec.name] + 1} and #{i + 1})"
            )
        seen[spec.name] = i
        specs.append(spec)
    return tuple(specs)


def load_config(path: str | os.PathLike[str]) -> AppConfig:
    """Parse and fully validate config.toml. Raises ConfigError on any problem."""
    try:
        raw = ConfigToml(path)
    except FileNotFoundError as e:
        raise ConfigError(f"config file not found: {path}") from e
    except OSError as e:
        raise ConfigError(f"cannot read config file {path}: {e}") from e
    except ValueError as e:  # TOMLDecodeError subclasses ValueError
        raise ConfigError(f"invalid TOML: {e}") from e

    known_sections = ("mqtt", "influx", "queue", "logging", "file_sink", "sensor")
    _reject_unknown("config.toml", raw, known_sections)

    return AppConfig(
        mqtt=_mqtt(raw),
        sensors=_sensors(raw),
        influx_write=_influx_write(raw),
        queue=_queue(raw),
        logging=_logging(raw),
        file_sink=_file_sink(raw),
    )
