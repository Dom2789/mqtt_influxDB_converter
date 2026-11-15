"""Spec for mqtt_influx_bridge.config — implementation plan step 5.

Written before the loader exists, so the first run fails. Make these pass by
writing build_config() and load_config(). Every ConfigError test asserts that
the offending field is named in the message — that is the point of the step.
"""

import tomllib
from pathlib import Path

import pytest

from mqtt_influx_bridge.config import (
    AppConfig,
    InfluxWriteConfig,
    MqttConfig,
    build_config,
    load_config,
)
from mqtt_influx_bridge.errors import ConfigError
from mqtt_influx_bridge.model import SensorSpec

REPO = Path(__file__).resolve().parent.parent

ENV = {
    "INFLUX_URL": "http://influx:8086",
    "INFLUX_TOKEN": "tok",
    "INFLUX_ORG": "homelab",
    "INFLUX_BUCKET": "telemetry",
}


def sensor(**overrides) -> dict:
    """A valid [[sensor]] table; pass key=None to drop a key."""
    s = {
        "name": "probe",
        "topic": "temp/+/+",
        "measurement": "environment",
        "fields": {"watertemperature": "temp"},
    } | overrides
    return {k: v for k, v in s.items() if v is not None}


def raw(*sensors: dict, **top) -> dict:
    """A valid top-level config; pass key=None to drop a key."""
    r = {"mqtt": {"host": "broker"}, "sensor": list(sensors) or [sensor()]} | top
    return {k: v for k, v in r.items() if v is not None}


def config_error(r: dict, env: dict = ENV) -> str:
    with pytest.raises(ConfigError) as exc:
        build_config(r, env)
    return str(exc.value)


# --------------------------------------------------------------------------
# Round trip
# --------------------------------------------------------------------------

def test_minimal_config_fills_defaults():
    assert build_config(raw(), ENV) == AppConfig(
        mqtt=MqttConfig(host="broker", port=1883),
        influx=InfluxWriteConfig(
            url="http://influx:8086",
            token="tok",
            org="homelab",
            bucket="telemetry",
        ),
        sensors=(
            SensorSpec(
                name="probe",
                topic="temp/+/+",
                measurement="environment",
                fields={"watertemperature": "temp"},
                cast="float",
                tags={},
                tags_from_topic={},
            ),
        ),
        log_level="INFO",
    )


FULL_TOML = """
log_level = "DEBUG"

[mqtt]
host = "192.168.1.225"
port = 1884

[influx]
batch_size        = 100
flush_interval_ms = 5000

[[sensor]]
name        = "water temperature techroom"
topic       = "temp/techroom/1"
measurement = "environment"
cast        = "float"
fields      = { watertemperature = "temp" }
tags        = { room = "techroom" }

[[sensor]]
name            = "shelly powermeter fleet"
topic           = "powermeter/+/+/status/+"
measurement     = "power"
cast            = "int"
tags_from_topic = { room = 1, device = 2, channel = 4 }

  [sensor.fields]
  energy  = "aenergy.total"
  on_time = "counts.on_time"
"""


def test_every_key_round_trips():
    cfg = build_config(tomllib.loads(FULL_TOML), ENV)
    assert cfg == AppConfig(
        mqtt=MqttConfig(host="192.168.1.225", port=1884),
        influx=InfluxWriteConfig(
            url="http://influx:8086",
            token="tok",
            org="homelab",
            bucket="telemetry",
            batch_size=100,
            flush_interval_ms=5000,
        ),
        sensors=(
            SensorSpec(
                name="water temperature techroom",
                topic="temp/techroom/1",
                measurement="environment",
                fields={"watertemperature": "temp"},
                cast="float",
                tags={"room": "techroom"},
            ),
            SensorSpec(
                name="shelly powermeter fleet",
                topic="powermeter/+/+/status/+",
                measurement="power",
                fields={"energy": "aenergy.total", "on_time": "counts.on_time"},
                cast="int",
                tags_from_topic={"room": 1, "device": 2, "channel": 4},
            ),
        ),
        log_level="DEBUG",
    )


def test_sensors_is_a_tuple():
    # frozen all the way down: a list here could still be appended to
    assert isinstance(build_config(raw(), ENV).sensors, tuple)


@pytest.mark.parametrize("name", ["config.toml", "config.toml.example"])
def test_committed_config_files_are_valid(name, monkeypatch):
    # Keeps the committed example from drifting out of validity.
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    cfg = load_config(REPO / name)
    assert len(cfg.sensors) >= 1


# --------------------------------------------------------------------------
# Unknown keys — the one check that catches a silent typo
# --------------------------------------------------------------------------

def test_unknown_top_level_key():
    msg = config_error(raw(loglevel="DEBUG"))
    assert "loglevel" in msg
    assert "log_level" in msg            # the valid keys are listed


def test_unknown_mqtt_key():
    msg = config_error(raw(mqtt={"host": "broker", "prot": 1883}))
    assert "prot" in msg
    assert "port" in msg


def test_unknown_influx_key():
    msg = config_error(raw(influx={"batchsize": 100}))
    assert "batchsize" in msg
    assert "batch_size" in msg


def test_unknown_sensor_key():
    # "tag" instead of "tags" would otherwise silently stop tagging
    msg = config_error(raw(sensor(tag={"room": "techroom"})))
    assert "'tag'" in msg or '"tag"' in msg
    assert "tags_from_topic" in msg
    assert "probe" in msg                # names the sensor


@pytest.mark.parametrize("key", ["url", "token", "org", "bucket"])
def test_credentials_in_the_file_are_rejected(key):
    # credentials come from the environment only
    msg = config_error(raw(influx={key: "secret"}))
    assert key in msg


def test_field_names_are_free_form():
    # keys inside fields are Influx field names, chosen by the user
    cfg = build_config(raw(sensor(fields={"anything_at_all": "x.y.z"})), ENV)
    assert cfg.sensors[0].fields == {"anything_at_all": "x.y.z"}


# --------------------------------------------------------------------------
# Missing required keys — ConfigError, never KeyError
# --------------------------------------------------------------------------

@pytest.mark.parametrize("key", ["name", "topic", "measurement", "fields"])
def test_missing_sensor_key(key):
    msg = config_error(raw(sensor(**{key: None})))
    assert key in msg


def test_missing_mqtt_table():
    assert "mqtt" in config_error(raw(mqtt=None))


def test_missing_mqtt_host():
    assert "host" in config_error(raw(mqtt={"port": 1883}))


def test_no_sensors():
    assert "sensor" in config_error(raw(sensor=None))


def test_missing_key_is_not_chained_to_a_key_error():
    # re-raise "from None": no KeyError in the traceback, no repr()-quoted message
    with pytest.raises(ConfigError) as exc:
        build_config(raw(sensor(topic=None)), ENV)
    assert exc.value.__cause__ is None
    assert exc.value.__suppress_context__


# --------------------------------------------------------------------------
# fields and cast
# --------------------------------------------------------------------------

def test_empty_fields_table():
    msg = config_error(raw(sensor(fields={})))
    assert "fields" in msg
    assert "probe" in msg


def test_fields_value_must_be_a_string():
    msg = config_error(raw(sensor(fields={"watertemperature": 1})))
    assert "watertemperature" in msg


def test_fields_must_be_a_table():
    assert "fields" in config_error(raw(sensor(fields="temp")))


def test_unknown_cast():
    msg = config_error(raw(sensor(cast="double")))
    assert "cast" in msg
    assert "double" in msg
    assert "float" in msg                # the valid casts are listed
    assert "probe" in msg


@pytest.mark.parametrize("cast", ["float", "int", "str", "bool"])
def test_every_valid_cast(cast):
    assert build_config(raw(sensor(cast=cast)), ENV).sensors[0].cast == cast


# --------------------------------------------------------------------------
# tags and tags_from_topic
# --------------------------------------------------------------------------

def test_non_string_tag_value():
    msg = config_error(raw(sensor(tags={"room": 1})))
    assert "tags" in msg
    assert "room" in msg


def test_negative_tags_from_topic_index():
    # the dangerous one from step 03: -1 would quietly tag the last level
    msg = config_error(raw(sensor(tags_from_topic={"room": -1})))
    assert "tags_from_topic" in msg
    assert "room" in msg


def test_tags_from_topic_index_must_be_an_int():
    msg = config_error(raw(sensor(tags_from_topic={"room": "1"})))
    assert "room" in msg


def test_tags_from_topic_index_past_a_fixed_length_pattern():
    # temp/+/+ always has exactly three levels: 0, 1, 2
    msg = config_error(raw(sensor(topic="temp/+/+", tags_from_topic={"device": 3})))
    assert "device" in msg


def test_tags_from_topic_last_level_of_fixed_pattern_is_fine():
    cfg = build_config(raw(sensor(topic="temp/+/+", tags_from_topic={"device": 2})), ENV)
    assert cfg.sensors[0].tags_from_topic == {"device": 2}


def test_tags_from_topic_past_a_hash_pattern_is_left_to_runtime():
    # temp/# has unbounded length; only tags_from_topic() can decide per message
    cfg = build_config(raw(sensor(topic="temp/#", tags_from_topic={"device": 5})), ENV)
    assert cfg.sensors[0].tags_from_topic == {"device": 5}


# --------------------------------------------------------------------------
# Topic filters
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "topic",
    [
        pytest.param("temp/#/x", id="hash-not-last"),
        pytest.param("temp/a#", id="hash-inside-level"),
        pytest.param("temp/+x", id="plus-inside-level"),
        pytest.param("", id="empty"),
    ],
)
def test_malformed_topic_filter(topic):
    msg = config_error(raw(sensor(topic=topic)))
    assert "topic" in msg
    assert "probe" in msg


@pytest.mark.parametrize("topic", ["#", "temp/#", "+/+", "temp/techroom/1", "powermeter/+/+/status/+"])
def test_valid_topic_filter(topic):
    assert build_config(raw(sensor(topic=topic)), ENV).sensors[0].topic == topic


# --------------------------------------------------------------------------
# Credentials
# --------------------------------------------------------------------------

@pytest.mark.parametrize("var", list(ENV))
def test_missing_credential(var):
    env = {k: v for k, v in ENV.items() if k != var}
    assert var in config_error(raw(), env)


@pytest.mark.parametrize("var", list(ENV))
def test_empty_credential_counts_as_missing(var):
    # "INFLUX_TOKEN=" in an env file gives "", not a missing key
    assert var in config_error(raw(), ENV | {var: ""})


# --------------------------------------------------------------------------
# load_config: files and the environment
# --------------------------------------------------------------------------

MINIMAL_TOML = """
[mqtt]
host = "broker"

[[sensor]]
name        = "probe"
topic       = "temp/+/+"
measurement = "environment"
fields      = { watertemperature = "temp" }
"""


@pytest.fixture
def clean_env(monkeypatch):
    for key in ENV:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


@pytest.fixture
def config_file(tmp_path) -> Path:
    p = tmp_path / "config.toml"
    p.write_text(MINIMAL_TOML)
    return p


def write_env_file(tmp_path: Path, env: dict) -> Path:
    p = tmp_path / "influx.env"
    p.write_text("".join(f"{k}={v}\n" for k, v in env.items()))
    return p


def test_credentials_from_env_file(clean_env, config_file, tmp_path):
    cfg = load_config(config_file, write_env_file(tmp_path, ENV))
    assert cfg.influx.token == "tok"
    assert cfg.influx.bucket == "telemetry"


def test_credentials_from_process_environment(clean_env, config_file):
    for key, value in ENV.items():
        clean_env.setenv(key, value)
    assert load_config(config_file).influx.url == "http://influx:8086"


def test_process_environment_wins_over_env_file(clean_env, config_file, tmp_path):
    # what the systemd EnvironmentFile relies on
    clean_env.setenv("INFLUX_TOKEN", "from-environ")
    cfg = load_config(config_file, write_env_file(tmp_path, ENV))
    assert cfg.influx.token == "from-environ"
    assert cfg.influx.org == "homelab"          # the rest still from the file


def test_missing_credential_through_load_config(clean_env, config_file, tmp_path):
    env = {k: v for k, v in ENV.items() if k != "INFLUX_BUCKET"}
    with pytest.raises(ConfigError, match="INFLUX_BUCKET"):
        load_config(config_file, write_env_file(tmp_path, env))


def test_missing_config_file(clean_env, tmp_path):
    with pytest.raises(ConfigError, match="nope.toml"):
        load_config(tmp_path / "nope.toml")


def test_invalid_toml_is_a_config_error(clean_env, tmp_path):
    p = tmp_path / "config.toml"
    p.write_text("[mqtt\nhost = ")
    with pytest.raises(ConfigError, match="config.toml"):
        load_config(p)
