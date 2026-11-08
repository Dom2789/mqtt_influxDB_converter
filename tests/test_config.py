import pytest

from mqtt_influx_bridge.config import (
    ConfigError,
    InfluxCredentials,
    load_config,
)
from mqtt_influx_bridge.pipeline import Router

VALID = """
[mqtt]
host = "192.168.1.225"

[[sensor]]
name = "techroom-water"
topic = "temp/techroom/1"
decoder = "json"
measurement = "environment"
tags = { room = "techroom" }
fields = { watertemperature = "temp" }
"""


def write(tmp_path, text: str, name: str = "config.toml"):
    path = tmp_path / name
    path.write_text(text)
    return path


def test_valid_config_round_trip(tmp_path):
    cfg = load_config(write(tmp_path, VALID))
    assert cfg.mqtt.host == "192.168.1.225"
    assert cfg.mqtt.port == 1883          # default
    assert cfg.mqtt.qos == 1              # default
    assert cfg.influx_write.batch_size == 500
    assert cfg.queue.maxsize == 10_000
    assert cfg.logging.target == "stdout"
    assert cfg.file_sink.enabled is False

    (sensor,) = cfg.sensors
    assert sensor.name == "techroom-water"
    assert sensor.fields == {"watertemperature": "temp"}
    assert sensor.tags == {"room": "techroom"}
    assert sensor.tags_from_topic == {}


def test_example_config_is_valid():
    """config.example.toml must stay loadable, since it is the documentation."""
    cfg = load_config("config.example.toml")
    assert len(cfg.sensors) == 4
    Router.from_specs(cfg.sensors)  # also validates every decoder_options block


def test_shipped_config_is_valid():
    cfg = load_config("config.toml")
    Router.from_specs(cfg.sensors)


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.toml")


def test_invalid_toml_names_the_file(tmp_path):
    path = write(tmp_path, "[mqtt\nhost =")
    with pytest.raises(ConfigError) as e:
        load_config(path)
    assert "invalid TOML" in str(e.value)
    assert "config.toml" in str(e.value)


def test_mqtt_section_required(tmp_path):
    body = VALID.replace('[mqtt]\nhost = "192.168.1.225"\n', "")
    with pytest.raises(ConfigError, match=r"\[mqtt\] section is required"):
        load_config(write(tmp_path, body))


def test_mqtt_host_required(tmp_path):
    """An empty [mqtt] table must blame host, not report the section as absent."""
    with pytest.raises(ConfigError, match=r"\[mqtt\].host is required"):
        load_config(write(tmp_path, VALID.replace('host = "192.168.1.225"', "")))


def test_bad_port_range(tmp_path):
    with pytest.raises(ConfigError, match=r"\[mqtt\].port must be in"):
        load_config(write(tmp_path, VALID.replace("[mqtt]", "[mqtt]\nport = 99999")))


def test_bad_qos(tmp_path):
    with pytest.raises(ConfigError, match=r"\[mqtt\].qos must be in"):
        load_config(write(tmp_path, VALID.replace("[mqtt]", "[mqtt]\nqos = 3")))


def test_reconnect_min_above_max(tmp_path):
    body = VALID.replace(
        "[mqtt]", "[mqtt]\nreconnect_min_delay_s = 100\nreconnect_max_delay_s = 10"
    )
    with pytest.raises(ConfigError, match="must not exceed"):
        load_config(write(tmp_path, body))


def test_unknown_key_is_rejected_with_suggestions(tmp_path):
    with pytest.raises(ConfigError) as e:
        load_config(write(tmp_path, VALID.replace("[mqtt]", "[mqtt]\nhsot = 1")))
    assert "hsot" in str(e.value) and "host" in str(e.value)


def test_unknown_section_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="unknown key"):
        load_config(write(tmp_path, VALID + "\n[typo_section]\nx = 1\n"))


def test_credentials_in_config_are_refused(tmp_path):
    body = VALID + '\n[influx]\ntoken = "secret"\n'
    with pytest.raises(ConfigError) as e:
        load_config(write(tmp_path, body))
    assert "must not live in config.toml" in str(e.value)


def test_at_least_one_sensor_required(tmp_path):
    body = '[mqtt]\nhost = "h"\n'
    with pytest.raises(ConfigError, match="at least one"):
        load_config(write(tmp_path, body))


def test_sensor_table_instead_of_array(tmp_path):
    body = '[mqtt]\nhost = "h"\n\n[sensor]\nname = "x"\n'
    with pytest.raises(ConfigError, match="array-of-tables"):
        load_config(write(tmp_path, body))


def test_unknown_decoder_names_the_alternatives(tmp_path):
    with pytest.raises(ConfigError) as e:
        load_config(write(tmp_path, VALID.replace('decoder = "json"', 'decoder = "yaml"')))
    assert "json" in str(e.value) and "scalar" in str(e.value)


def test_malformed_topic(tmp_path):
    body = VALID.replace('topic = "temp/techroom/1"', 'topic = "a/#/b"')
    with pytest.raises(ConfigError, match="last level"):
        load_config(write(tmp_path, body))


DUPLICATE_BLOCK = """
[[sensor]]
name = "techroom-water"
topic = "temp/elsewhere/2"
decoder = "json"
measurement = "environment"
fields = { watertemperature = "temp" }
"""


def test_duplicate_sensor_name(tmp_path):
    with pytest.raises(ConfigError, match="duplicate sensor name"):
        load_config(write(tmp_path, VALID + DUPLICATE_BLOCK))


def test_tags_from_topic_index_out_of_range(tmp_path):
    body = VALID + "tags_from_topic = { x = 9 }\n"
    with pytest.raises(ConfigError, match="out of range"):
        load_config(write(tmp_path, body))


def test_tags_from_topic_unbounded_pattern_is_allowed(tmp_path):
    """A '#' filter cannot be bounds-checked at startup, only at runtime."""
    body = VALID.replace('topic = "temp/techroom/1"', 'topic = "temp/#"')
    body += "tags_from_topic = { x = 9 }\n"
    cfg = load_config(write(tmp_path, body))
    assert cfg.sensors[0].tags_from_topic == {"x": 9}


def test_tags_from_topic_must_be_int(tmp_path):
    body = VALID + 'tags_from_topic = { x = "1" }\n'
    with pytest.raises(ConfigError, match="must be an integer"):
        load_config(write(tmp_path, body))


def test_tag_values_must_be_strings(tmp_path):
    body = VALID.replace('tags = { room = "techroom" }', "tags = { room = 7 }")
    with pytest.raises(ConfigError, match="tag values are always strings"):
        load_config(write(tmp_path, body))


def test_overlapping_tag_sources_rejected(tmp_path):
    body = VALID + "tags_from_topic = { room = 1 }\n"
    with pytest.raises(ConfigError, match="defined in both"):
        load_config(write(tmp_path, body))


def test_field_and_fields_are_mutually_exclusive(tmp_path):
    body = VALID + 'field = "x"\n'
    with pytest.raises(ConfigError, match="not both"):
        load_config(write(tmp_path, body))


def test_fields_required(tmp_path):
    body = VALID.replace("fields = { watertemperature = \"temp\" }", "")
    with pytest.raises(ConfigError, match="either field or fields is required"):
        load_config(write(tmp_path, body))


def test_scalar_with_multiple_fields_rejected(tmp_path):
    body = VALID.replace('decoder = "json"', 'decoder = "scalar"').replace(
        'fields = { watertemperature = "temp" }',
        'fields = { a = "a", b = "b" }',
    )
    with pytest.raises(ConfigError, match="exactly one field"):
        load_config(write(tmp_path, body))


def test_scalar_field_shorthand(tmp_path):
    body = VALID.replace('decoder = "json"', 'decoder = "scalar"').replace(
        'fields = { watertemperature = "temp" }', 'field = "pressure"'
    )
    cfg = load_config(write(tmp_path, body))
    assert cfg.sensors[0].fields == {"pressure": "pressure"}


def test_file_sink_path_must_exist(tmp_path):
    body = VALID + '\n[file_sink]\nenabled = true\npath = "/nonexistent/xyz"\n'
    with pytest.raises(ConfigError, match="not an existing directory"):
        load_config(write(tmp_path, body))


def test_log_target_file_requires_path_and_name(tmp_path):
    body = VALID + '\n[logging]\ntarget = "file"\n'
    with pytest.raises(ConfigError, match="requires both path and name"):
        load_config(write(tmp_path, body))


def test_bad_log_level(tmp_path):
    body = VALID + '\n[logging]\nlevel = "CHATTY"\n'
    with pytest.raises(ConfigError, match="not a valid level"):
        load_config(write(tmp_path, body))


# --- credentials -----------------------------------------------------------


def test_credentials_from_environment(monkeypatch):
    for key, value in {
        "INFLUX_URL": "http://x:8086", "INFLUX_TOKEN": "t",
        "INFLUX_ORG": "o", "INFLUX_BUCKET": "b",
    }.items():
        monkeypatch.setenv(key, value)
    creds = InfluxCredentials.from_env()
    assert creds == InfluxCredentials("http://x:8086", "t", "o", "b")


def test_missing_credentials_names_every_missing_key(monkeypatch):
    for key in ("INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ConfigError) as e:
        InfluxCredentials.from_env()
    for key in ("INFLUX_URL", "INFLUX_TOKEN", "INFLUX_ORG", "INFLUX_BUCKET"):
        assert key in str(e.value)


def test_missing_env_file(tmp_path):
    with pytest.raises(ConfigError, match="env file not found"):
        InfluxCredentials.from_env(tmp_path / "nope.env")


def test_process_env_wins_over_env_file(tmp_path, monkeypatch):
    """The precedence the systemd deployment relies on."""
    env = tmp_path / "influx.env"
    env.write_text(
        "INFLUX_URL=http://from-file:8086\nINFLUX_TOKEN=file-token\n"
        "INFLUX_ORG=file-org\nINFLUX_BUCKET=file-bucket\n"
    )
    monkeypatch.setenv("INFLUX_TOKEN", "env-token")
    creds = InfluxCredentials.from_env(env)
    assert creds.token == "env-token"          # process env wins
    assert creds.org == "file-org"             # file fills the rest
