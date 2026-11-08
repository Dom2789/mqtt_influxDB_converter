"""Tests for the carried-over _lib modules and the file sink's naming."""

import tomllib

import pytest

from mqtt_influx_bridge._lib.ConfigToml import ConfigToml
from mqtt_influx_bridge._lib.logger import setup_logging
from mqtt_influx_bridge.sinks.file import dated_name


# --- ConfigToml ------------------------------------------------------------


def test_reads_nested_tables(tmp_path):
    path = tmp_path / "c.toml"
    path.write_text('[mqtt]\nhost = "h"\nport = 1883\n')
    cfg = ConfigToml(path)
    assert cfg["mqtt"]["host"] == "h"
    assert len(cfg) == 1
    assert list(cfg) == ["mqtt"]
    assert cfg.get("missing") is None


def test_accepts_a_path_object(tmp_path):
    path = tmp_path / "c.toml"
    path.write_text("x = 1\n")
    assert ConfigToml(path)["x"] == 1


def test_top_level_is_read_only(tmp_path):
    path = tmp_path / "c.toml"
    path.write_text("x = 1\n")
    with pytest.raises(TypeError):
        ConfigToml(path)["x"] = 2


def test_nested_tables_are_read_only_too(tmp_path):
    """The old version handed out the live dict, so this silently succeeded."""
    path = tmp_path / "c.toml"
    path.write_text('[mqtt]\nhost = "h"\n')
    cfg = ConfigToml(path)
    with pytest.raises(TypeError):
        cfg["mqtt"]["host"] = "evil"


def test_decode_error_names_the_file(tmp_path):
    path = tmp_path / "broken.toml"
    path.write_text("[oops\n")
    with pytest.raises(tomllib.TOMLDecodeError) as e:
        ConfigToml(path)
    assert "broken.toml" in str(e.value)


def test_str_round_trips_through_toml(tmp_path):
    path = tmp_path / "c.toml"
    original = '[mqtt]\nhost = "h"\nport = 1883\n\n[[sensor]]\nname = "s"\n'
    path.write_text(original)
    cfg = ConfigToml(path)
    # __str__ must survive the MappingProxyType/tuple freezing
    reparsed = tomllib.loads(str(cfg))
    assert reparsed == {"mqtt": {"host": "h", "port": 1883},
                        "sensor": [{"name": "s"}]}


def test_path_is_exposed(tmp_path):
    path = tmp_path / "c.toml"
    path.write_text("x = 1\n")
    assert ConfigToml(path).path == str(path)


# --- logger ----------------------------------------------------------------


def test_stdout_logging_is_the_default(capsys):
    import logging

    setup_logging(target="stdout", level="INFO")
    logging.getLogger("t").info("hello")
    assert "hello" in capsys.readouterr().out


def test_file_logging_still_works(tmp_path):
    import logging

    setup_logging(
        path_logfile=str(tmp_path), name_logfile="bridge", target="file", level="INFO"
    )
    logging.getLogger("t").info("to-a-file")
    logging.shutdown()
    assert "to-a-file" in (tmp_path / "bridge.log").read_text()


def test_file_logging_requires_path_and_name():
    with pytest.raises(ValueError, match="requires path_logfile"):
        setup_logging(target="file")


def test_unknown_target_rejected():
    with pytest.raises(ValueError, match='must be "stdout" or "file"'):
        setup_logging(target="syslog")


def test_unknown_level_rejected():
    with pytest.raises(ValueError, match="unknown log level"):
        setup_logging(level="CHATTY")


def test_repeated_setup_does_not_duplicate_handlers(capsys):
    import logging

    setup_logging(target="stdout", level="INFO")
    setup_logging(target="stdout", level="INFO")
    logging.getLogger("t").info("once")
    assert capsys.readouterr().out.count("once") == 1


# --- file sink naming ------------------------------------------------------


@pytest.mark.parametrize(
    "name,expected",
    [
        ("readings.log", "readings_20261107.log"),
        ("readings", "readings_20261107"),              # no dot: used to raise
        ("my.readings.log", "my.readings_20261107.log"),  # 2 dots: used to raise
        (".hidden", "_20261107.hidden"),
    ],
)
def test_dated_name_handles_any_number_of_dots(name, expected):
    assert dated_name(name, date="20261107") == expected
