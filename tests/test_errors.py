"""Spec for mqtt_influx_bridge.errors — implementation plan steps 2 and 4."""

from mqtt_influx_bridge.errors import BridgeError, ConfigError, DecodeError


def test_decode_error_message_has_topic_and_short_excerpt():
    err = DecodeError("t/1", b"x" * 100, "bad json")
    assert "t/1" in str(err)
    assert "bad json" in str(err)
    assert "x" * 41 not in str(err)       # payload cut to 40 bytes


def test_decode_error_hierarchy():
    assert issubclass(DecodeError, BridgeError)
    assert not issubclass(DecodeError, ValueError)
    assert issubclass(ConfigError, BridgeError)
