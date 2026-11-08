"""The Influx line protocol a Reading turns into — the real output format.

No server required: Point.to_line_protocol() renders exactly what would be sent.
"""

from datetime import datetime, timezone

from mqtt_influx_bridge.model import Reading
from mqtt_influx_bridge.sinks.influx import _to_point

TS = datetime(2026, 11, 7, 12, 0, 0, tzinfo=timezone.utc)
TS_NS = int(TS.timestamp() * 1_000_000_000)


def test_reading_renders_the_expected_line_protocol():
    reading = Reading(
        measurement="environment",
        fields={"watertemperature": 21.5},
        tags={"room": "techroom"},
        timestamp=TS,
    )
    line = _to_point(reading).to_line_protocol()
    assert line == f"environment,room=techroom watertemperature=21.5 {TS_NS}"


def test_timestamp_is_explicit_rather_than_server_side():
    """The pre-refactor writer set no time, so Influx stamped arrival itself."""
    reading = Reading("m", {"v": 1.0}, {}, TS)
    assert str(TS_NS) in _to_point(reading).to_line_protocol()


def test_multiple_tags_and_fields():
    reading = Reading(
        measurement="power",
        fields={"voltage": 231.4, "current": 0.52, "power": 118.7},
        tags={"device": "shelly", "device_id": "kitchen"},
        timestamp=TS,
    )
    line = _to_point(reading).to_line_protocol()
    # tags are sorted by the client; fields keep insertion order
    assert line.startswith("power,device=shelly,device_id=kitchen ")
    for part in ("voltage=231.4", "current=0.52", "power=118.7"):
        assert part in line


def test_untagged_reading_is_valid():
    line = _to_point(Reading("m", {"v": 2.5}, {}, TS)).to_line_protocol()
    assert line == f"m v=2.5 {TS_NS}"


def test_float_cast_avoids_the_integer_suffix():
    """Why decoder_options.cast = "float" matters.

    A bare int renders with Influx's 'i' suffix and is stored as an integer, so
    a sensor that sends 21 then 21.5 would collide on field type. Casting to
    float keeps every point the same type — and a whole float still renders
    without a '.0', which is a float in line protocol, not an integer.
    """
    as_int = _to_point(Reading("m", {"v": 21}, {}, TS)).to_line_protocol()
    assert "v=21i" in as_int

    as_float = _to_point(Reading("m", {"v": float(21)}, {}, TS)).to_line_protocol()
    assert "v=21 " in as_float
    assert "v=21i" not in as_float


def test_integer_and_boolean_and_string_fields_survive():
    reading = Reading(
        measurement="mixed",
        fields={"count": 7, "ok": True, "label": "fine"},
        tags={},
        timestamp=TS,
    )
    line = _to_point(reading).to_line_protocol()
    assert "count=7i" in line        # Influx integer suffix
    assert "ok=true" in line
    assert 'label="fine"' in line
