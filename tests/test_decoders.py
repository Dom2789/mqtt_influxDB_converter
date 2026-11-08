import pytest

from mqtt_influx_bridge import decoders
from mqtt_influx_bridge.config import SensorSpec
from mqtt_influx_bridge.decoders.base import DecodeError

from .conftest import TS, msg


def test_registry_lists_builtin_decoders():
    assert decoders.available() == ["json", "scalar"]


def test_build_unknown_decoder_names_the_alternatives():
    with pytest.raises(KeyError) as e:
        decoders.build("yaml", {})
    assert "json" in e.value.args[0] and "scalar" in e.value.args[0]


# --- json ------------------------------------------------------------------


def test_json_decodes_the_water_temperature_payload(water_spec):
    decoder = decoders.build("json", {"cast": "float"})
    (reading,) = decoder.decode(msg("temp/techroom/1", '{"temp":21.5}'), water_spec)
    assert reading.measurement == "environment"
    assert reading.fields == {"watertemperature": 21.5}
    assert reading.timestamp == TS


def test_json_decodes_the_shelly_payload(shelly_spec):
    decoder = decoders.build("json", {"cast": "float"})
    payload = '{"voltage": 231.4, "current": 0.52, "apower": 118.7, "freq": 50}'
    (reading,) = decoder.decode(msg("shellies/abc/status/pm1:0", payload), shelly_spec)
    assert reading.measurement == "power"
    assert reading.fields == {"voltage": 231.4, "current": 0.52, "power": 118.7}


def test_json_cast_pins_the_field_type(water_spec):
    """Without a cast, an int payload and a float payload disagree in Influx."""
    plain = decoders.build("json", {})
    (as_int,) = plain.decode(msg("t", '{"temp":21}'), water_spec)
    assert isinstance(as_int.fields["watertemperature"], int)

    cast = decoders.build("json", {"cast": "float"})
    (as_float,) = cast.decode(msg("t", '{"temp":21}'), water_spec)
    assert isinstance(as_float.fields["watertemperature"], float)


def test_json_dotted_path_descends():
    spec = SensorSpec(
        name="nested", topic="w/+/report", decoder="json", measurement="weather",
        fields={"temperature": "sensors.outdoor.temp_c"},
    )
    decoder = decoders.build("json", {"cast": "float"})
    payload = '{"sensors": {"outdoor": {"temp_c": 4.25}}}'
    (reading,) = decoder.decode(msg("w/a/report", payload), spec)
    assert reading.fields == {"temperature": 4.25}


def test_json_missing_key_names_the_key_and_the_alternatives(water_spec):
    decoder = decoders.build("json", {})
    with pytest.raises(DecodeError) as e:
        decoder.decode(msg("t", '{"temperature":21.5}'), water_spec)
    assert "'temp'" in str(e.value)
    assert "temperature" in str(e.value)  # lists what was available


def test_json_ignore_missing_skips_absent_fields(shelly_spec):
    decoder = decoders.build("json", {"cast": "float", "ignore_missing": True})
    (reading,) = decoder.decode(
        msg("shellies/a/status/pm1:0", '{"voltage": 230.0}'), shelly_spec
    )
    assert reading.fields == {"voltage": 230.0}


def test_json_all_fields_missing_is_an_error(shelly_spec):
    decoder = decoders.build("json", {"ignore_missing": True})
    with pytest.raises(DecodeError, match="no fields"):
        decoder.decode(msg("shellies/a/status/pm1:0", '{"other": 1}'), shelly_spec)


def test_json_rejects_non_scalar_value(water_spec):
    decoder = decoders.build("json", {})
    with pytest.raises(DecodeError, match="expected a scalar"):
        decoder.decode(msg("t", '{"temp": [1, 2]}'), water_spec)


def test_json_rejects_null_value(water_spec):
    decoder = decoders.build("json", {})
    with pytest.raises(DecodeError, match="expected a scalar"):
        decoder.decode(msg("t", '{"temp": null}'), water_spec)


def test_json_uncastable_value_names_the_field(water_spec):
    decoder = decoders.build("json", {"cast": "float"})
    with pytest.raises(DecodeError) as e:
        decoder.decode(msg("t", '{"temp": "warm"}'), water_spec)
    assert "watertemperature" in str(e.value)


def test_json_rejects_malformed_json(water_spec):
    decoder = decoders.build("json", {})
    with pytest.raises(DecodeError, match="not valid JSON"):
        decoder.decode(msg("t", "garbage"), water_spec)


def test_json_rejects_non_utf8(water_spec):
    decoder = decoders.build("json", {})
    with pytest.raises(DecodeError, match="not valid UTF-8"):
        decoder.decode(msg("t", b"\xff\xfe\x00"), water_spec)


def test_json_rejects_descending_into_a_scalar():
    spec = SensorSpec(
        name="s", topic="t", decoder="json", measurement="m",
        fields={"x": "a.b"},
    )
    decoder = decoders.build("json", {})
    with pytest.raises(DecodeError, match="cannot descend"):
        decoder.decode(msg("t", '{"a": 5}'), spec)


def test_json_unknown_cast_fails_at_construction():
    with pytest.raises(ValueError, match="unknown cast"):
        decoders.build("json", {"cast": "complex"})


# --- scalar ----------------------------------------------------------------


def _scalar_spec() -> SensorSpec:
    return SensorSpec(
        name="boiler", topic="boiler/pressure", decoder="scalar",
        measurement="environment", fields={"pressure": "pressure"},
    )


def test_scalar_parses_a_bare_value():
    decoder = decoders.build("scalar", {})
    (reading,) = decoder.decode(msg("boiler/pressure", "1.8"), _scalar_spec())
    assert reading.fields == {"pressure": 1.8}


def test_scalar_strips_whitespace():
    decoder = decoders.build("scalar", {})
    (reading,) = decoder.decode(msg("boiler/pressure", "  2.5\n"), _scalar_spec())
    assert reading.fields == {"pressure": 2.5}


def test_scalar_parses_a_negative_value():
    decoder = decoders.build("scalar", {})
    (reading,) = decoder.decode(msg("boiler/pressure", "-7.25"), _scalar_spec())
    assert reading.fields == {"pressure": -7.25}


def test_scalar_pattern_extracts_the_capture_group():
    decoder = decoders.build("scalar", {"pattern": r"([-\d.]+)\s*bar"})
    (reading,) = decoder.decode(msg("boiler/pressure", "now 1.8 bar"), _scalar_spec())
    assert reading.fields == {"pressure": 1.8}


def test_scalar_replaces_the_old_split_hack():
    """The pre-refactor parser was message.split(':')[1][:-1]."""
    decoder = decoders.build("scalar", {"pattern": r":\s*([-\d.]+)"})
    (reading,) = decoder.decode(msg("t", '{"temp":21.5}'), _scalar_spec())
    assert reading.fields == {"pressure": 21.5}


def test_scalar_pattern_not_matching_is_a_decode_error():
    decoder = decoders.build("scalar", {"pattern": r"([-\d.]+)\s*bar"})
    with pytest.raises(DecodeError, match="did not match"):
        decoder.decode(msg("boiler/pressure", "no reading"), _scalar_spec())


def test_scalar_non_numeric_is_a_decode_error():
    decoder = decoders.build("scalar", {})
    with pytest.raises(DecodeError, match="cannot parse"):
        decoder.decode(msg("boiler/pressure", "warm"), _scalar_spec())


def test_scalar_rejects_non_utf8():
    decoder = decoders.build("scalar", {})
    with pytest.raises(DecodeError, match="not valid UTF-8"):
        decoder.decode(msg("boiler/pressure", b"\xff\xfe"), _scalar_spec())


def test_scalar_pattern_must_have_exactly_one_group():
    with pytest.raises(ValueError, match="exactly one capture group"):
        decoders.build("scalar", {"pattern": r"(\d+)\.(\d+)"})
    with pytest.raises(ValueError, match="exactly one capture group"):
        decoders.build("scalar", {"pattern": r"\d+"})
