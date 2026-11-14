"""Spec for mqtt_influx_bridge.decoders.decode_json — implementation plan step 4.

Written before decode_json exists, so the first run fails at import. That is
the intended starting state: make these pass by writing decode_json(). Don't
edit the fixtures to match an implementation — they are copied verbatim from
example_messages/power.txt.
"""

from datetime import UTC, datetime

import pytest

from mqtt_influx_bridge.decoders import decode_json
from mqtt_influx_bridge.errors import BridgeError, ConfigError, DecodeError
from mqtt_influx_bridge.model import RawMessage, Reading, SensorSpec

AT = datetime(2026, 11, 8, 10, 9, tzinfo=UTC)


def msg(topic: str, payload: bytes) -> RawMessage:
    return RawMessage(topic, payload, AT)


# --------------------------------------------------------------------------
# Fixtures: real bodies from example_messages/power.txt, verbatim
# --------------------------------------------------------------------------

PLUG_PM = msg(
    "powermeter/serverraum/rack/status/pm1:0",
    b'{"id":0, "voltage":225.1, "current":0.194, "apower":21.7 ,"freq":49.9,"aenergy":{"total":40503.682,"by_minute":[210.574,210.574,421.148],"minute_ts":1794647340},"ret_aenergy":{"total":0.000,"by_minute":[0.000,0.000,0.000],"minute_ts":1794647340}}',
)

STRIP_SWITCH_0 = msg(
    "powermeter/serverraum/strip/status/switch:0",
    b'{"id":0,"source":"init","tag":null, "output":false, "apower":0.0, "voltage":225.8, "freq":50.0, "current":0.000, "pf":0.00, "aenergy":{"total":3.045,"by_minute":[0.000,0.000,0.000],"minute_ts":1794647280}, "ret_aenergy":{"total":0.000,"by_minute":[0.000,0.000,0.000],"minute_ts":1794647280},"counts":{"on_time":870,"on_time_rst_ts":0,"switch_on":10,"switch_on_rst_ts":0,"on_above_thr":0,"on_above_thr_rst_ts":0}}',
)

STRIP_SWITCH_3 = msg(
    "powermeter/serverraum/strip/status/switch:3",
    b'{"id":3,"source":"init","tag":null, "output":false, "apower":0.0, "voltage":224.5, "freq":50.0, "current":0.000, "pf":0.00, "aenergy":{"total":0.000,"by_minute":[0.000,0.000,0.000],"minute_ts":1794647340}, "ret_aenergy":{"total":0.000,"by_minute":[0.000,0.000,0.000],"minute_ts":1794647340},"counts":{"on_time":0,"on_time_rst_ts":0,"switch_on":4,"switch_on_rst_ts":0,"on_above_thr":0,"on_above_thr_rst_ts":0}}',
)

PLUG_M = msg(
    "powermeter/esszimmer/schrank/status/switch:0",
    b'{"id":0,"source":"button","tag":null, "output":true, "apower":15.1, "voltage":227.0, "freq":50.0, "current":0.067, "aenergy":{"total":2218.573,"by_minute":[208.512,208.512,417.025],"minute_ts":1794647340}, "ret_aenergy":{"total":0.000,"by_minute":[0.000,0.000,0.000],"minute_ts":1794647340},"counts":{"on_time":519210,"on_time_rst_ts":0,"switch_on":25,"switch_on_rst_ts":0,"on_above_thr":0,"on_above_thr_rst_ts":0},"temperature":{"tC":37.8, "tF":100.0}}',
)

DIMMER = msg(
    "powermeter/esszimmer/lampe/status/light:0",
    b'{"id":0,"source":"WS_in","tag":null,"output":false,"brightness":11,"temperature":{"tC":41.4, "tF":106.5},"aenergy":{"total":102.164,"by_minute":[40.522,48.307,0.000],"minute_ts":1794651120},"apower":0.0,"current":0.000,"voltage":226.0}',
)

# The one fields table from the plan / config.toml.example.
SHELLY = SensorSpec(
    name="shelly powermeter fleet",
    topic="powermeter/+/+/status/+",
    measurement="power",
    cast="float",
    fields={
        "voltage": "voltage",
        "current": "current",
        "power": "apower",
        "freq": "freq",
        "energy": "aenergy.total",
        "pf": "pf",
        "temperature": "temperature.tC",
        "on_time": "counts.on_time",
        "output": "output",
        "brightness": "brightness",
    },
    tags_from_topic={"room": 1, "device": 2, "channel": 4},
)

WATER = SensorSpec(
    name="water temperature techroom",
    topic="temp/techroom/1",
    measurement="environment",
    cast="float",
    fields={"watertemperature": "temp"},
    tags={"room": "techroom"},
)


def spec(fields: dict[str, str], cast: str = "float") -> SensorSpec:
    return SensorSpec("test", "t/#", "m", fields, cast)


def decode_one(m: RawMessage, s: SensorSpec) -> Reading:
    readings = decode_json(m, s)
    assert len(readings) == 1
    return readings[0]


# --------------------------------------------------------------------------
# Happy path
# --------------------------------------------------------------------------

def test_water_temperature_exact_reading():
    m = msg("temp/techroom/1", b'{"temp":21.5}')
    assert decode_json(m, WATER) == [
        Reading(
            measurement="environment",
            fields={"watertemperature": 21.5},
            tags={},                      # step 07 fills tags, not decode_json
            timestamp=AT,
        )
    ]


def test_plug_pm():
    r = decode_one(PLUG_PM, SHELLY)
    assert r.measurement == "power"
    assert r.fields == {
        "voltage": 225.1,
        "current": 0.194,
        "power": 21.7,
        "freq": 49.9,
        "energy": 40503.682,
    }
    assert r.tags == {}


def test_strip_switch_0():
    r = decode_one(STRIP_SWITCH_0, SHELLY)
    assert r.fields["pf"] == 0.0
    assert r.fields["on_time"] == 870.0
    assert "temperature" not in r.fields


def test_plug_m():
    r = decode_one(PLUG_M, SHELLY)
    assert r.fields["temperature"] == 37.8
    assert "pf" not in r.fields


def test_dimmer_skips_freq_and_keeps_the_rest():
    # Not one field is sent by all four models. This test stops anyone turning
    # the skip rule into a "required core + optional extras" split.
    r = decode_one(DIMMER, SHELLY)
    assert r.fields == {
        "voltage": 226.0,
        "current": 0.0,
        "power": 0.0,
        "energy": 102.164,
        "temperature": 41.4,
        "output": 0.0,
        "brightness": 11.0,
    }
    assert "freq" not in r.fields


def test_timestamp_is_received_at():
    assert decode_one(PLUG_PM, SHELLY).timestamp == AT


# --------------------------------------------------------------------------
# Cast: one per sensor
# --------------------------------------------------------------------------

def test_float_cast_applies_to_every_field():
    r = decode_one(PLUG_M, SHELLY)
    assert all(type(v) is float for v in r.fields.values())


def test_json_bool_lands_as_float():
    assert decode_one(PLUG_M, SHELLY).fields["output"] == 1.0
    assert decode_one(STRIP_SWITCH_0, SHELLY).fields["output"] == 0.0


# --------------------------------------------------------------------------
# Missing and null fields are skipped, falsy values are kept
# --------------------------------------------------------------------------

def test_falsy_values_are_data_not_missing():
    # switch:3 is idle: output false, apower 0.0, current 0.000, on_time 0.
    # A truthiness check would drop all of these.
    r = decode_one(STRIP_SWITCH_3, SHELLY)
    assert r.fields["output"] == 0.0
    assert r.fields["power"] == 0.0
    assert r.fields["current"] == 0.0
    assert r.fields["energy"] == 0.0
    assert r.fields["on_time"] == 0.0
    assert r.fields["pf"] == 0.0


def test_missing_parent_skips():
    # pm1:0 has no "counts" object at all.
    r = decode_one(PLUG_PM, spec({"on_time": "counts.on_time", "v": "voltage"}))
    assert r.fields == {"v": 225.1}


def test_missing_leaf_under_present_parent_skips():
    r = decode_one(PLUG_PM, spec({"x": "aenergy.nope", "v": "voltage"}))
    assert r.fields == {"v": 225.1}


def test_missing_top_level_key_skips():
    r = decode_one(PLUG_PM, spec({"pf": "pf", "v": "voltage"}))
    assert r.fields == {"v": 225.1}


def test_null_value_skips():
    # "tag": null on the strip
    r = decode_one(STRIP_SWITCH_0, spec({"tag": "tag", "v": "voltage"}))
    assert r.fields == {"v": 225.8}


def test_no_fields_at_all_gives_no_reading():
    assert decode_json(PLUG_PM, spec({"pf": "pf", "x": "counts.on_time"})) == []


def test_empty_object_gives_no_reading():
    assert decode_json(msg("t/1", b"{}"), WATER) == []


# --------------------------------------------------------------------------
# Failures: always DecodeError, never anything else
# --------------------------------------------------------------------------

def test_list_value_is_an_error_not_a_skip():
    with pytest.raises(DecodeError):
        decode_json(PLUG_PM, spec({"bm": "aenergy.by_minute"}))


def test_object_value_is_an_error_not_a_skip():
    with pytest.raises(DecodeError):
        decode_json(PLUG_PM, spec({"e": "aenergy"}))


BAD_PAYLOADS = [
    pytest.param(b"\xff\xfe\x00", id="not-utf8"),
    pytest.param(b"", id="empty"),
    pytest.param(b"{temp: 21.5}", id="invalid-json"),
    pytest.param(b'{"temp": 21.5', id="truncated-json"),
    pytest.param(b"[21.5]", id="json-list"),
    pytest.param(b"21.5", id="json-number"),
    pytest.param(b'"21.5"', id="json-string"),
    pytest.param(b"null", id="json-null"),
    pytest.param(b'{"temp": "warm"}', id="value-will-not-cast"),
]


@pytest.mark.parametrize("payload", BAD_PAYLOADS)
def test_bad_payload_raises_decode_error(payload):
    m = msg("temp/techroom/1", payload)
    with pytest.raises(DecodeError) as exc:
        decode_json(m, WATER)
    assert exc.value.topic == "temp/techroom/1"
    assert exc.value.payload == payload


def test_decode_error_message_has_topic_and_short_excerpt():
    err = DecodeError("t/1", b"x" * 100, "bad json")
    assert "t/1" in str(err)
    assert "bad json" in str(err)
    assert "x" * 41 not in str(err)       # payload cut to 40 bytes


def test_decode_error_hierarchy():
    assert issubclass(DecodeError, BridgeError)
    assert not issubclass(DecodeError, ValueError)
    assert issubclass(ConfigError, BridgeError)
