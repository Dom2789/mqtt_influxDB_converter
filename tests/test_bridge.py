"""Spec for mqtt_influx_bridge.bridge — implementation plan step 7.

Written before the bridge exists, so the first run fails. No broker and no
database: handle() is called directly and the readings land in a
RecordingSink from conftest.py.
"""

import logging
from dataclasses import replace
from datetime import UTC, datetime

import pytest

import mqtt_influx_bridge.bridge as bridge
from conftest import RecordingSink
from mqtt_influx_bridge.bridge import Counters, Router, handle, with_tags
from mqtt_influx_bridge.model import RawMessage, Reading, SensorSpec

AT = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)


def spec(**overrides) -> SensorSpec:
    s = dict(
        name="probe",
        topic="temp/+/+",
        measurement="environment",
        fields={"watertemperature": "temp"},
    ) | overrides
    return SensorSpec(**s)


def msg(topic="temp/techroom/1", payload=b'{"temp":21.5}') -> RawMessage:
    return RawMessage(topic, payload, AT)


def run(*messages: RawMessage, specs=None, sinks=None) -> tuple[RecordingSink, Counters]:
    """handle() every message with one recording sink; return it and the counters."""
    sink = RecordingSink()
    counters = Counters()
    router = Router(specs if specs is not None else [spec()])
    for m in messages:
        handle(m, router, sinks if sinks is not None else [sink], counters)
    return sink, counters


class RaisingSink:
    def write(self, readings) -> None:
        raise RuntimeError("sink down")

    def close(self) -> None:
        pass


# --------------------------------------------------------------------------
# Counters
# --------------------------------------------------------------------------

def test_counters_start_at_zero():
    assert Counters() == Counters(messages_in=0, readings_out=0, decode_errors=0)


def test_since_is_the_field_wise_difference():
    now = Counters(messages_in=10, readings_out=7, decode_errors=2)
    then = Counters(messages_in=4, readings_out=3, decode_errors=2)
    assert now.since(then) == Counters(messages_in=6, readings_out=4, decode_errors=0)


def test_since_changes_neither_side():
    now = Counters(messages_in=10, readings_out=7, decode_errors=2)
    then = Counters(messages_in=4, readings_out=3, decode_errors=1)
    now.since(then)
    assert now == Counters(messages_in=10, readings_out=7, decode_errors=2)
    assert then == Counters(messages_in=4, readings_out=3, decode_errors=1)


def test_since_counts_only_what_came_after_the_copy():
    # the step 9 pattern: copy, keep counting, log the difference
    router = Router([spec()])
    sink = RecordingSink()
    counters = Counters()
    handle(msg(), router, [sink], counters)
    previous = replace(counters)
    handle(msg(), router, [sink], counters)
    handle(msg(payload=b"not json"), router, [sink], counters)
    assert counters.since(previous) == Counters(messages_in=2, readings_out=1, decode_errors=1)


# --------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------

def test_match_exact_topic():
    s = spec(topic="temp/techroom/1")
    assert Router([s]).match("temp/techroom/1") == [s]


def test_match_nothing():
    assert Router([spec(topic="temp/+/+")]).match("powermeter/a/b/status/pm1:0") == []


def test_match_returns_all_overlapping_specs_in_config_order():
    broad = spec(name="broad", topic="temp/#")
    specific = spec(name="specific", topic="temp/techroom/1")
    other = spec(name="other", topic="power/#")
    assert Router([broad, other, specific]).match("temp/techroom/1") == [broad, specific]


def test_match_hash_covers_the_parent_level():
    # MQTT spec: "temp/#" also matches "temp" itself
    s = spec(topic="temp/#")
    assert Router([s]).match("temp") == [s]


def test_match_wildcards_do_not_match_dollar_topics():
    # MQTT spec: a filter starting with a wildcard never matches $SYS/...
    assert Router([spec(topic="#")]).match("$SYS/broker/uptime") == []


def test_subscriptions_are_deduplicated():
    router = Router([
        spec(name="a", topic="temp/+/+"),
        spec(name="b", topic="temp/+/+"),
        spec(name="c", topic="powermeter/+/+/status/+"),
    ])
    assert router.subscriptions() == {"temp/+/+", "powermeter/+/+/status/+"}


# --------------------------------------------------------------------------
# with_tags — precedence
# --------------------------------------------------------------------------

def plain_reading() -> Reading:
    return Reading("environment", {"watertemperature": 21.5}, {}, AT)


def test_with_tags_static_only():
    r = with_tags(plain_reading(), spec(tags={"site": "home"}), "temp/techroom/1")
    assert r.tags == {"site": "home"}


def test_with_tags_from_topic_only():
    r = with_tags(plain_reading(), spec(tags_from_topic={"room": 1, "device": 2}), "temp/techroom/1")
    assert r.tags == {"room": "techroom", "device": "1"}


def test_with_tags_topic_wins_over_static():
    s = spec(tags={"room": "fallback", "site": "home"}, tags_from_topic={"room": 1})
    r = with_tags(plain_reading(), s, "temp/techroom/1")
    assert r.tags == {"room": "techroom", "site": "home"}


def test_with_tags_returns_a_new_reading():
    # Reading is frozen; the original keeps its empty tags
    original = plain_reading()
    with_tags(original, spec(tags={"site": "home"}), "temp/techroom/1")
    assert original.tags == {}


# --------------------------------------------------------------------------
# handle — the happy path
# --------------------------------------------------------------------------

def test_exact_reading_arrives():
    s = spec(tags={"site": "home"}, tags_from_topic={"room": 1, "device": 2})
    sink, counters = run(msg(), specs=[s])
    assert sink.readings == [
        Reading(
            measurement="environment",
            fields={"watertemperature": 21.5},
            tags={"site": "home", "room": "techroom", "device": "1"},
            timestamp=AT,
        )
    ]
    assert counters == Counters(messages_in=1, readings_out=1, decode_errors=0)


def test_one_wildcard_sensor_covers_a_fleet():
    s = spec(topic="temp/+/+", tags_from_topic={"room": 1, "device": 2})
    sink, _ = run(
        msg("temp/techroom/1"),
        msg("temp/kitchen/1"),
        msg("temp/kitchen/2"),
        specs=[s],
    )
    assert [r.tags for r in sink.readings] == [
        {"room": "techroom", "device": "1"},
        {"room": "kitchen", "device": "1"},
        {"room": "kitchen", "device": "2"},
    ]


def test_two_overlapping_patterns_both_fire():
    broad = spec(name="broad", topic="temp/#", tags={"via": "broad"})
    specific = spec(name="specific", topic="temp/techroom/1", tags={"via": "specific"})
    sink, counters = run(msg(), specs=[broad, specific])
    assert [r.tags["via"] for r in sink.readings] == ["broad", "specific"]
    assert counters.readings_out == 2


def test_unmatched_topic_is_counted_in_but_writes_nothing():
    sink, counters = run(msg("other/topic"))
    assert sink.readings == []
    assert counters == Counters(messages_in=1, readings_out=0, decode_errors=0)


def test_every_sink_gets_the_readings():
    a, b = RecordingSink(), RecordingSink()
    _, counters = run(msg(), sinks=[a, b])
    assert len(a.readings) == len(b.readings) == 1
    assert counters.readings_out == 1        # readings, not readings x sinks


# --------------------------------------------------------------------------
# handle — failures
# --------------------------------------------------------------------------

def test_bad_message_does_not_stop_the_next():
    sink, counters = run(msg(payload=b"not json"), msg())
    assert len(sink.readings) == 1
    assert counters == Counters(messages_in=2, readings_out=1, decode_errors=1)


def test_bad_message_is_logged_with_its_topic(caplog):
    with caplog.at_level(logging.DEBUG):
        run(msg("temp/techroom/1", payload=b"not json"))
    [record] = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert "temp/techroom/1" in record.getMessage()


def test_tag_index_error_drops_the_message():
    # "#" leaves the index to runtime; a too-short topic is a BridgeError
    s = spec(topic="temp/#", tags_from_topic={"device": 5})
    sink, counters = run(msg("temp/techroom/1"), specs=[s])
    assert sink.readings == []
    assert counters.decode_errors == 1


def test_error_in_one_spec_drops_the_whole_message():
    # nothing from a bad message is written, even what another spec decoded
    ok = spec(name="ok", topic="temp/#")
    bad = spec(name="bad", topic="temp/#", tags_from_topic={"device": 5})
    sink, counters = run(msg(), specs=[ok, bad])
    assert sink.readings == []
    assert counters == Counters(messages_in=1, readings_out=0, decode_errors=1)


def test_a_raising_sink_does_not_lose_the_others():
    after = RecordingSink()
    _, counters = run(msg(), sinks=[RaisingSink(), after])
    assert len(after.readings) == 1
    assert counters.readings_out == 1


def test_a_raising_sink_is_logged(caplog):
    with caplog.at_level(logging.DEBUG):
        run(msg(), sinks=[RaisingSink()])
    assert any("sink down" in r.getMessage() or r.exc_info for r in caplog.records
               if r.levelno >= logging.ERROR)


def test_a_bug_is_not_mistaken_for_a_bad_message(monkeypatch):
    # only BridgeError is expected; anything else must surface
    def broken(msg, spec):
        raise ZeroDivisionError("bug")

    monkeypatch.setattr(bridge, "decode_json", broken)
    with pytest.raises(ZeroDivisionError):
        run(msg())
