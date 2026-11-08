"""Pipeline tests — the ones that were impossible before, when subscribe() blocked forever."""

import pytest

from mqtt_influx_bridge.config import ConfigError, SensorSpec
from mqtt_influx_bridge.pipeline import Pipeline, Router
from mqtt_influx_bridge.sinks.stdout import RecordingSink

from .conftest import TS, msg


def _router(*specs: SensorSpec) -> Router:
    return Router.from_specs(specs)


# --- router ----------------------------------------------------------------


def test_router_matches_all_applicable_sensors(water_spec):
    broad = SensorSpec(
        name="catch-all", topic="temp/#", decoder="json", measurement="raw",
        fields={"t": "temp"},
    )
    router = _router(water_spec, broad)
    assert len(router.match("temp/techroom/1")) == 2
    assert len(router.match("temp/other/9")) == 1


def test_router_deduplicates_subscriptions(water_spec):
    second = SensorSpec(
        name="other-field", topic="temp/techroom/1", decoder="json",
        measurement="environment2", fields={"t": "temp"},
    )
    router = _router(water_spec, second)
    assert router.subscriptions(qos=1) == [("temp/techroom/1", 1)]


def test_router_rejects_bad_decoder_options_at_startup():
    spec = SensorSpec(
        name="bad", topic="t", decoder="scalar", measurement="m",
        fields={"x": "x"}, decoder_options={"pattern": r"(\d)(\d)"},
    )
    with pytest.raises(ConfigError) as e:
        _router(spec)
    assert "bad" in str(e.value) and "capture group" in str(e.value)


def test_router_rejects_unknown_decoder_at_startup():
    spec = SensorSpec(
        name="bad", topic="t", decoder="nope", measurement="m", fields={"x": "x"}
    )
    with pytest.raises(ConfigError, match="unknown decoder"):
        _router(spec)


# --- decode + tag merging --------------------------------------------------


def test_static_and_topic_tags_are_merged(shelly_spec):
    sink = RecordingSink()
    pipeline = Pipeline(_router(shelly_spec), [sink])
    payload = '{"voltage": 231.4, "current": 0.52, "apower": 118.7}'
    pipeline._handle(msg("shellies/abc123/status/pm1:0", payload))

    (reading,) = sink.readings
    assert reading.tags == {"device": "shelly", "device_id": "abc123"}
    assert reading.fields == {"voltage": 231.4, "current": 0.52, "power": 118.7}
    assert reading.timestamp == TS
    assert pipeline.stats.written == 1


def test_one_wildcard_block_covers_a_fleet(shelly_spec):
    """The mechanism that makes 'lots more sensors' a config-only change."""
    sink = RecordingSink()
    pipeline = Pipeline(_router(shelly_spec), [sink])
    payload = '{"voltage": 230.0, "current": 1.0, "apower": 230.0}'
    for device in ("kitchen", "boiler", "garage"):
        pipeline._handle(msg(f"shellies/{device}/status/pm1:0", payload))

    assert [r.tags["device_id"] for r in sink.readings] == [
        "kitchen", "boiler", "garage"
    ]
    assert pipeline.stats.written == 3


def test_readings_go_to_every_sink(water_spec):
    a, b = RecordingSink(), RecordingSink()
    pipeline = Pipeline(_router(water_spec), [a, b])
    pipeline._handle(msg("temp/techroom/1", '{"temp":21.5}'))
    assert len(a.readings) == len(b.readings) == 1


def test_unrouted_topic_is_counted_not_fatal(water_spec):
    sink = RecordingSink()
    pipeline = Pipeline(_router(water_spec), [sink])
    pipeline._handle(msg("some/other/topic", '{"temp":1}'))
    assert sink.readings == []
    assert pipeline.stats.unrouted == 1
    assert pipeline.stats.decode_errors == 0


def test_one_bad_message_does_not_stop_the_rest(water_spec):
    sink = RecordingSink()
    pipeline = Pipeline(_router(water_spec), [sink])
    for payload in ['{"temp":1.0}', "garbage", '{"temp":3.0}']:
        pipeline._handle(msg("temp/techroom/1", payload))

    assert [r.fields["watertemperature"] for r in sink.readings] == [1.0, 3.0]
    assert pipeline.stats.decode_errors == 1
    assert pipeline.stats.written == 2


def test_out_of_range_topic_tag_is_a_handled_decode_error():
    spec = SensorSpec(
        name="greedy", topic="temp/#", decoder="json", measurement="m",
        fields={"t": "temp"}, tags_from_topic={"deep": 5},
    )
    sink = RecordingSink()
    pipeline = Pipeline(_router(spec), [sink])
    pipeline._handle(msg("temp/a", '{"temp":1}'))
    assert sink.readings == []
    assert pipeline.stats.decode_errors == 1


def test_a_failing_sink_does_not_lose_the_other(water_spec):
    class Broken:
        def write(self, readings):
            raise RuntimeError("influx is down in a way the client did not catch")

        def close(self):
            pass

    good = RecordingSink()
    pipeline = Pipeline(_router(water_spec), [Broken(), good])
    pipeline._handle(msg("temp/techroom/1", '{"temp":21.5}'))
    assert len(good.readings) == 1
    assert pipeline.stats.sink_errors == 1


# --- queue behaviour -------------------------------------------------------


def test_queue_full_drops_and_counts_without_raising(water_spec):
    pipeline = Pipeline(_router(water_spec), [RecordingSink()], maxsize=1)
    # writer thread deliberately not started, so nothing is consumed
    for _ in range(4):
        pipeline.submit(msg("temp/techroom/1", '{"temp":1}'))

    assert pipeline.stats.received == 4
    assert pipeline.stats.dropped_full == 3
    assert pipeline._queue.qsize() == 1


def test_shutdown_drains_the_queue(water_spec):
    sink = RecordingSink()
    pipeline = Pipeline(_router(water_spec), [sink], maxsize=100)
    pipeline.start()
    for i in range(50):
        pipeline.submit(msg("temp/techroom/1", f'{{"temp":{i}.0}}'))
    pipeline.stop(timeout=10)

    assert len(sink.readings) == 50
    assert [r.fields["watertemperature"] for r in sink.readings] == [
        float(i) for i in range(50)
    ]
    assert sink.closed is True
    assert pipeline.stats.written == 50


def test_stop_closes_sinks_even_with_nothing_queued(water_spec):
    sink = RecordingSink()
    pipeline = Pipeline(_router(water_spec), [sink])
    pipeline.start()
    pipeline.stop(timeout=5)
    assert sink.closed is True


def test_stats_summary_is_readable(water_spec):
    pipeline = Pipeline(_router(water_spec), [RecordingSink()])
    pipeline._handle(msg("temp/techroom/1", '{"temp":21.5}'))
    assert "written=1" in pipeline.stats.summary()
