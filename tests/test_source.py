"""Wiring tests: the full source -> queue -> writer -> sink path.

paho's own callbacks are driven directly, so everything except the TCP socket is
exercised. That covers the two claims that matter most operationally: on_message
only enqueues, and on_connect resubscribes so reconnects are transparent.
"""

from dataclasses import dataclass

from mqtt_influx_bridge.config import MqttConfig, SensorSpec
from mqtt_influx_bridge.pipeline import Pipeline, Router
from mqtt_influx_bridge.sinks.stdout import RecordingSink
from mqtt_influx_bridge.sources.mqtt import MqttSource


@dataclass
class FakeMessage:
    topic: str
    payload: bytes


@dataclass
class FakeReasonCode:
    is_failure: bool = False
    value: int = 1

    def __str__(self) -> str:
        return "Success" if not self.is_failure else "Refused"


class FakeClient:
    def __init__(self):
        self.subscriptions: list = []

    def subscribe(self, topics):
        self.subscriptions.append(topics)


def _specs() -> tuple[SensorSpec, ...]:
    return (
        SensorSpec(
            name="techroom-water", topic="temp/techroom/1", decoder="json",
            measurement="environment", fields={"watertemperature": "temp"},
            tags={"room": "techroom"}, decoder_options={"cast": "float"},
        ),
        SensorSpec(
            name="shelly-pm", topic="shellies/+/status/pm1:0", decoder="json",
            measurement="power",
            fields={"voltage": "voltage", "power": "apower"},
            tags={"device": "shelly"}, tags_from_topic={"device_id": 1},
            decoder_options={"cast": "float"},
        ),
    )


def _build():
    router = Router.from_specs(_specs())
    sink = RecordingSink()
    pipeline = Pipeline(router, [sink], maxsize=100)
    source = MqttSource(MqttConfig(host="broker.invalid"), router.subscriptions(1))
    return source, pipeline, sink


def test_end_to_end_without_a_socket():
    source, pipeline, sink = _build()
    pipeline.start()
    source.start(pipeline.submit)
    try:
        # deliver exactly what the broker would hand to on_message
        source._handle_message(
            None, None, FakeMessage("temp/techroom/1", b'{"temp":21.5}')
        )
        source._handle_message(
            None, None,
            FakeMessage(
                "shellies/kitchen/status/pm1:0",
                b'{"voltage": 231.4, "current": 0.5, "apower": 118.7}',
            ),
        )
        source._handle_message(
            None, None, FakeMessage("temp/techroom/1", b"not json at all")
        )
    finally:
        source.stop()
        pipeline.stop(timeout=10)

    water, shelly = sink.readings
    assert water.measurement == "environment"
    assert water.fields == {"watertemperature": 21.5}
    assert water.tags == {"room": "techroom"}
    assert water.timestamp.tzinfo is not None  # stamped in UTC on arrival

    assert shelly.measurement == "power"
    assert shelly.fields == {"voltage": 231.4, "power": 118.7}
    assert shelly.tags == {"device": "shelly", "device_id": "kitchen"}

    assert pipeline.stats.received == 3
    assert pipeline.stats.written == 2
    assert pipeline.stats.decode_errors == 1   # the garbage payload, survived
    assert sink.closed is True


def test_subscriptions_are_issued_on_connect():
    """Subscribing inside on_connect is what makes a reconnect transparent."""
    source, _, _ = _build()
    client = FakeClient()

    source._handle_connect(client, None, {}, FakeReasonCode(), None)
    assert client.subscriptions == [
        [("temp/techroom/1", 1), ("shellies/+/status/pm1:0", 1)]
    ]

    # a second connect (i.e. after a reconnect) resubscribes identically
    source._handle_connect(client, None, {}, FakeReasonCode(), None)
    assert len(client.subscriptions) == 2
    assert client.subscriptions[0] == client.subscriptions[1]


def test_refused_connect_does_not_subscribe():
    source, _, _ = _build()
    client = FakeClient()
    source._handle_connect(client, None, {}, FakeReasonCode(is_failure=True), None)
    assert client.subscriptions == []


def test_callbacks_tolerate_being_called_before_start():
    """paho may deliver a late message after stop(); it must not raise."""
    source, _, _ = _build()
    source._handle_message(None, None, FakeMessage("temp/techroom/1", b"{}"))


def test_disconnect_callbacks_do_not_raise():
    source, _, _ = _build()
    source._handle_disconnect(None, None, {}, FakeReasonCode(), None)
    source._handle_disconnect(None, None, {}, FakeReasonCode(is_failure=True), None)
    source._handle_subscribe(None, None, 1, [FakeReasonCode(), FakeReasonCode()], None)
    source._handle_subscribe(
        None, None, 1, [FakeReasonCode(is_failure=True), FakeReasonCode()], None
    )
