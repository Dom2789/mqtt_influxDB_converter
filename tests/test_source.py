"""Spec for mqtt_influx_bridge.source — implementation plan step 8.

Written before the source exists, so the first run fails. No broker: the
paho client is swapped for a FakeClient before start(), and the callbacks
are called directly, the way paho's network thread would call them.
"""

import logging
from datetime import UTC, datetime
from types import SimpleNamespace

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from conftest import RecordingSink
from mqtt_influx_bridge.bridge import Counters, Router, handle
from mqtt_influx_bridge.config import MqttConfig
from mqtt_influx_bridge.model import RawMessage, SensorSpec
from mqtt_influx_bridge.source import MqttSource

CONFIG = MqttConfig(host="broker.test", port=1884)
FILTERS = {"temp/+/+", "powermeter/+/+/status/+"}

SUCCESS = ReasonCode(PacketTypes.CONNACK, "Success")
NOT_AUTHORIZED = ReasonCode(PacketTypes.CONNACK, "Not authorized")
DISCONNECT_ERROR = ReasonCode(PacketTypes.DISCONNECT, "Unspecified error")


class FakeClient:
    """Records the calls the source makes on its paho client."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.subscribed: list[str] = []

    def connect_async(self, host, port=1883, *args, **kwargs):
        self.calls.append(("connect_async", host, port))

    def loop_start(self):
        self.calls.append(("loop_start",))

    def loop_stop(self):
        self.calls.append(("loop_stop",))

    def disconnect(self, *args, **kwargs):
        self.calls.append(("disconnect",))

    def subscribe(self, topic, qos=0, *args, **kwargs):
        # paho accepts one filter or a list of (filter, qos) pairs
        if isinstance(topic, str):
            self.subscribed.append(topic)
        else:
            self.subscribed.extend(t if isinstance(t, str) else t[0] for t in topic)


def source(subscriptions=FILTERS) -> tuple[MqttSource, FakeClient]:
    s = MqttSource(CONFIG, subscriptions)
    fake = FakeClient()
    s.client = fake
    return s, fake


def paho_message(topic="temp/techroom/1", payload=b'{"temp":21.5}'):
    """Stands in for paho's MQTTMessage: the source reads only these two."""
    return SimpleNamespace(topic=topic, payload=payload)


# --------------------------------------------------------------------------
# construction
# --------------------------------------------------------------------------

def test_init_builds_a_paho_client_with_the_callbacks():
    s = MqttSource(CONFIG, FILTERS)
    assert isinstance(s.client, mqtt.Client)
    assert s.client.on_connect == s.on_connect
    assert s.client.on_disconnect == s.on_disconnect
    assert s.client.on_message == s.on_message


def test_init_does_not_touch_the_network():
    # building the source must work with no broker anywhere
    s = MqttSource(MqttConfig(host="no-such-host.invalid"), FILTERS)
    assert not s.client.is_connected()


# --------------------------------------------------------------------------
# start / stop
# --------------------------------------------------------------------------

def test_start_connects_async_and_starts_the_loop():
    s, fake = source()
    s.start(lambda m: None)
    assert fake.calls == [("connect_async", "broker.test", 1884), ("loop_start",)]


def test_start_does_not_subscribe():
    # subscribing belongs in on_connect, so it happens again on reconnect
    s, fake = source()
    s.start(lambda m: None)
    assert fake.subscribed == []


def test_stop_stops_the_loop_and_disconnects():
    s, fake = source()
    s.start(lambda m: None)
    fake.calls.clear()
    s.stop()
    assert sorted(fake.calls) == [("disconnect",), ("loop_stop",)]


# --------------------------------------------------------------------------
# on_connect / on_disconnect
# --------------------------------------------------------------------------

def test_on_connect_subscribes_to_every_filter():
    s, fake = source()
    s.on_connect(fake, None, None, SUCCESS, None)
    assert sorted(fake.subscribed) == sorted(FILTERS)


def test_every_reconnect_subscribes_again():
    s, fake = source()
    s.on_connect(fake, None, None, SUCCESS, None)
    s.on_connect(fake, None, None, SUCCESS, None)
    assert sorted(fake.subscribed) == sorted(list(FILTERS) * 2)


def test_failed_connect_does_not_subscribe():
    s, fake = source()
    s.on_connect(fake, None, None, NOT_AUTHORIZED, None)
    assert fake.subscribed == []


def test_failed_connect_is_logged_with_its_reason(caplog):
    s, fake = source()
    with caplog.at_level(logging.DEBUG):
        s.on_connect(fake, None, None, NOT_AUTHORIZED, None)
    assert any("Not authorized" in r.getMessage() for r in caplog.records
               if r.levelno >= logging.WARNING)


def test_successful_connect_is_logged(caplog):
    s, fake = source()
    with caplog.at_level(logging.DEBUG):
        s.on_connect(fake, None, None, SUCCESS, None)
    assert any("Success" in r.getMessage() for r in caplog.records)


def test_disconnect_is_logged_with_its_reason(caplog):
    s, fake = source()
    with caplog.at_level(logging.DEBUG):
        s.on_disconnect(fake, None, None, DISCONNECT_ERROR, None)
    assert any("Unspecified error" in r.getMessage() for r in caplog.records
               if r.levelno >= logging.WARNING)


# --------------------------------------------------------------------------
# on_message
# --------------------------------------------------------------------------

def test_message_is_passed_on_as_a_raw_message():
    received: list[RawMessage] = []
    s, fake = source()
    s.start(received.append)

    before = datetime.now(UTC)
    s.on_message(fake, None, paho_message("temp/techroom/1", b'{"temp":21.5}'))
    after = datetime.now(UTC)

    [m] = received
    assert m.topic == "temp/techroom/1"
    assert m.payload == b'{"temp":21.5}'
    assert m.received_at.tzinfo is not None
    assert before <= m.received_at <= after


def test_each_message_is_passed_on_once():
    received: list[RawMessage] = []
    s, fake = source()
    s.start(received.append)
    s.on_message(fake, None, paho_message("temp/a/1"))
    s.on_message(fake, None, paho_message("temp/b/2"))
    assert [m.topic for m in received] == ["temp/a/1", "temp/b/2"]


def test_whole_path_source_to_sink():
    # source -> handle -> recording sink, the wiring step 9 does for real
    spec = SensorSpec(
        name="probe",
        topic="temp/+/+",
        measurement="environment",
        fields={"watertemperature": "temp"},
        tags_from_topic={"room": 1, "device": 2},
    )
    router = Router([spec])
    sink = RecordingSink()
    counters = Counters()

    s, fake = source(router.subscriptions())
    s.start(lambda m: handle(m, router, [sink], counters))
    s.on_connect(fake, None, None, SUCCESS, None)
    s.on_message(fake, None, paho_message("temp/techroom/1", b'{"temp":21.5}'))

    assert fake.subscribed == ["temp/+/+"]
    [r] = sink.readings
    assert r.measurement == "environment"
    assert r.fields == {"watertemperature": 21.5}
    assert r.tags == {"room": "techroom", "device": "1"}
    assert counters == Counters(messages_in=1, readings_out=1, decode_errors=0)
