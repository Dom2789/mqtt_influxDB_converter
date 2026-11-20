"""From one MQTT message to readings in every sink.

handle() runs on paho's network thread. loop_start runs exactly one such
thread, so handle has one caller and the counters need no lock.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

from paho.mqtt.client import topic_matches_sub

from .decoders import decode_json
from .errors import BridgeError
from .model import RawMessage, Reading, SensorSpec
from .routing import tags_from_topic

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Counters:
    """Running totals since start. They only ever grow.

    decode_errors counts every message dropped for a BridgeError, which
    includes a TagIndexError as well as a DecodeError.
    """
    messages_in: int = 0
    readings_out: int = 0
    decode_errors: int = 0

    def since(self, previous: "Counters") -> "Counters":
        """The field-wise difference self - previous; neither is changed."""
        return Counters(
            messages_in=self.messages_in - previous.messages_in,
            readings_out=self.readings_out - previous.readings_out,
            decode_errors=self.decode_errors - previous.decode_errors,
        )


class Router:
    """The sensor specs from config, looked up by topic."""

    def __init__(self, sensors: Sequence[SensorSpec]) -> None:
        self.sensors: tuple[SensorSpec, ...] = tuple(sensors)

    def match(self, topic: str) -> list[SensorSpec]:
        """All specs whose filter matches topic, in config order."""
        matches:list[SensorSpec] = []

        for sensor in self.sensors:
            if topic_matches_sub(sensor.topic, topic):
                matches.append(sensor)

        return matches

    def subscriptions(self) -> set[str]:
        """The topic filters to subscribe to, deduplicated."""
        return {s.topic for s in self.sensors}


def handle(msg: RawMessage, router: Router, sinks: Sequence[Any], counters: Counters) -> None:
    """Decode msg with every matching spec, tag the readings, write them out.

    A BridgeError drops the whole message: it is logged with the topic and a
    payload excerpt, counted, and nothing from it is written. Any other
    exception is a bug and propagates. Each sink is called separately, so one
    that raises does not stop the others.
    """
    readings = []
    try:
        counters.messages_in += 1
        matched = router.match(msg.topic)

        for spec in matched:
            for reading in decode_json(msg, spec):
                reading = with_tags(reading, spec, msg.topic)
                readings.append(reading)

        counters.readings_out += len(readings)

    except BridgeError as e:
        counters.decode_errors += 1
        log.warning("dropped message on %s: %s (payload %r)", msg.topic, e, msg.payload[:200])
    else:

        for sink in sinks:
            try:
                sink.write(readings)
            except Exception:
                log.exception("sink %r failed", sink)


def with_tags(reading: Reading, spec: SensorSpec, topic: str) -> Reading:
    """Static tags from spec, then tags_from_topic (which wins on a clash)."""
    tags = {**spec.tags, **tags_from_topic(topic, spec.tags_from_topic)}

    return replace(reading, tags=tags)
