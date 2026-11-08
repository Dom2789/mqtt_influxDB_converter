from datetime import datetime, timezone

import pytest

from mqtt_influx_bridge.config import SensorSpec
from mqtt_influx_bridge.model import RawMessage

TS = datetime(2026, 11, 7, 12, 0, 0, tzinfo=timezone.utc)


def msg(topic: str, payload: str | bytes, received_at: datetime = TS) -> RawMessage:
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    return RawMessage(topic=topic, payload=data, received_at=received_at)


@pytest.fixture
def water_spec() -> SensorSpec:
    return SensorSpec(
        name="techroom-water",
        topic="temp/techroom/1",
        decoder="json",
        measurement="environment",
        fields={"watertemperature": "temp"},
        tags={"room": "techroom"},
    )


@pytest.fixture
def shelly_spec() -> SensorSpec:
    return SensorSpec(
        name="shelly-pm",
        topic="shellies/+/status/pm1:0",
        decoder="json",
        measurement="power",
        fields={"voltage": "voltage", "current": "current", "power": "apower"},
        tags={"device": "shelly"},
        tags_from_topic={"device_id": 1},
    )
