from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from mqtt_influx_bridge.model import RawMessage, Reading


def test_raw_message_keeps_the_payload_as_bytes():
    at = datetime(2026, 11, 8, 10, 30, tzinfo=UTC)
    msg = RawMessage("temp/techroom/1", b'{"temp":21.5}', at)
    assert msg.payload == b'{"temp":21.5}'
    assert msg.received_at.tzinfo is not None