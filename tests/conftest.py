"""Shared test doubles. Lives here, not in the shipped package."""

from collections.abc import Sequence

from mqtt_influx_bridge.model import Reading


class RecordingSink:
    """A sink that keeps what it was given. The test instrument for step 7."""

    def __init__(self) -> None:
        self.readings: list[Reading] = []
        self.closed = False

    def write(self, readings: Sequence[Reading]) -> None:
        self.readings.extend(readings)

    def close(self) -> None:
        self.closed = True
