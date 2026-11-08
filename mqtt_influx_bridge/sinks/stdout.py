"""Prints readings. The --dry-run target, and the natural test double."""

import sys
from collections.abc import Sequence

from ..model import Reading


def format_reading(reading: Reading) -> str:
    tags = ",".join(f"{k}={v}" for k, v in sorted(reading.tags.items()))
    fields = ",".join(f"{k}={v!r}" for k, v in sorted(reading.fields.items()))
    return (
        f"{reading.timestamp.isoformat()} {reading.measurement}"
        f"[{tags}] {fields}"
    )


class StdoutSink:
    def __init__(self, stream=None):
        self._stream = stream if stream is not None else sys.stdout

    def write(self, readings: Sequence[Reading]) -> None:
        for reading in readings:
            print(format_reading(reading), file=self._stream)
        self._stream.flush()

    def close(self) -> None:
        self._stream.flush()


class RecordingSink:
    """Collects readings in memory. Used by the pipeline tests."""

    def __init__(self):
        self.readings: list[Reading] = []
        self.closed = False

    def write(self, readings: Sequence[Reading]) -> None:
        self.readings.extend(readings)

    def close(self) -> None:
        self.closed = True
