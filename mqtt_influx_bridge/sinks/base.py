"""Sink protocol — synchronous, because the writer thread owns all sinks."""

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from ..model import Reading


@runtime_checkable
class Sink(Protocol):
    def write(self, readings: Sequence[Reading]) -> None: ...

    def close(self) -> None:
        """Flush anything buffered and release resources."""
        ...
