"""Source protocol — push-based, because MQTT delivers via callback."""

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from ..model import RawMessage


@runtime_checkable
class Source(Protocol):
    def start(self, on_message: Callable[[RawMessage], None]) -> None:
        """Begin delivering messages. Must not block."""
        ...

    def stop(self) -> None:
        """Stop delivering messages and disconnect."""
        ...
