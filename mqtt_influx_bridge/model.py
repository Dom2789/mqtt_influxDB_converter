from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True, slots=True)
class RawMessage:
    topic: str
    payload: bytes
    received_at: datetime          # UTC, stamped on arrival

@dataclass(frozen=True, slots=True)
class Reading:
    measurement: str
    fields: Mapping[str, float | int | str | bool]
    tags: Mapping[str, str]
    timestamp: datetime