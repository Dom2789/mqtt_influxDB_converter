from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

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

@dataclass(frozen=True, slots=True)
class SensorSpec:
    """One [[sensor]] block from config.toml."""
    name: str
    topic: str                                     # MQTT filter, may contain + and #
    measurement: str
    fields: Mapping[str, str]                      # influx field name -> JSON key (dotted path allowed)
    cast: Literal["float", "int", "str", "bool"] = "float"
    tags: Mapping[str, str] = field(default_factory=dict)            # static tags
    tags_from_topic: Mapping[str, int] = field(default_factory=dict) # tag -> topic level index
