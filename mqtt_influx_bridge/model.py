"""Core data types passed between the pipeline stages.

`Reading` is deliberately *not* an InfluxDB `Point`: decoders never import
`influxdb_client`, which is what keeps them pure and makes a second sink cheap.
"""

from dataclasses import dataclass
from datetime import datetime
from collections.abc import Mapping

FieldValue = float | int | str | bool


@dataclass(frozen=True, slots=True)
class RawMessage:
    """An MQTT message as received, before any interpretation."""

    topic: str
    payload: bytes
    received_at: datetime  # UTC, stamped on arrival


@dataclass(frozen=True, slots=True)
class Reading:
    """One measurement ready to be written to any sink."""

    measurement: str
    fields: Mapping[str, FieldValue]
    tags: Mapping[str, str]
    timestamp: datetime
