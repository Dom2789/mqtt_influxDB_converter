"""JSON payload -> Reading, mapping field names to (optionally dotted) keys."""

import json
from collections.abc import Iterable, Mapping
from typing import Any, TYPE_CHECKING

from ..model import FieldValue, RawMessage, Reading
from .base import DecodeError, register

if TYPE_CHECKING:
    from ..config import SensorSpec

_CASTS = {"float": float, "int": int, "str": str, "bool": bool}


def _lookup(payload: Any, path: str) -> Any:
    """Resolve a dotted path, so `pm1:0.voltage` reaches a nested object."""
    current = payload
    for part in path.split("."):
        if not isinstance(current, Mapping):
            raise DecodeError(
                f"path {path!r}: cannot descend into {type(current).__name__}"
            )
        try:
            current = current[part]
        except KeyError:
            available = ", ".join(sorted(map(str, current))) or "<empty object>"
            raise DecodeError(
                f"path {path!r}: key {part!r} not found; available: {available}"
            ) from None
    return current


class JsonDecoder:
    def __init__(self, options: Mapping[str, Any]):
        cast_name = options.get("cast")
        if cast_name is not None and cast_name not in _CASTS:
            raise ValueError(
                f"unknown cast {cast_name!r}; available: {', '.join(sorted(_CASTS))}"
            )
        self._cast = _CASTS[cast_name] if cast_name else None
        self._ignore_missing = bool(options.get("ignore_missing", False))

    def decode(self, msg: RawMessage, spec: "SensorSpec") -> Iterable[Reading]:
        try:
            text = msg.payload.decode("utf-8")
        except UnicodeDecodeError as e:
            raise DecodeError(f"payload is not valid UTF-8: {e}") from e

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as e:
            raise DecodeError(f"payload is not valid JSON: {e}") from e

        fields: dict[str, FieldValue] = {}
        for field_name, path in spec.fields.items():
            try:
                value = _lookup(payload, path)
            except DecodeError:
                if self._ignore_missing:
                    continue
                raise

            if value is None or isinstance(value, (list, dict)):
                raise DecodeError(
                    f"field {field_name!r} at {path!r}: expected a scalar, "
                    f"got {type(value).__name__}"
                )

            if self._cast is not None:
                try:
                    value = self._cast(value)
                except (TypeError, ValueError) as e:
                    raise DecodeError(
                        f"field {field_name!r} at {path!r}: cannot cast "
                        f"{value!r} — {e}"
                    ) from e
            fields[field_name] = value

        if not fields:
            raise DecodeError("no fields could be decoded from the payload")

        return (
            Reading(
                measurement=spec.measurement,
                fields=fields,
                tags={},  # the pipeline merges static and topic-derived tags
                timestamp=msg.received_at,
            ),
        )


@register("json")
def _factory(options: Mapping[str, Any]) -> JsonDecoder:
    return JsonDecoder(options)
