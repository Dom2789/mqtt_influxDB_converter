"""One number per payload, via regex or a plain parse.

Replaces the `message.split(":")[1][:-1]` hack in the pre-refactor on_message,
and covers sensors that publish a bare value rather than JSON.
"""

import re
from collections.abc import Iterable, Mapping
from typing import Any, TYPE_CHECKING

from ..model import FieldValue, RawMessage, Reading
from .base import DecodeError, register

if TYPE_CHECKING:
    from ..config import SensorSpec

_CASTS = {"float": float, "int": int, "str": str}


class ScalarDecoder:
    def __init__(self, options: Mapping[str, Any]):
        pattern = options.get("pattern")
        self._regex = re.compile(pattern) if pattern else None
        if self._regex is not None and self._regex.groups != 1:
            raise ValueError(
                f"pattern {pattern!r} must contain exactly one capture group, "
                f"found {self._regex.groups}"
            )
        cast_name = options.get("cast", "float")
        if cast_name not in _CASTS:
            raise ValueError(
                f"unknown cast {cast_name!r}; available: {', '.join(sorted(_CASTS))}"
            )
        self._cast = _CASTS[cast_name]

    def decode(self, msg: RawMessage, spec: "SensorSpec") -> Iterable[Reading]:
        try:
            text = msg.payload.decode("utf-8").strip()
        except UnicodeDecodeError as e:
            raise DecodeError(f"payload is not valid UTF-8: {e}") from e

        if self._regex is not None:
            match = self._regex.search(text)
            if match is None:
                raise DecodeError(
                    f"pattern {self._regex.pattern!r} did not match payload {text!r}"
                )
            raw = match.group(1)
        else:
            raw = text

        try:
            value: FieldValue = self._cast(raw)
        except ValueError as e:
            raise DecodeError(f"cannot parse {raw!r}: {e}") from e

        # the loader guarantees scalar sensors declare exactly one field
        field_name = next(iter(spec.fields))

        return (
            Reading(
                measurement=spec.measurement,
                fields={field_name: value},
                tags={},  # the pipeline merges static and topic-derived tags
                timestamp=msg.received_at,
            ),
        )


@register("scalar")
def _factory(options: Mapping[str, Any]) -> ScalarDecoder:
    return ScalarDecoder(options)
