"""Decoder protocol and name registry.

Decoders are synchronous and pure: no I/O, no shared state. That is what makes
every payload format testable with a plain assert, no broker and no database.
"""

from collections.abc import Iterable, Mapping
from typing import Any, Callable, Protocol, TYPE_CHECKING, runtime_checkable

from ..model import RawMessage, Reading

if TYPE_CHECKING:  # avoid a circular import at runtime
    from ..config import SensorSpec


class DecodeError(ValueError):
    """Raised for a payload this decoder cannot interpret.

    The pipeline catches this per message: it is logged and counted, never fatal.
    """


@runtime_checkable
class Decoder(Protocol):
    def decode(
        self, msg: RawMessage, spec: "SensorSpec"
    ) -> Iterable[Reading]: ...


DecoderFactory = Callable[[Mapping[str, Any]], Decoder]

_DECODERS: dict[str, DecoderFactory] = {}


def register(name: str) -> Callable[[DecoderFactory], DecoderFactory]:
    """@register("json") — makes a decoder selectable by name from config."""

    def deco(factory: DecoderFactory) -> DecoderFactory:
        if name in _DECODERS:
            raise RuntimeError(f"decoder {name!r} is already registered")
        _DECODERS[name] = factory
        return factory

    return deco


def available() -> list[str]:
    return sorted(_DECODERS)


def build(name: str, options: Mapping[str, Any]) -> Decoder:
    """Instantiate a decoder by config name.

    Raises KeyError with the valid options so an unknown name fails at startup
    rather than on the first message.
    """
    try:
        factory = _DECODERS[name]
    except KeyError:
        raise KeyError(
            f"unknown decoder {name!r}; available: {', '.join(available())}"
        ) from None
    return factory(options)
