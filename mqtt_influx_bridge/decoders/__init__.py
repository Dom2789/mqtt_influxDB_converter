"""Importing this package registers every built-in decoder.

Adding a format = a new module here plus one @register decorator, then one line
below so the registry is populated before config validation runs.
"""

from . import json_data, scalar  # noqa: F401  (imported for the side effect)
from .base import DecodeError, Decoder, available, build, register

__all__ = ["DecodeError", "Decoder", "available", "build", "register"]
