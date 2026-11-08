"""Append readings to a date-stamped file.

Carries over MQTT.file_output and fixes its two latent bugs: the old
`file_name.split(".")` unpacked to exactly two parts, so any name with zero or
two-plus dots raised ValueError; and `self.path + "/"` double-slashed a path that
already ended in a separator.
"""

import logging
from collections.abc import Sequence
from pathlib import Path
from time import strftime

from ..config import FileSinkConfig
from ..model import Reading
from .stdout import format_reading

log = logging.getLogger("sink.file")


def dated_name(file_name: str, date: str | None = None) -> str:
    """readings.log -> readings_20261107.log; handles 0 or many dots."""
    stem, dot, suffix = file_name.rpartition(".")
    date = date if date is not None else strftime("%Y%m%d")
    if not dot:  # no extension at all
        return f"{file_name}_{date}"
    return f"{stem}_{date}{dot}{suffix}"


class FileSink:
    def __init__(self, cfg: FileSinkConfig):
        self._dir = Path(cfg.path)
        self._file_name = cfg.file_name
        log.info("file sink ready: %s", self._dir / dated_name(self._file_name))

    def _target(self) -> Path:
        # recomputed per write so the file rolls over at midnight
        return self._dir / dated_name(self._file_name)

    def write(self, readings: Sequence[Reading]) -> None:
        if not readings:
            return
        lines = "".join(f"{format_reading(r)}\n" for r in readings)
        with open(self._target(), "a", encoding="utf-8") as handle:
            handle.write(lines)

    def close(self) -> None:
        pass  # each write opens and closes its own handle
