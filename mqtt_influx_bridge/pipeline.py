"""Router + bounded queue + writer thread.

The queue is what stops a slow or unreachable sink from blocking the broker
loop. `submit()` runs on paho's network thread and does nothing but enqueue; all
routing, decoding and writing happens on the writer thread, where every message
is isolated by its own try/except.
"""

import logging
import queue
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from . import decoders
from .config import ConfigError, SensorSpec
from .decoders.base import DecodeError, Decoder
from .model import RawMessage, Reading
from .routing import tags_from_topic, topic_matches
from .sinks.base import Sink

log = logging.getLogger("pipeline")

_SENTINEL = object()


@dataclass(frozen=True, slots=True)
class Route:
    spec: SensorSpec
    decoder: Decoder


class Router:
    """Built once at startup; maps a topic to every sensor that matches it."""

    def __init__(self, routes: Sequence[Route]):
        self._routes = tuple(routes)

    @classmethod
    def from_specs(cls, specs: Iterable[SensorSpec]) -> "Router":
        """Instantiate each sensor's decoder, surfacing bad options at startup."""
        routes = []
        for spec in specs:
            try:
                decoder = decoders.build(spec.decoder, spec.decoder_options)
            except KeyError as e:
                raise ConfigError(f"sensor {spec.name!r}: {e.args[0]}") from e
            except (ValueError, TypeError) as e:
                raise ConfigError(
                    f"sensor {spec.name!r}: invalid decoder_options for "
                    f"{spec.decoder!r} — {e}"
                ) from e
            routes.append(Route(spec=spec, decoder=decoder))
        return cls(routes)

    def match(self, topic: str) -> list[Route]:
        """All matching routes — a broad and a specific filter can both apply."""
        return [r for r in self._routes if topic_matches(r.spec.topic, topic)]

    def subscriptions(self, qos: int) -> list[tuple[str, int]]:
        """Deduplicated topic filters, ready for client.subscribe()."""
        seen: dict[str, None] = {}
        for route in self._routes:
            seen.setdefault(route.spec.topic, None)
        return [(topic, qos) for topic in seen]

    def __len__(self) -> int:
        return len(self._routes)


@dataclass
class Stats:
    received: int = 0
    dropped_full: int = 0
    unrouted: int = 0
    decode_errors: int = 0
    sink_errors: int = 0
    written: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def summary(self) -> str:
        return (
            f"received={self.received} written={self.written} "
            f"unrouted={self.unrouted} decode_errors={self.decode_errors} "
            f"sink_errors={self.sink_errors} dropped_queue_full={self.dropped_full}"
        )


class Pipeline:
    def __init__(
        self,
        router: Router,
        sinks: Sequence[Sink],
        maxsize: int = 10_000,
    ):
        self._router = router
        self._sinks = tuple(sinks)
        self._queue: queue.Queue = queue.Queue(maxsize=maxsize)
        self._thread = threading.Thread(
            target=self._run, name="writer", daemon=True
        )
        self.stats = Stats()
        self._full_logged = False

    # ---- producer side: runs on paho's network thread ----------------------

    def submit(self, msg: RawMessage) -> None:
        """Enqueue a message. Never blocks, never raises."""
        self.stats.received += 1
        try:
            self._queue.put_nowait(msg)
            self._full_logged = False
        except queue.Full:
            self.stats.dropped_full += 1
            if not self._full_logged:
                # log once per congestion episode rather than per message
                log.error(
                    "queue full (maxsize=%d) — dropping messages; sink is not "
                    "keeping up. Further drops logged only on recovery.",
                    self._queue.maxsize,
                )
                self._full_logged = True

    # ---- consumer side: the writer thread ---------------------------------

    def start(self) -> None:
        self._thread.start()
        log.info("writer thread started (queue maxsize=%d)", self._queue.maxsize)

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is _SENTINEL:
                self._queue.task_done()
                return
            try:
                self._handle(item)
            except Exception:  # last line of defence: the thread must not die
                log.exception("unexpected error handling a message")
            finally:
                self._queue.task_done()

    def _handle(self, msg: RawMessage) -> None:
        routes = self._router.match(msg.topic)
        if not routes:
            self.stats.unrouted += 1
            log.debug("no sensor matches topic %r", msg.topic)
            return

        readings: list[Reading] = []
        for route in routes:
            try:
                readings.extend(self._decode(msg, route))
            except DecodeError as e:
                self.stats.decode_errors += 1
                log.warning(
                    "sensor %r: %s | topic=%s payload=%s",
                    route.spec.name, e, msg.topic, _excerpt(msg.payload),
                )
            except Exception as e:
                self.stats.decode_errors += 1
                log.exception(
                    "sensor %r: unexpected decode failure (%s) | topic=%s payload=%s",
                    route.spec.name, e, msg.topic, _excerpt(msg.payload),
                )

        if not readings:
            return

        for sink in self._sinks:
            try:
                sink.write(readings)
            except Exception:
                self.stats.sink_errors += 1
                log.exception("%s.write failed for %d reading(s)",
                              type(sink).__name__, len(readings))
        self.stats.written += len(readings)

    def _decode(self, msg: RawMessage, route: Route) -> list[Reading]:
        spec = route.spec
        try:
            topic_tags = tags_from_topic(msg.topic, spec.tags_from_topic)
        except IndexError as e:
            raise DecodeError(str(e)) from e

        merged_tags = {**spec.tags, **topic_tags}
        out = []
        for reading in route.decoder.decode(msg, spec):
            # decoders leave tags empty; the pipeline owns tag merging so no
            # decoder needs to know about static or topic-derived tags
            out.append(
                Reading(
                    measurement=reading.measurement,
                    fields=reading.fields,
                    tags={**merged_tags, **reading.tags},
                    timestamp=reading.timestamp,
                )
            )
        return out

    # ---- shutdown ---------------------------------------------------------

    def stop(self, timeout: float = 10.0) -> None:
        """Drain whatever is queued, then flush and close every sink."""
        if self._thread.is_alive():
            self._queue.put(_SENTINEL)
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                log.warning(
                    "writer thread did not finish within %.1fs; %d message(s) "
                    "left unprocessed", timeout, self._queue.qsize(),
                )
            else:
                log.info("writer thread drained")

        for sink in self._sinks:
            try:
                sink.close()
            except Exception:
                log.exception("%s.close failed", type(sink).__name__)

        log.info("pipeline stopped: %s", self.stats.summary())


def _excerpt(payload: bytes, limit: int = 120) -> str:
    text = payload[:limit].decode("utf-8", errors="replace")
    return f"{text!r}{'…' if len(payload) > limit else ''}"
