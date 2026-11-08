# Architecture: modular MQTT → InfluxDB converter

Status: **implemented** in `mqtt_influx_bridge/`. An earlier revision of this
document proposed an `aiomqtt`/`asyncio` design; see
[Decision: threading, not asyncio](#decision-threading-not-asyncio) for why the
modular design was kept and the async substrate was not.

## Why

The converter worked, but only for exactly one sensor. The old
`src/mqtt.py:33-53` (`on_message`) decoded the payload, parsed it with
`message.split(":")[1][:-1]`, printed it, opened a **new** `InfluxDBClient` per
message, and wrote a hardcoded
`Point("environment").tag("room","techroom").field("watertemperature", …)`. Every
piece of the sensor's identity was baked into that one method.

The clearest evidence that this was the real pain point: the `experiments` branch
contains the *same class* with a *different* hardcoded parser (Shelly JSON →
voltage/current/power). Supporting a second sensor meant editing core code or
maintaining a fork.

Three further problems made extension risky, all now fixed:

- **Config was hardcoded** at `main.py:7-8`, including a developer path
  (`/Users/dom/temp/influx.env`) that did not exist on the deployment host, where
  the systemd unit uses `/etc/automation/influx.env`. Three config subsystems
  existed but were unwired dead code: `src/paramters.py` (CLI parsing; no
  `ArgumentParser` was ever constructed), `ConfigToml`, and `setup_logging`.
- **One bad message killed the service.** `on_message` had no `try/except`, so a
  payload without a colon (`IndexError`), a non-numeric value (`ValueError`), or
  an Influx outage raised inside the paho network loop.
- **Persistence was gated on a display flag.** The Influx write sat in the `else`
  branch of `timestamp_active`, so `timestamp_active=True` silently disabled all
  writing. It only worked because `main.py:7` happened to pass `False`.

**Goal, achieved:** adding a sensor is a `[[sensor]]` block in `config.toml` — no
Python — while a genuinely new payload *format* costs one small decoder class.

## Target architecture

One-way flow, three replaceable roles, with a pure function in the middle:

```
paho ──► RawMessage ──► queue ──► Router ──► Decoder ──► Reading ──► Sink(s)
(source,               (bounded,  (topic→    (pure,                 (Influx,
 auto-reconnect)        decouples  sensors)   no I/O)                 stdout, file)
                        broker
                        from DB)
```

`on_message` runs on paho's network thread and does nothing but stamp the arrival
time and enqueue. Routing, decoding and writing all happen on the writer thread,
where each message is isolated by its own `try/except`. The queue is not
incidental: it is what stops a slow or unreachable sink from blocking the broker
loop, which the pre-refactor `write_api.write()` did on every message.

### Package layout

```
mqtt_influx_bridge/
  __main__.py            # argparse, signals, wiring, shutdown order
  config.py              # TOML -> frozen dataclasses, eager validation
  model.py               # RawMessage, Reading
  routing.py             # MQTT wildcard matching, tags-from-topic extraction
  pipeline.py            # Router, bounded queue, writer thread, error isolation
  sources/base.py        # Source protocol
  sources/mqtt.py        # paho source; reconnect is paho's job, not ours
  decoders/base.py       # Decoder protocol + name registry
  decoders/scalar.py     # regex/plain numeric extraction
  decoders/json_data.py  # JSON payload -> fields via dotted paths
  sinks/base.py          # Sink protocol
  sinks/influx.py        # long-lived client, batching WriteApi
  sinks/stdout.py        # --dry-run target, and RecordingSink for tests
  sinks/file.py          # replaces MQTT.file_output
  _lib/                  # carried over: ConfigToml, setup_logging
tests/                   # pytest; no broker, no database, no event loop
```

`src/` and `main.py` are retired. There was no `src/__init__.py`, so imports only
worked from the repo root as an implicit namespace package; the new package fixes
that and gains a console-script entry point.

### Core types (`model.py`)

```python
@dataclass(frozen=True, slots=True)
class RawMessage:
    topic: str
    payload: bytes
    received_at: datetime          # UTC, stamped at arrival

@dataclass(frozen=True, slots=True)
class Reading:
    measurement: str
    fields: Mapping[str, float | int | str | bool]
    tags: Mapping[str, str]
    timestamp: datetime
```

`Reading` is the universal currency, and deliberately **not** an Influx `Point`.
Decoders never import `influxdb_client`, which keeps them pure and makes a second
sink cost nothing. This continues the `frozen=True` dataclass style the project
already used for its Influx parameters.

### The three protocols

```python
class Source(Protocol):
    def start(self, on_message: Callable[[RawMessage], None]) -> None: ...
    def stop(self) -> None: ...

class Decoder(Protocol):                      # pure: no I/O
    def decode(self, msg: RawMessage, spec: SensorSpec) -> Iterable[Reading]: ...

class Sink(Protocol):
    def write(self, readings: Sequence[Reading]) -> None: ...
    def close(self) -> None: ...              # must flush
```

`Decoder` being pure is the single most important decision here: it makes every
payload format testable with a plain assert, no broker and no database. Decoders
also leave `tags` empty — the pipeline merges static and topic-derived tags, so no
decoder needs to know tags exist.

### Decoder registry

Config names a decoder by string. An unknown name fails **at startup** with the
list of valid options, not at the first message. Adding a format = a new file in
`decoders/` plus one decorator and one import.

### Configuration

`config.toml` holds only non-secret settings and is safe to commit. InfluxDB
credentials come from the process environment, which the systemd unit populates
via `EnvironmentFile`; naming one in `[influx]` is a startup error. The
`dotenv_values(path) | environ` precedence from the original
`Influx_parameters.py` is kept — process environment wins, which is correct for
the deployment.

The loader produces frozen `AppConfig` / `MqttConfig` / `InfluxWriteConfig` /
`SensorSpec` / `QueueConfig` / `LoggingConfig` / `FileSinkConfig` and validates
eagerly: unknown decoder, unknown key (with the valid ones listed), malformed
topic filter, duplicate sensor name, out-of-range `tags_from_topic` index,
non-string tag value, a scalar sensor with more than one field, and missing
Influx credentials — each a `ConfigError` naming the field.

### Routing

`routing.topic_matches` implements MQTT `+` / `#` semantics, including the rule
that wildcards do not match `$`-prefixed topics. `tags_from_topic` turns topic
levels into tags, which is what lets a single `[[sensor]]` block cover a whole
fleet of identical devices — the key mechanism for scaling sensor count. The
router is built once at startup; `match()` returns **all** matching routes, since
a broad and a specific filter can both cover one topic. Subscriptions are the
deduplicated set of sensor patterns.

A `tags_from_topic` index is bounds-checked at startup against the pattern, except
where the pattern ends in `#` — the topic length is then unbounded, so only the
runtime check in `tags_from_topic` can decide, and it surfaces as a handled
per-message decode error.

### Reliability

- **Per-message isolation** — decode errors are caught, logged with topic and a
  payload excerpt, and counted. A malformed message is dropped, never fatal. A
  failing sink does not prevent the others from receiving the reading.
- **Long-lived Influx client**, replacing the per-message `InfluxDBClient`, which
  cost a fresh TCP (and TLS) handshake per reading.
- **Batching and retry come from the library.** `WriteOptions` (batch size, flush
  interval, `max_retries` with exponential backoff) runs on influxdb-client's own
  thread pool, so `write()` returns immediately. `error_callback` and
  `retry_callback` are wired to the log so async failures are visible.
- **Explicit timestamps** from `RawMessage.received_at` instead of relying on
  server-side time.
- **Reconnect is paho's job.** `reconnect_delay_set(min_delay, max_delay)` gives
  exponential backoff and `connect_async` + `loop_start` retries a broker that is
  down at startup. Subscriptions are issued inside `on_connect`, so a reconnect
  renews them automatically.
- **Bounded queue** with an explicit overflow policy: log once per congestion
  episode, count the drops, never block the network thread.
- **Graceful shutdown** on SIGTERM/SIGINT, in this order: stop ingest, drain the
  queue, flush and close every sink. This matters because the unit is
  `Restart=always`. `KillSignal=SIGTERM` and `TimeoutStopSec=30` give the drain
  room to finish.
- **Logging to stdout** by default, which journald captures. `setup_logging` keeps
  its original file mode as an option.

## Decision: threading, not asyncio

An earlier revision of this document proposed `aiomqtt` + `asyncio` +
`influxdb-client[async]`. The modular design above was kept; the async substrate
was rejected. The reasoning, since it will come up again:

- **More sensors is not more concurrency.** N sensors ride on *one* MQTT
  connection via wildcard subscriptions, plus *one* batched InfluxDB connection.
  That is two I/O endpoints whether N is 1 or 500. asyncio's advantage is cheap
  concurrency across many simultaneous waits; there are two here. Growing the
  sensor count grows message *throughput*, not concurrency.
- **The sync stack already provides the two reliability features the async design
  would have hand-written.** paho's `reconnect_delay_set` is exponential-backoff
  auto-reconnect; `influxdb_client.WriteOptions` defaults to `WriteType.batching`
  with batch size, flush interval, retry interval, `max_retries` and
  `exponential_base`, on its own thread pool. `InfluxDBClientAsync` has no
  batching WriteApi at all. Choosing asyncio would mean owning both mechanisms as
  project code rather than library guarantees.
- **Decoders are pure and synchronous either way**, so with threads the *whole*
  pipeline is sync: the tests need no event loop and no `pytest-asyncio`.
- **Zero new runtime dependencies**, versus `aiomqtt` + `aiohttp`.
- **CPU headroom has an escape path.** Decoding is GIL-bound Python in both
  models, but threads can be scaled out (more writer threads, or a free-threaded
  interpreter); a single-threaded event loop cannot.

This decision assumes every sensor **pushes** over MQTT. It should be revisited if
many sensors ever need active polling (REST, Modbus, BLE) on independent timers:
dozens-to-hundreds of concurrent timed I/O waits is exactly where asyncio wins,
and the `Source` protocol is the seam where that would plug in.

## Verification

**Unit (no infrastructure required):** `uv run pytest` — 136 tests covering

- `routing` — `+` / `#` / exact / non-match, `$`-topic exclusion, multi-match,
  tag extraction including an out-of-range index.
- decoders — the real water-temperature payload, the Shelly payload from the
  `experiments` branch, dotted paths, missing keys, null and non-scalar values,
  non-UTF-8 payloads, uncastable values, and bad decoder options rejected at
  construction. Each expects a handled `DecodeError` rather than a traceback.
- `config` — valid round-trip; `config.toml` and `config.example.toml` both load;
  every `ConfigError` path listed under [Configuration](#configuration); and the
  env-file precedence rule.
- `pipeline` — a recording sink plus direct message injection: the exact
  `Reading`s arrive, tags merge correctly, one wildcard block covers a fleet, one
  bad message does not stop the rest, a failing sink does not lose the others,
  `queue.Full` counts drops without raising, and shutdown drains the queue. These
  were impossible before, since `subscribe()` blocked forever.
- `sinks/influx` — the rendered line protocol, including the explicit nanosecond
  timestamp and why `cast = "float"` avoids an integer/float field conflict.
- `sources/mqtt` — the full source → queue → writer → sink path with paho's
  callbacks driven directly, and `on_connect` resubscribing on every connect.

**Integration (needs the broker and InfluxDB):**

1. `uv run mqtt-influx-bridge --dry-run`, then
   `mosquitto_pub -h 192.168.1.225 -t temp/techroom/1 -m '{"temp":21.5}'` — the
   decoded `Reading` prints and nothing is written.
2. Same without `--dry-run`, then confirm in Influx:
   `from(bucket:"…") |> range(start:-5m) |> filter(fn:(r) => r._measurement == "environment")`
   shows `watertemperature` tagged `room=techroom`.
3. Stop Influx mid-run and publish — expect retry/backoff logs from the WriteApi
   and no crash; restart it and confirm buffered points land.
4. Stop the **broker** mid-run — expect reconnect-backoff logs, then automatic
   resubscribe and resumed writes. This is the behaviour the asyncio design would
   have given up.
5. Publish `-m 'garbage'` — one warning line naming the topic, service still alive.
6. Add a second `[[sensor]]` block with no code change and confirm both topics
   write; then a wildcard sensor with `tags_from_topic` for several devices.
7. `systemctl restart`, then `journalctl -u mqtt_influxDB_converter` for a clean
   startup and a drained shutdown with no traceback.
