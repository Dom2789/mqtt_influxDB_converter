"""Where readings go: the terminal (--dry-run) or InfluxDB.

The sink contract, shared by both classes (a docstring, not a Protocol):

    write(readings)  takes a sequence of Readings, so a sink is free to batch.
                     It must not block on the network and must not raise for
                     a failed write; failures are logged.
    close()          flushes anything still pending, then releases resources.
                     After close() the sink is not used again.
"""

import logging
from collections.abc import Sequence

from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import WriteApi

from .config import InfluxWriteConfig
from .model import Reading

log = logging.getLogger(__name__)


class StdoutSink:
    """Prints one readable line per reading. What --dry-run swaps in."""

    def write(self, readings: Sequence[Reading]) -> None:
        for r in readings:
            tags = " ".join(f"{k}={v}" for k, v in r.tags.items())
            fields = " ".join(f"{k}={v}" for k, v in r.fields.items())
            print(r.timestamp.isoformat(), r.measurement, tags, fields)

    def close(self) -> None:
        pass  # nothing buffered, nothing to release


class InfluxSink:
    """Writes readings to InfluxDB through one long-lived, batching WriteApi.

    The client and the write API are built once in __init__, with
    WriteOptions from InfluxWriteConfig (batch size, flush interval) plus
    retries with exponential backoff, and error/retry callbacks wired to the
    logger, since batched writes fail asynchronously.
    """

    def __init__(self, config: InfluxWriteConfig) -> None:
        self.bucket = config.bucket
        self.org = config.org
        self._client = InfluxDBClient(url=config.url, token=config.token, org=config.org)
        self._write_api: WriteApi = self._client.write_api(
            write_options=WriteOptions(
                batch_size=config.batch_size,
                flush_interval=config.flush_interval_ms,
                max_retries=5,           # then error_callback gives up on the batch
                retry_interval=5_000,    # ms before the first retry ...
                exponential_base=2,      # ... doubling each time after
            ),
            error_callback=self._on_error,
            retry_callback=self._on_retry,
        )

    def write(self, readings: Sequence[Reading]) -> None:
        # Batching mode: this queues and returns; failures reach the callbacks.
        self._write_api.write(
            bucket=self.bucket,
            org=self.org,
            record=[self._to_point(r) for r in readings],
        )

    def close(self) -> None:
        """Close the write API first, then the client — that order flushes."""
        self._write_api.close()
        self._client.close()

    @staticmethod
    def _to_point(reading: Reading) -> Point:
        """Reading -> Point, stamped from reading.timestamp at ns precision."""
        point = Point(reading.measurement).time(reading.timestamp, WritePrecision.NS)
        for key, value in reading.tags.items():
            point.tag(key, value)
        for key, value in reading.fields.items():
            point.field(key, value)
        return point

    @staticmethod
    def _on_error(conf: tuple[str, str, str], data: str, exception: Exception) -> None:
        """error_callback: a batch was given up on after all retries."""
        bucket, org, _ = conf
        log.error("influx write to %s/%s failed, batch dropped: %s", org, bucket, exception)

    @staticmethod
    def _on_retry(conf: tuple[str, str, str], data: str, exception: Exception) -> None:
        """retry_callback: a batch failed and will be retried."""
        bucket, org, _ = conf
        log.warning("influx write to %s/%s failed, retrying: %s", org, bucket, exception)
