"""InfluxDB sink: one long-lived client, batching WriteApi.

Batching, retry and exponential backoff are provided by influxdb-client's
WriteOptions (write_type defaults to WriteType.batching) and run on its own
thread pool, so write() returns immediately. This module deliberately implements
none of that itself.
"""

import logging
from collections.abc import Sequence

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

from ..config import InfluxCredentials, InfluxWriteConfig
from ..model import Reading

log = logging.getLogger("sink.influx")


def _to_point(reading: Reading) -> Point:
    point = Point(reading.measurement)
    for key, value in reading.tags.items():
        point = point.tag(key, value)
    for key, value in reading.fields.items():
        point = point.field(key, value)
    # explicit client-side timestamp rather than relying on server-side time
    return point.time(reading.timestamp, WritePrecision.NS)


class InfluxSink:
    def __init__(self, creds: InfluxCredentials, write_cfg: InfluxWriteConfig):
        self._bucket = creds.bucket
        self._client = InfluxDBClient(url=creds.url, token=creds.token, org=creds.org)
        self._write_api = self._client.write_api(
            write_options=WriteOptions(
                batch_size=write_cfg.batch_size,
                flush_interval=write_cfg.flush_interval_ms,
                max_retries=write_cfg.max_retries,
            ),
            error_callback=self._on_error,
            retry_callback=self._on_retry,
        )
        log.info(
            "influx sink ready: %s org=%s bucket=%s (batch_size=%d, flush=%dms)",
            creds.url, creds.org, creds.bucket,
            write_cfg.batch_size, write_cfg.flush_interval_ms,
        )

    @staticmethod
    def _on_error(conf, data, exception) -> None:
        bucket, org, _ = conf
        log.error(
            "influx write failed permanently (bucket=%s org=%s, %d byte(s) lost): %s",
            bucket, org, len(data or ""), exception,
        )

    @staticmethod
    def _on_retry(conf, data, exception) -> None:
        bucket, org, _ = conf
        log.warning(
            "influx write failed, retrying (bucket=%s org=%s): %s",
            bucket, org, exception,
        )

    def write(self, readings: Sequence[Reading]) -> None:
        if not readings:
            return
        # non-blocking: hands the batch to the WriteApi's own scheduler
        self._write_api.write(bucket=self._bucket, record=[_to_point(r) for r in readings])

    def close(self) -> None:
        try:
            self._write_api.close()  # flushes pending batches
        finally:
            self._client.close()
        log.info("influx sink closed")
