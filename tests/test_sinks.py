"""Spec for mqtt_influx_bridge.sinks — implementation plan step 6.

Written before the sinks exist, so the first run fails. No database needed:
the Influx sink is checked through the line protocol its points render to,
and through a fake WriteApi swapped in after construction. The real database
comes in step 10.
"""

import logging
from datetime import UTC, datetime, timedelta, timezone

import pytest

from mqtt_influx_bridge.config import InfluxWriteConfig
from mqtt_influx_bridge.model import Reading
from mqtt_influx_bridge.sinks import InfluxSink, StdoutSink

AT = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
AT_NS = 1794823200000000000              # AT in nanoseconds since the epoch

CONFIG = InfluxWriteConfig(
    url="http://localhost:1",            # never contacted: nothing is flushed to it
    token="tok",
    org="homelab",
    bucket="telemetry",
    batch_size=7,
    flush_interval_ms=1234,
)


def reading(fields=None, tags=None, measurement="environment", timestamp=AT) -> Reading:
    return Reading(
        measurement=measurement,
        fields={"watertemperature": 21.5} if fields is None else fields,
        tags={"room": "techroom", "device": "1"} if tags is None else tags,
        timestamp=timestamp,
    )


def line(r: Reading) -> str:
    return InfluxSink._to_point(r).to_line_protocol()


# --------------------------------------------------------------------------
# StdoutSink
# --------------------------------------------------------------------------

def test_stdout_one_line_per_reading(capsys):
    StdoutSink().write([reading(), reading(), reading()])
    assert len(capsys.readouterr().out.splitlines()) == 3


def test_stdout_line_shows_the_whole_reading(capsys):
    StdoutSink().write([reading()])
    out = capsys.readouterr().out
    assert "environment" in out
    assert "room=techroom" in out
    assert "device=1" in out
    assert "watertemperature=21.5" in out
    assert AT.isoformat() in out


def test_stdout_empty_write_prints_nothing(capsys):
    StdoutSink().write([])
    assert capsys.readouterr().out == ""


def test_stdout_close_is_harmless(capsys):
    sink = StdoutSink()
    sink.write([reading()])
    sink.close()
    assert len(capsys.readouterr().out.splitlines()) == 1


# --------------------------------------------------------------------------
# InfluxSink._to_point — the rendered line protocol
# --------------------------------------------------------------------------

def test_point_line_protocol():
    # tags come out sorted by key, whatever order the Reading had them in
    assert line(reading()) == f"environment,device=1,room=techroom watertemperature=21.5 {AT_NS}"


def test_point_without_tags():
    assert line(reading(tags={})) == f"environment watertemperature=21.5 {AT_NS}"


def test_point_with_several_fields():
    r = reading(fields={"voltage": 225.1, "power": 21.7})
    assert line(r) == f"environment,device=1,room=techroom power=21.7,voltage=225.1 {AT_NS}"


def test_float_without_fraction_stays_a_float():
    # The series-type trap from the plan: cast="float" gives 21.0, and that
    # must reach Influx as a float. The client renders it as "21" — no "i"
    # suffix, which in line protocol *is* a float. "21i" would be an int.
    out = line(reading(fields={"watertemperature": 21.0}))
    assert " watertemperature=21 " in out


def test_int_field_has_the_i_suffix():
    assert " energy=40503i " in line(reading(fields={"energy": 40503}))


def test_bool_field():
    assert " output=true " in line(reading(fields={"output": True}))


def test_str_field_is_quoted():
    assert ' state="on" ' in line(reading(fields={"state": "on"}))


def test_timestamp_is_nanoseconds_from_the_reading():
    # explicit precision: the stamp is the reading's, never the server's
    assert line(reading()).endswith(f" {AT_NS}")


def test_timestamp_with_another_timezone_is_the_same_instant():
    cet = AT.astimezone(timezone(timedelta(hours=1)))
    assert line(reading(timestamp=cet)).endswith(f" {AT_NS}")


# --------------------------------------------------------------------------
# InfluxSink — construction, write, close
# --------------------------------------------------------------------------

@pytest.fixture
def sink():
    s = InfluxSink(CONFIG)
    yield s
    s.close()


class FakeWriteApi:
    def __init__(self, calls: list) -> None:
        self.calls = calls

    def write(self, **kwargs) -> None:
        self.calls.append(("write", kwargs))

    def close(self) -> None:
        self.calls.append(("write_api.close", None))


class FakeClient:
    def __init__(self, calls: list) -> None:
        self.calls = calls

    def close(self) -> None:
        self.calls.append(("client.close", None))


@pytest.fixture
def faked():
    """An InfluxSink whose real client is closed and replaced by recorders."""
    s = InfluxSink(CONFIG)
    s._write_api.close()
    s._client.close()
    calls: list = []
    s._write_api = FakeWriteApi(calls)
    s._client = FakeClient(calls)
    return s, calls


def test_bucket_and_org_are_kept(sink):
    assert sink.bucket == "telemetry"
    assert sink.org == "homelab"


def test_write_options_come_from_the_config(sink):
    options = sink._write_api._write_options
    assert options.batch_size == 7
    assert options.flush_interval == 1234


def test_write_options_retry(sink):
    options = sink._write_api._write_options
    assert options.max_retries > 0
    assert options.exponential_base > 1


def test_error_and_retry_callbacks_are_wired(sink):
    # without these, asynchronous write failures are invisible
    assert sink._write_api._error_callback is not None
    assert sink._write_api._retry_callback is not None


def test_write_sends_all_readings_in_one_call(faked):
    s, calls = faked
    s.write([reading(), reading(fields={"watertemperature": 22.0})])
    [(name, kwargs)] = calls
    assert name == "write"
    assert kwargs["bucket"] == "telemetry"
    assert kwargs["org"] == "homelab"
    assert [p.to_line_protocol() for p in kwargs["record"]] == [
        f"environment,device=1,room=techroom watertemperature=21.5 {AT_NS}",
        f"environment,device=1,room=techroom watertemperature=22 {AT_NS}",
    ]


def test_close_closes_write_api_before_client(faked):
    # closing the write API is what flushes the last batch; the client must
    # still be open when that happens
    s, calls = faked
    s.close()
    assert [name for name, _ in calls] == ["write_api.close", "client.close"]


# --------------------------------------------------------------------------
# Callbacks — write failures end up in the log
# --------------------------------------------------------------------------

CONF = ("telemetry", "homelab", "ns")


def test_on_error_logs_at_error(caplog):
    with caplog.at_level(logging.DEBUG):
        InfluxSink._on_error(CONF, "environment watertemperature=21.5", Exception("boom"))
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "boom" in record.getMessage()


def test_on_retry_logs_at_warning(caplog):
    with caplog.at_level(logging.DEBUG):
        InfluxSink._on_retry(CONF, "environment watertemperature=21.5", Exception("timeout"))
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert "timeout" in record.getMessage()
