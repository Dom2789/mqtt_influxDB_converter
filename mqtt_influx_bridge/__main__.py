"""Entry point: wire config, sinks, router and source, then wait for a signal.

The main thread only waits: every message is handled on paho's network
thread. Once an hour it logs that hour's counts; on SIGTERM or SIGINT it
stops the source, flushes the sinks and logs the totals.
"""

import argparse
import logging
import signal
import sys
import threading
from dataclasses import replace

from .bridge import Counters, Router, handle
from .config import load_config
from .errors import ConfigError
from .sinks import InfluxSink, StdoutSink
from .source import MqttSource

REPORT_INTERVAL_S = 3600

log = logging.getLogger("mqtt_influx_bridge")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="mqtt-influx-bridge", description="MQTT JSON -> InfluxDB")
    p.add_argument("--config", default="config.toml", help="path to config.toml (default: %(default)s)")
    p.add_argument("--dry-run", action="store_true", help="print readings instead of writing to InfluxDB")
    p.add_argument("--debug", action="store_true", help="log at DEBUG, overriding log_level from the config")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # stdout, so journald captures it; --debug is in effect before the config loads
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    try:
        config = load_config(args.config)
    except ConfigError as e:
        log.error("config: %s", e)
        return 1
    if not args.debug:
        logging.getLogger().setLevel(config.log_level)

    # bottom-up: sinks, router, source
    sinks = [StdoutSink() if args.dry_run else InfluxSink(config.influx)]
    router = Router(config.sensors)
    counters = Counters()
    source = MqttSource(config.mqtt, router.subscriptions())

    # handlers only set the event; the main thread does the work
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda signum, frame: stop.set())
    signal.signal(signal.SIGINT, lambda signum, frame: stop.set())

    source.start(lambda msg: handle(msg, router, sinks, counters))
    log.info("running: %d sensor(s), %s", len(config.sensors), "dry run" if args.dry_run else "writing to InfluxDB")

    try:
        previous = replace(counters)
        while not stop.wait(REPORT_INTERVAL_S):
            log.info("last hour: %s", counters.since(previous))
            previous = replace(counters)
    finally:
        source.stop()
        for sink in sinks:
            sink.close()
        log.info("stopped, totals: %s", counters)

    return 0


if __name__ == "__main__":
    sys.exit(main())
