"""Entry point: wiring, signals, and the shutdown ordering that matters."""

import argparse
import logging
import signal
import sys
import threading

from . import __version__
from ._lib.ConfigToml import ConfigToml
from ._lib.logger import setup_logging
from .config import ConfigError, InfluxCredentials, load_config
from .pipeline import Pipeline, Router
from .sinks.base import Sink
from .sinks.file import FileSink
from .sinks.stdout import StdoutSink
from .sources.mqtt import MqttSource

log = logging.getLogger("main")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mqtt-influx-bridge",
        description="Subscribe to MQTT topics and write the values into InfluxDB.",
    )
    parser.add_argument(
        "-c", "--config", default="config.toml",
        help="path to config.toml (default: %(default)s)",
    )
    parser.add_argument(
        "-e", "--env-file", default=None,
        help="optional env file with INFLUX_* credentials; the process "
             "environment always takes precedence (systemd uses EnvironmentFile)",
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true",
        help="decode and print readings, write nothing to InfluxDB",
    )
    parser.add_argument(
        "-l", "--log-level", default=None,
        help="override [logging].level (DEBUG, INFO, WARNING, ERROR)",
    )
    parser.add_argument(
        "--print-config", action="store_true",
        help="print the parsed config file and exit",
    )
    parser.add_argument("--version", action="version", version=__version__)
    return parser


def _build_sinks(cfg, args) -> list[Sink]:
    sinks: list[Sink] = []
    if args.dry_run:
        sinks.append(StdoutSink())
        log.warning("--dry-run: readings are printed, nothing is written to InfluxDB")
    else:
        from .sinks.influx import InfluxSink  # imported late so --dry-run needs no creds

        creds = InfluxCredentials.from_env(args.env_file)
        sinks.append(InfluxSink(creds, cfg.influx_write))
    if cfg.file_sink.enabled:
        sinks.append(FileSink(cfg.file_sink))
    return sinks


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.print_config:
        try:
            print(ConfigToml(args.config), end="")
        except OSError as e:
            print(f"cannot read {args.config}: {e}", file=sys.stderr)
            return 2
        return 0

    try:
        cfg = load_config(args.config)
    except ConfigError as e:
        # logging is not configured yet, so go straight to stderr
        print(f"configuration error: {e}", file=sys.stderr)
        return 2

    setup_logging(
        path_logfile=cfg.logging.path,
        name_logfile=cfg.logging.name,
        add_date_to_name=cfg.logging.add_date_to_name,
        target=cfg.logging.target,
        level=args.log_level or cfg.logging.level,
    )
    log.info("mqtt-influx-bridge %s starting", __version__)

    try:
        router = Router.from_specs(cfg.sensors)
        sinks = _build_sinks(cfg, args)
    except ConfigError as e:
        log.error("configuration error: %s", e)
        return 2

    subscriptions = router.subscriptions(cfg.mqtt.qos)
    log.info(
        "%d sensor(s) over %d subscription(s): %s",
        len(router), len(subscriptions), ", ".join(t for t, _ in subscriptions),
    )

    pipeline = Pipeline(router, sinks, maxsize=cfg.queue.maxsize)
    source = MqttSource(cfg.mqtt, subscriptions)

    stop_event = threading.Event()

    def request_stop(signum, _frame):
        # only sets a flag; the real work happens on the main thread below
        log.info("received %s, shutting down", signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    pipeline.start()
    source.start(pipeline.submit)

    try:
        stop_event.wait()
    finally:
        # order matters: stop ingest, then drain the queue, then flush sinks
        source.stop()
        pipeline.stop()
        logging.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
