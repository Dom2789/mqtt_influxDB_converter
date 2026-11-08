"""paho-mqtt source.

Reconnection is paho's job, not ours: `reconnect_delay_set` gives exponential
backoff and `reconnect_on_failure` (default True) keeps `loop_start`'s network
thread retrying. Subscriptions are issued inside `on_connect`, so they are
renewed automatically after every reconnect.
"""

import logging
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from os import environ

import paho.mqtt.client as mqtt

from ..config import MqttConfig
from ..model import RawMessage

log = logging.getLogger("source.mqtt")

PASSWORD_ENV = "MQTT_PASSWORD"


class MqttSource:
    def __init__(self, cfg: MqttConfig, subscriptions: Sequence[tuple[str, int]]):
        if not subscriptions:
            raise ValueError("MqttSource needs at least one subscription")
        self._cfg = cfg
        self._subscriptions = list(subscriptions)
        self._on_message: Callable[[RawMessage], None] | None = None

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=cfg.client_id,
            protocol=mqtt.MQTTv5,
            transport="tcp",
        )
        if cfg.username:
            self._client.username_pw_set(cfg.username, environ.get(PASSWORD_ENV))
            if PASSWORD_ENV not in environ:
                log.warning(
                    "[mqtt].username is set but %s is not in the environment; "
                    "connecting without a password", PASSWORD_ENV,
                )

        self._client.reconnect_delay_set(
            min_delay=cfg.reconnect_min_delay_s,
            max_delay=cfg.reconnect_max_delay_s,
        )
        self._client.on_connect = self._handle_connect
        self._client.on_disconnect = self._handle_disconnect
        self._client.on_subscribe = self._handle_subscribe
        self._client.on_message = self._handle_message

    # ---- callbacks (network thread) ---------------------------------------

    def _handle_connect(self, client, userdata, connect_flags, reason_code, properties):
        if reason_code.is_failure:
            # paho retries on its own; nothing to do but say why
            log.error("connect refused: %s", reason_code)
            return
        log.info(
            "connected to %s:%d as %r",
            self._cfg.host, self._cfg.port, self._cfg.client_id,
        )
        # resubscribing here is what makes reconnects transparent
        client.subscribe(self._subscriptions)

    def _handle_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties
    ):
        if reason_code is not None and reason_code.is_failure:
            log.warning(
                "disconnected unexpectedly (%s); paho will reconnect with backoff "
                "(%d-%ds)", reason_code,
                self._cfg.reconnect_min_delay_s, self._cfg.reconnect_max_delay_s,
            )
        else:
            log.info("disconnected cleanly")

    def _handle_subscribe(self, client, userdata, mid, reason_code_list, properties):
        for (topic, _), code in zip(self._subscriptions, reason_code_list):
            if code.is_failure:
                log.error("subscribe to %r rejected by broker: %s", topic, code)
            else:
                log.info("subscribed to %r (qos %s)", topic, code.value)

    def _handle_message(self, client, userdata, message):
        """Stamp and hand off. Deliberately the only work done on this thread."""
        if self._on_message is None:
            return
        self._on_message(
            RawMessage(
                topic=message.topic,
                payload=message.payload,
                received_at=datetime.now(timezone.utc),
            )
        )

    # ---- lifecycle --------------------------------------------------------

    def start(self, on_message: Callable[[RawMessage], None]) -> None:
        self._on_message = on_message
        # connect_async + loop_start means a broker that is down at startup is
        # retried rather than fatal
        self._client.connect_async(
            self._cfg.host, self._cfg.port, keepalive=self._cfg.keepalive
        )
        self._client.loop_start()
        log.info(
            "connecting to %s:%d, %d subscription(s)",
            self._cfg.host, self._cfg.port, len(self._subscriptions),
        )

    def stop(self) -> None:
        self._client.disconnect()  # stops ingest before the queue is drained
        self._client.loop_stop()
