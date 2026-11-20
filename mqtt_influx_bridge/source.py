"""MQTT in: one paho client, its callbacks, and nothing else.

start() returns immediately; paho's network thread (loop_start) then calls
on_message for every message, which hands a RawMessage to the callback given
to start() — in step 9 that is handle().
"""

import logging
from collections.abc import Callable, Set
from datetime import UTC, datetime
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from .config import MqttConfig
from .model import RawMessage

log = logging.getLogger(__name__)


class MqttSource:
    """Connects to the broker, subscribes, and passes every message on.

    Everything is decided in __init__: the client is built, the three
    callbacks are assigned and the reconnect backoff is set. start() and
    stop() only switch it on and off.
    """

    def __init__(
        self,
        config: MqttConfig,
        subscriptions: Set[str],
        min_delay: int = 1,
        max_delay: int = 120,
    ) -> None:
        self.handler = None
        self.config: MqttConfig = config
        self.subscriptions: frozenset[str] = frozenset(subscriptions)
        self.client: mqtt.Client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(min_delay, max_delay)

    def start(self, on_message: Callable[[RawMessage], None]) -> None:
        """connect_async + loop_start; returns at once, a down broker is retried."""
        self.handler = on_message
        self.client.connect_async(self.config.host, self.config.port)
        self.client.loop_start()

    def stop(self) -> None:
        """loop_stop + disconnect."""
        self.client.disconnect()
        self.client.loop_stop()

    # --- paho callbacks (VERSION2 signatures), run on paho's thread ---------

    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        """Log the reason code; on success subscribe to every filter.

        Subscribing here, not in start(), is what renews the subscriptions
        after every reconnect.
        """
        if reason_code.is_failure:
            log.warning("connect to %s:%s failed: %s", self.config.host, self.config.port, reason_code)
            return
        log.info("connected to %s:%s: %s", self.config.host, self.config.port, reason_code)
        client.subscribe([(topic, 0) for topic in sorted(self.subscriptions)])

    def on_disconnect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        """Log the reason code. paho reconnects by itself."""
        if reason_code.is_failure:
            log.warning("disconnected from %s:%s: %s", self.config.host, self.config.port, reason_code)
        else:
            log.info("disconnected from %s:%s: %s", self.config.host, self.config.port, reason_code)

    def on_message(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        """Two statements: stamp datetime.now(UTC) into a RawMessage, pass it on."""
        raw = RawMessage(message.topic, message.payload, datetime.now(UTC))
        self.handler(raw)
