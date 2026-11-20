"""MQTT in: one paho client, its callbacks, and nothing else.

start() returns immediately; paho's network thread (loop_start) then calls
on_message for every message, which hands a RawMessage to the callback given
to start() — in step 9 that is handle().
"""

import logging
from collections.abc import Callable, Set
from typing import Any

import paho.mqtt.client as mqtt

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
        self.config: MqttConfig
        self.subscriptions: frozenset[str]
        self.client: mqtt.Client  # CallbackAPIVersion.VERSION2

    def start(self, on_message: Callable[[RawMessage], None]) -> None:
        """connect_async + loop_start; returns at once, a down broker is retried."""
        ...

    def stop(self) -> None:
        """loop_stop + disconnect."""
        ...

    # --- paho callbacks (VERSION2 signatures), run on paho's thread ---------

    def on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        """Log the reason code; on success subscribe to every filter.

        Subscribing here, not in start(), is what renews the subscriptions
        after every reconnect.
        """
        ...

    def on_disconnect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        """Log the reason code. paho reconnects by itself."""
        ...

    def on_message(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        """Two statements: stamp datetime.now(UTC) into a RawMessage, pass it on."""
        ...
