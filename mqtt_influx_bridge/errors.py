class BridgeError(Exception):
    """Base for all expected, per-message failures in the bridge."""
    

class TagIndexError(BridgeError):
    """A [[sensor]] tag index doesn't fit the topic it was applied to."""


class DecodeError(BridgeError):          # NOT ValueError — see Step 02
    def __init__(self, topic: str, payload: bytes, reason: str) -> None:
        self.topic, self.payload, self.reason = topic, payload, reason
        super().__init__(f"{topic}: {reason} (payload={payload[:40]!r})")

class ConfigError(BridgeError):
    """A config file that cannot produce a working bridge."""