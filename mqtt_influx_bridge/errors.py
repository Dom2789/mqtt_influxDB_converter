class BridgeError(Exception):
    """Base for all expected, per-message failures in the bridge."""
    pass

class TagIndexError(BridgeError):
    """A [[sensor]] tag index doesn't fit the topic it was applied to."""
    pass