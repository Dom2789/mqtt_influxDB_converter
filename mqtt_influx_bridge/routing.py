"""MQTT topic-filter matching and topic-level tag extraction.

`+` matches exactly one level, `#` matches the remaining levels and is only legal
as the final level. This is what lets a single `[[sensor]]` block cover a whole
fleet of identical devices.
"""

from collections.abc import Mapping, Sequence


class InvalidTopicFilter(ValueError):
    pass


def validate_topic_filter(pattern: str) -> None:
    """Raise InvalidTopicFilter if `pattern` is not a legal MQTT subscription."""
    if pattern == "":
        raise InvalidTopicFilter("topic filter must not be empty")
    levels = pattern.split("/")
    for i, level in enumerate(levels):
        if "#" in level:
            if level != "#":
                raise InvalidTopicFilter(
                    f"{pattern!r}: '#' must occupy a whole level, got {level!r}"
                )
            if i != len(levels) - 1:
                raise InvalidTopicFilter(
                    f"{pattern!r}: '#' is only allowed as the last level"
                )
        if "+" in level and level != "+":
            raise InvalidTopicFilter(
                f"{pattern!r}: '+' must occupy a whole level, got {level!r}"
            )


def topic_matches(pattern: str, topic: str) -> bool:
    """True if `topic` matches the MQTT subscription filter `pattern`."""
    p_levels = pattern.split("/")
    t_levels = topic.split("/")

    for i, p in enumerate(p_levels):
        if p == "#":
            # '#' matches the parent level too, but not a leading $-topic
            if i == 0 and t_levels and t_levels[0].startswith("$"):
                return False
            return True
        if i >= len(t_levels):
            return False
        if p == "+":
            if i == 0 and t_levels[0].startswith("$"):
                return False
            continue
        if p != t_levels[i]:
            return False

    return len(p_levels) == len(t_levels)


def tags_from_topic(
    topic: str, spec: Mapping[str, int]
) -> dict[str, str]:
    """Extract tags from topic levels: {"device_id": 1} -> level 1 of the topic.

    Raises IndexError if a configured level is out of range for this topic, which
    the pipeline turns into a logged, counted, per-message error.
    """
    levels: Sequence[str] = topic.split("/")
    out: dict[str, str] = {}
    for tag_name, index in spec.items():
        if index < 0 or index >= len(levels):
            raise IndexError(
                f"tags_from_topic[{tag_name!r}] = {index} is out of range for "
                f"topic {topic!r} ({len(levels)} levels)"
            )
        out[tag_name] = levels[index]
    return out


def max_safe_topic_index(pattern: str) -> int | None:
    """Highest topic level index guaranteed to exist for every match of `pattern`.

    None when the pattern ends in '#', since the topic length is then unbounded
    below as well as above and only a runtime check can decide.
    """
    levels = pattern.split("/")
    if levels[-1] == "#":
        return None
    return len(levels) - 1
