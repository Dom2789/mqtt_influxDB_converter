from collections.abc import Mapping, Sequence
from .errors import TagIndexError


def tags_from_topic(topic: str, mapping: Mapping[str, int]) -> dict[str, str]:
    tags: dict[str, str] = {}
    levels:Sequence[str] = topic.split("/")
    for key, index in mapping.items():
        if index < 0 or index >= len(levels):
            raise TagIndexError(
                # The three flags are !s (str, the default), !r (repr), !a (ascii — repr with non-ASCII escaped).
                f"tags_from_topic[{key!r}] = {index} is out of range for "
                f"topic {topic!r} ({len(levels)} levels)"
            )
        tags[key] = levels[index]
    return tags