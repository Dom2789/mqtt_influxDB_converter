"""Spec for mqtt_influx_bridge.routing — implementation plan steps 3 and 4.

Written before routing.py exists, so the first run fails at import. That is
the intended starting state: make these pass by writing topic_matches() and
tags_from_topic(). Don't edit the table to match an implementation.

Wildcard rules follow the MQTT v5 spec, section 4.7 (Topic Names and Filters).
"""

import pytest

from mqtt_influx_bridge.errors import TagIndexError
from mqtt_influx_bridge.routing import tags_from_topic, topic_matches

# What tags_from_topic raises when a tag points at a topic level that isn't
# there. Step 07 catches this per message and drops that one reading. It must
# NOT be IndexError: see Step 02 on why the handler catches BridgeError only.
OUT_OF_RANGE = TagIndexError


# --------------------------------------------------------------------------
# topic_matches
# --------------------------------------------------------------------------

MATCH_CASES = [
    # --- exact names, no wildcards ---
    ("temp/techroom/1", "temp/techroom/1", True),
    ("temp/techroom/1", "temp/techroom/2", False),
    ("temp/techroom/1", "TEMP/techroom/1", False),        # topics are case sensitive
    ("temp/techroom", "temp/techroom/1", False),          # filter shorter than topic
    ("temp/techroom/1/extra", "temp/techroom/1", False),  # filter longer than topic

    # --- '+' consumes exactly one level ---
    ("temp/+/1", "temp/techroom/1", True),
    ("+/techroom/1", "temp/techroom/1", True),
    ("+/+/+", "temp/techroom/1", True),
    ("temp/+", "temp/techroom", True),
    ("temp/+", "temp/techroom/1", False),                 # one level, not two
    ("temp/+", "temp", False),                            # '+' needs a level to consume
    ("temp/+", "temp/", True),                            # ...and an empty level is one
    ("+", "temp", True),
    ("+", "temp/techroom", False),

    # --- '#' consumes every remaining level, including none ---
    ("temp/#", "temp/techroom/1", True),
    ("temp/#", "temp/techroom", True),
    ("temp/#", "temp", True),                             # '#' includes its parent level
    ("temp/#", "temp/", True),
    ("#", "temp/techroom/1", True),
    ("temp/#", "temperature/1", False),                   # prefix must match a whole level

    # --- a filter STARTING with a wildcard never matches a '$' topic ---
    ("#", "$SYS/broker/load", False),
    ("+/broker/load", "$SYS/broker/load", False),
    # ...but an explicit '$' prefix matches fine. An implementation that just
    # rejects every topic beginning with '$' passes the two above and fails
    # these two.
    ("$SYS/#", "$SYS/broker/load", True),
    ("$SYS/+/load", "$SYS/broker/load", True),

    # --- the overlap case Step 12 depends on: this filter and the exact
    # "temp/techroom/1" above must both match the same topic, which is why
    # Router.match() returns a list rather than the first hit. ---
    ("temp/+/+", "temp/techroom/1", True),
]


@pytest.mark.parametrize(
    "pattern,topic,expected",
    MATCH_CASES,
    ids=[f"{p}--vs--{t}" for p, t, _ in MATCH_CASES],
)
def test_topic_matches(pattern, topic, expected):
    # 'is' rather than '==' pins the declared -> bool signature: a truthy
    # match object or an int would pass '==' and fail here.
    assert topic_matches(pattern, topic) is expected


# --------------------------------------------------------------------------
# tags_from_topic
# --------------------------------------------------------------------------

def test_extracts_tag_values_from_levels():
    assert tags_from_topic("temp/techroom/1", {"room": 1, "device": 2}) == {
        "room": "techroom",
        "device": "1",
    }


def test_level_zero_is_addressable():
    assert tags_from_topic("temp/techroom/1", {"kind": 0}) == {"kind": "temp"}


def test_empty_mapping_yields_no_tags():
    assert tags_from_topic("temp/techroom/1", {}) == {}


def test_values_are_always_strings():
    # Influx tag values are strings by definition, so a numeric-looking level
    # must not leak through as an int.
    tags = tags_from_topic("temp/techroom/1", {"device": 2})
    assert all(isinstance(v, str) for v in tags.values())


def test_returns_a_plain_dict():
    # Step 12 merges this with the static tags from config, so it needs to be
    # an ordinary dict, not a Mapping view.
    assert type(tags_from_topic("temp/techroom/1", {"room": 1})) is dict


def test_out_of_range_index_raises():
    with pytest.raises(OUT_OF_RANGE):
        tags_from_topic("temp/techroom/1", {"room": 7})


def test_out_of_range_does_not_return_partial_tags():
    # One valid index, one past the end. No half-tagged result may escape,
    # or a reading lands in Influx with an incomplete identity.
    with pytest.raises(OUT_OF_RANGE):
        tags_from_topic("temp/techroom/1", {"room": 1, "missing": 9})


def test_wildcard_tail_is_only_checkable_at_runtime():
    # A pattern ending in '#' makes the topic length unbounded, so config
    # validation in Step 8 cannot bounds-check an index against it. A short
    # topic arriving under 'temp/#' therefore has to fail here, not at startup.
    with pytest.raises(OUT_OF_RANGE):
        tags_from_topic("temp", {"device": 2})
