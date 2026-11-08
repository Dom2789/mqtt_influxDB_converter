import pytest

from mqtt_influx_bridge.routing import (
    InvalidTopicFilter,
    max_safe_topic_index,
    tags_from_topic,
    topic_matches,
    validate_topic_filter,
)


@pytest.mark.parametrize(
    "pattern,topic",
    [
        ("temp/techroom/1", "temp/techroom/1"),
        ("temp/+/1", "temp/techroom/1"),
        ("temp/#", "temp/techroom/1"),
        ("temp/#", "temp"),                       # '#' matches the parent level
        ("#", "anything/at/all"),
        ("+/+/+", "a/b/c"),
        ("shellies/+/status/pm1:0", "shellies/abc123/status/pm1:0"),
        ("+", "single"),
    ],
)
def test_matches(pattern, topic):
    assert topic_matches(pattern, topic)


@pytest.mark.parametrize(
    "pattern,topic",
    [
        ("temp/techroom/1", "temp/techroom/2"),
        ("temp/+/1", "temp/techroom/extra/1"),    # '+' is exactly one level
        ("temp/+", "temp/techroom/1"),
        ("temp/techroom/1", "temp/techroom"),     # topic shorter than filter
        ("temp/techroom", "temp/techroom/1"),     # topic longer than filter
        ("#", "$SYS/broker/uptime"),              # wildcards skip $-topics
        ("+/broker", "$SYS/broker"),
        ("shellies/+/status/pm1:0", "shellies/abc/status/switch:0"),
    ],
)
def test_does_not_match(pattern, topic):
    assert not topic_matches(pattern, topic)


@pytest.mark.parametrize("pattern", ["a/#", "#", "a/+/b", "+", "a/b"])
def test_valid_filters(pattern):
    validate_topic_filter(pattern)


@pytest.mark.parametrize(
    "pattern,reason",
    [
        ("", "empty"),
        ("a/#/b", "last level"),
        ("a/b#", "whole level"),
        ("a/+b/c", "whole level"),
    ],
)
def test_invalid_filters(pattern, reason):
    with pytest.raises(InvalidTopicFilter) as e:
        validate_topic_filter(pattern)
    assert reason in str(e.value)


def test_tags_from_topic():
    tags = tags_from_topic("shellies/abc123/status/pm1:0", {"device_id": 1})
    assert tags == {"device_id": "abc123"}


def test_tags_from_topic_multiple_levels():
    tags = tags_from_topic("a/b/c", {"first": 0, "last": 2})
    assert tags == {"first": "a", "last": "c"}


def test_tags_from_topic_out_of_range():
    with pytest.raises(IndexError) as e:
        tags_from_topic("a/b", {"nope": 7})
    assert "out of range" in str(e.value)
    assert "nope" in str(e.value)


def test_tags_from_topic_empty_spec():
    assert tags_from_topic("a/b/c", {}) == {}


@pytest.mark.parametrize(
    "pattern,expected",
    [("a/b/c", 2), ("a/+/c", 2), ("a", 0), ("a/#", None), ("#", None)],
)
def test_max_safe_topic_index(pattern, expected):
    assert max_safe_topic_index(pattern) == expected
