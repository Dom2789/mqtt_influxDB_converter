from mqtt_influx_bridge.model import SensorSpec, Reading
from mqtt_influx_bridge.bridge import Router, with_tags
from datetime import UTC, datetime

AT = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)
def spec(**overrides) -> SensorSpec:
    s = dict(
        name="probe",
        topic="temp/+/+",
        measurement="environment",
        fields={"watertemperature": "temp"},
    ) | overrides
    return SensorSpec(**s)

broad = spec(name="broad", topic="temp/#")
specific = spec(name="specific", topic="temp/techroom/1")
other = spec(name="other", topic="power/#")



def plain_reading() -> Reading:
    return Reading("environment", {"watertemperature": 21.5}, {}, AT)

print(plain_reading())
r = with_tags(plain_reading(), spec(tags_from_topic={"room": 1, "device": 2}), "temp/techroom/1")
r = with_tags(plain_reading(), spec(tags={"site": "home"}), "temp/techroom/1")
print(r)