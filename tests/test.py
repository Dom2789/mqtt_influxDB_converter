from mqtt_influx_bridge.decoders import decode_json

from mqtt_influx_bridge.model import RawMessage, Reading, SensorSpec

from datetime import UTC, datetime

AT = datetime(2026, 11, 8, 10, 9, tzinfo=UTC)
def msg(topic: str, payload: bytes) -> RawMessage:
    return RawMessage(topic, payload, AT)

WATER = SensorSpec(
    name="water temperature techroom",
    topic="temp/techroom/1",
    measurement="environment",
    cast="float",
    fields={"watertemperature": "temp"},
    tags={"room": "techroom"},
)

TECH = msg(
    "temp/techroom/1",
    b'{"temp":39.12}',
)


PLUG_PM = msg(
    "powermeter/serverraum/rack/status/pm1:0",
    b'{"id":0, "voltage":225.1, "current":0.194, "apower":21.7 ,"freq":49.9,"aenergy":{"total":40503.682,"by_minute":[210.574,210.574,421.148],"minute_ts":1794647340},"ret_aenergy":{"total":0.000,"by_minute":[0.000,0.000,0.000],"minute_ts":1794647340}}',
)

SHELLY = SensorSpec(
    name="shelly powermeter fleet",
    topic="powermeter/+/+/status/+",
    measurement="power",
    cast="float",
    fields={
        "voltage": "voltage",
        "current": "current",
        "power": "apower",
        "freq": "freq",
        "energy": "aenergy.total",
        "pf": "pf",
        "temperature": "temperature.tC",
        "on_time": "counts.on_time",
        "output": "output",
        "brightness": "brightness",
    },
    tags_from_topic={"room": 1, "device": 2, "channel": 4},
)

#decode_json(TECH, WATER)
decode_json(PLUG_PM, SHELLY)

def spec(fields: dict[str, str], cast: str = "float") -> SensorSpec:
    return SensorSpec("test", "t/#", "m", fields, cast)

decode_json(PLUG_PM, spec({"bm": "aenergy.by_minute"}))