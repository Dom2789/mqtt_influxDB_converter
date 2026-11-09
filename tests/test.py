from mqtt_influx_bridge.routing import tags_from_topic


print(tags_from_topic("temp/techroom/1", {"room": 1, "device": 2}))
print(tags_from_topic("temp/techroom/1", {"room": 1, "missing": 9}))