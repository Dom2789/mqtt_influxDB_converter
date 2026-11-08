from mqtt_influx_bridge.routing import topic_matches


case = ("temp/techroom/1", "temp/techroom/1", True)

print(topic_matches(case[0], case[1]))