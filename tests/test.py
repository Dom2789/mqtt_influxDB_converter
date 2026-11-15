from mqtt_influx_bridge.config import load_config, build_config

load_config("config.toml", "/Users/dom/temp/influx.env")

print()
load_config("configerror.toml")
print()
#load_config("2.toml")