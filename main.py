from src.mqtt import MQTT
from src.paramters import Parameters
from src.Influx_parameters import InfluxParameters

def main():
    # default values
    parameters = Parameters(False, False, "/Users/dom/temp/", "logfile.txt", "192.168.1.225", 1883, "temp/techroom/1")
    influx = InfluxParameters.from_env_file("/Users/dom/temp/influx.env")

    # instantiation of MQTT-class and subscribe to topic
    mqtt = MQTT(parameters, influx)
    mqtt.subscribe()

if __name__ == "__main__":
    main()
