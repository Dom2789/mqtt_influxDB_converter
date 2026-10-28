from src.mqtt import MQTT
from src.paramters import Parameters
from src.Influx_parameters import Influx_parameters

def main():
    # default values
    parameters = Parameters(False, False, "/Users/dom/temp/", "logfile.txt", "192.168.1.225", 1883, "temp/techroom/1")
    influx = Influx_parameters().read_env_file("/Users/dom_mini/temp/influx.env")

    # instantiation of MQTT-class and subscribe to topic
    mqtt = MQTT(parameters, influx)
    mqtt.subscribe()

if __name__ == "__main__":
    main()
