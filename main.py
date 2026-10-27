from src.mqtt import MQTT
from src.paramters import Parameters

def main():
    # default values
    parameters = Parameters(False, True, "/Users/dom/temp/", "logfile.txt", "192.168.1.225", 1883, "powermeter/+/+/status/+")

    # instantiation of MQTT-class and subscribe to topic
    mqtt = MQTT(parameters)
    mqtt.subscribe()


if __name__ == "__main__":
    main()
