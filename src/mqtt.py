import paho.mqtt.client as mqtt
from src.paramters import Parameters
from time import strftime
import json

class MQTT:
    def __init__(self, parameters: Parameters):
        self.file_output_active = parameters.file_output_active
        self.timestamp_active = parameters.timestamp_active
        self.path = parameters.path
        self.file_name = parameters.file_name
        self.broker_IP = parameters.broker_IP
        self.broker_port = parameters.broker_port
        self.topic = parameters.topic


    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc, properties=None):
        print("Connected with result code " + str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(self.topic)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        room, unit, _, tag = msg.topic.split("/")[1:5]

        if tag == "switch:0" or tag == "pm1:0":
            payload = json.loads(message)
            volts = payload.get("voltage")
            amps = payload.get("current")
            power = payload.get("apower")

            output = f"[{msg.topic}][voltage: {volts}, amps: {amps}, power: {power}]"

            if self.timestamp_active:
                timestamp = strftime("%H:%M:%S")
                print(f"[{timestamp}]{output}")
            else:
                print(output)

            if self.file_output_active:
                self.file_output(msg.topic, message)


    # subscribe method
    def subscribe(self):
        client = mqtt.Client(protocol=mqtt.MQTTv5, transport="tcp")
        client.on_connect = self.on_connect
        client.on_message = self.on_message

        client.connect(self.broker_IP, self.broker_port, 60, bind_address="0.0.0.0")

        print(f"subcribed to topic: {self.topic}")

        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        client.loop_forever()

    def file_output(self, topic, message):
        header, tail = self.file_name.split(".")
        name_logfile = f"{header}_{strftime('%Y%m%d')}.{tail}"
        path = self.path + "/" + name_logfile

        with open(path, "a") as file:
            file.write(f"[{topic}] {message}\n")