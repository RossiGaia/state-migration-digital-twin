# for now only mqtt is available
import paho.mqtt.client as mqtt
import yaml
import logging
import time
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# maybe set it as env var
conf_path = "config.yaml"

conf_raw = open(conf_path)
connections_conf = yaml.safe_load(conf_raw)["connections"]
mqtt_conf = connections_conf["mqtt"]
mqtt_broker_url = mqtt_conf["broker_url"]
mqtt_port = mqtt_conf["port"]
mqtt_topics = mqtt_conf["topics"]

buffer_conf = connections_conf["buffer"]


class MqttConnection:

    def __init__(self, recv_buffer):
        self.mqtt_loop_run = True
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        self.recv_buffer = recv_buffer

        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"connected to {mqtt_broker_url} at port {mqtt_port}.")
        for topic in mqtt_topics:
            logger.debug(f"subscribing to {topic}.")
            self.mqtt_client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        data = {
            "topic": msg.topic,
            "payload": json.loads(msg.payload),
            "recv_timestamp": time.time(),
        }
        self.recv_buffer.append(data)

    def run(self):
        logger.debug(f"connecting to {mqtt_broker_url} at {mqtt_port}.")
        self.mqtt_client.connect(mqtt_broker_url, mqtt_port)
        self.mqtt_client.loop_start()

        while self.mqtt_loop_run:
            continue

        logger.debug("mqtt disconnection.")
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        return

    def stop(self):
        self.mqtt_loop_run = False
        logger.debug(f"stop requested. mqtt_loop_run = {self.mqtt_loop_run}")
