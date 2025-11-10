import paho.mqtt.client as mqtt
import logging
import time
import json
import random
import string

logging.basicConfig(
    format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
    handlers=[logging.FileHandler("dt_connections.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class MqttConnection:

    def __init__(self, *, connection_buffer, mqtt_conf):
        self.mqtt_loop_run = True
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        self.connection_buffer = connection_buffer

        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_broker_url = mqtt_conf["broker_url"]
        self.mqtt_port = mqtt_conf["port"]
        self.mqtt_topics = mqtt_conf["topics"]

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"connected to {self.mqtt_broker_url} at port {self.mqtt_port}.")
        for topic in self.mqtt_topics:
            logger.debug(f"subscribing to {topic}.")
            self.mqtt_client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        data = {
            "topic": msg.topic,
            "payload": json.loads(msg.payload),
            "recv_timestamp": time.time(),
            "key": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        }
        self.connection_buffer.append(data)

    def run(self):
        logger.debug(f"connecting to {self.mqtt_broker_url} at {self.mqtt_port}.")
        self.mqtt_client.connect(self.mqtt_broker_url, self.mqtt_port)
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
