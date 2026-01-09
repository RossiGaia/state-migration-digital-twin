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
        self.mqtt_client = None

        self.connection_buffer = connection_buffer

        self.mqtt_broker_url = mqtt_conf["broker_url"]
        self.mqtt_port = mqtt_conf["port"]
        self.mqtt_topics = mqtt_conf["topics"]

    def new_client(self):
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

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
        logger.debug(f"Received message with seq_id: {json.loads(msg.payload)["seq_id"]}")
        self.connection_buffer.append(data)

    def run(self):
        self.new_client()
        logger.debug(f"connecting to {self.mqtt_broker_url} at {self.mqtt_port}.")
        self.mqtt_client.connect(self.mqtt_broker_url, self.mqtt_port)
        self.mqtt_client.loop_start()

        while self.mqtt_loop_run:
            time.sleep(0.05)

        logger.debug("mqtt disconnection.")
        try:
            self.mqtt_client.disconnect()
        finally:
            self.mqtt_client.loop_stop()

        self.mqtt_client = None
        return

    def stop(self):
        self.mqtt_loop_run = False
        logger.debug(f"stop requested. mqtt_loop_run = {self.mqtt_loop_run}")
