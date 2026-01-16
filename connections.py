import paho.mqtt.client as mqtt
import logging
import time
import json
import random
import string
import pymongo
import threading
import collections
import sys

logger = logging.getLogger(__name__)


class MqttConnection:

    def __init__(
        self,
        *,
        connection_buffer,
        mqtt_conf,
        use_mongo=False,
        mongo_url=None,
        mongo_db=None,
        mongo_collection=None,
    ):
        self.mqtt_loop_run = True
        self.mqtt_client = None

        self.connection_buffer = connection_buffer

        self.mqtt_broker_url = mqtt_conf["broker_url"]
        self.mqtt_port = mqtt_conf["port"]
        self.mqtt_topics = mqtt_conf["topics"]
        self.use_mongo = use_mongo
        self.mongo_url = mongo_url
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        if self.use_mongo:
            try:
                self.mongo_client = pymongo.MongoClient(self.mongo_url)
            except:
                logger.error("Could not connect to mongo.")
                sys.exit(1)
            self.mongo_deque = collections.deque()
            self.mongo_t = threading.Thread(target=self.mongo_writer)

    def new_client(self):
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def mongo_writer(self):
        while self.mqtt_loop_run:
            if len(self.mongo_deque) > 0:
                data = self.mongo_deque.popleft()
                self.mongo_client[self.mongo_db][self.mongo_collection].insert_one(data)
        self.mongo_client.close()
        return

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"connected to {self.mqtt_broker_url} at port {self.mqtt_port}.")
        for topic in self.mqtt_topics:
            logger.debug(f"subscribing to {topic}.")
            self.mqtt_client.subscribe(topic)

        if self.use_mongo:
            logger.debug("Starting also mongodb writer.")
            self.mongo_t.start()

    def on_message(self, client, userdata, msg):
        data = {
            "topic": msg.topic,
            "payload": json.loads(msg.payload.decode("UTF-8")),
            "recv_timestamp": time.time(),
            "key": "".join(
                random.choices(string.ascii_uppercase + string.digits, k=10)
            ),
        }
        logger.debug(f"Received message with seq_id: {data['payload']['seq_id']}")
        self.connection_buffer.append(data)
        if self.use_mongo:
            self.mongo_deque.append(data)

    def run(self):
        self.new_client()
        logger.info(f"connecting to {self.mqtt_broker_url} at {self.mqtt_port}.")
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

        if self.use_mongo:
            self.mongo_t.join()

        return

    def stop(self):
        self.mqtt_loop_run = False
        logger.info(f"stop requested. mqtt_loop_run = {self.mqtt_loop_run}")
