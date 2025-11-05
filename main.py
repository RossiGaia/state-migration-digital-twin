from connections import MqttConnection
from process import Processing
import threading
import collections
import signal
from flask import Flask, jsonify
import yaml

conf_path = "config.yaml"

confs = yaml.safe_load(open(conf_path))
process_conf = confs["process"]
process_buffer_conf = process_conf["buffer"]["size"]

connection_conf = confs["connections"]
connection_buffer_conf = connection_conf["buffer"]["size"]
connection_mqtt_conf = connection_conf["mqtt"]

state_max_size = confs["state"]["max_size_mb"]


def graceful_shutdown(signum, frame):
    processing.stop()
    processing_t.join()

    mqtt_connection.stop()
    mqtt_t.join()
    exit(0)


signal.signal(signal.SIGINT, graceful_shutdown)

app = Flask(__name__)

processing_buffer = collections.deque(maxlen=process_buffer_conf)
connection_buffer = collections.deque(maxlen=connection_buffer_conf)

mqtt_connection = MqttConnection(
    connection_buffer=connection_buffer, mqtt_conf=connection_mqtt_conf
)
processing = Processing(
    connection_buffer=connection_buffer,
    processing_buffer=processing_buffer,
    state_max_size=state_max_size,
)


mqtt_t = threading.Thread(target=mqtt_connection.run)
mqtt_t.start()

processing_t = threading.Thread(target=processing.run)
processing_t.start()


@app.route("/dt_health")
def health_check():
    return jsonify("OK"), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
