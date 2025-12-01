from connections import MqttConnection
from process import Processing
import threading
import collections
import signal
from flask import Flask, jsonify, request
import yaml
import logging
import time
import requests
import json
import os

logging.basicConfig(
    format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
    handlers=[logging.FileHandler("dt_main.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

conf_path = "config.yaml"

confs = yaml.safe_load(open(conf_path))
process_conf = confs["process"]
process_buffer_conf = process_conf["buffer"]["size"]
process_burn_worker = process_conf["burn"]["workers"]
process_burn_work = process_conf["burn"]["work"]

connection_conf = confs["connections"]
connection_buffer_conf = connection_conf["buffer"]["size"]
connection_mqtt_conf = connection_conf["mqtt"]

state_max_size = confs["state"]["max_size_mb"]

# check if migrated dt
source_dt_url = os.environ.get("SOURCE_DT_URL")

metrics_file_name = f"{confs['name']}_{confs['metrics']['file_name']}"
with open(metrics_file_name, "a") as metrics_file:
    metrics_file.write("Log started.\n")

def graceful_shutdown(signum, frame):
    processing.stop()
    processing_t.join()

    mqtt_connection.stop()
    mqtt_t.join()
    with open(metrics_file_name, "a") as metrics_file:
        metrics_file.write(
            f"Stopped serving at {time.time()}.\n"
        )
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
    worker=process_burn_worker,
    work=process_burn_work
)

mqtt_t = threading.Thread(target=mqtt_connection.run)
mqtt_t.start()

processing_t = threading.Thread(target=processing.run)
processing_t.start()

# recover state
if source_dt_url is not None:
    restore_start_timestamp = time.time()
    try:
        response = requests.get(source_dt_url)
        serialized_state = json.loads(response.text)["state"]
    except:
        logger.error("Error in the state request.")
        

    result = processing.deserialize_state(serialized_state)
    restore_end_timestamp = time.time()
    
    if result == 0:
        with open(metrics_file_name, "a") as metrics_file:
            metrics_file.write(
                f"Restoring total time: {restore_end_timestamp - restore_start_timestamp}. Started at {restore_start_timestamp}, ended at {restore_end_timestamp}.\n"
            )
    else:
        logger.error("Error in the state deserialization.")

@app.route("/state", methods=["GET"])
def get_state():
    dump_start_timestamp = time.time()
    state = processing.serialize_state()
    dump_end_timestamp = time.time()
    with open(metrics_file_name, "a") as metrics_file:
        metrics_file.write(
            f"Dumping total time: {dump_end_timestamp - dump_start_timestamp}. Started at {dump_start_timestamp}, ended at {dump_end_timestamp}.\n"
        )
    return jsonify({"state": state})


@app.route("/state", methods=["POST"])
def set_state():
    restore_start_timestamp = time.time()
    serialized_state = request.get_json()["state"]
    result = processing.deserialize_state(serialized_state)
    restore_end_timestamp = time.time()
    
    with open(metrics_file_name, "a") as metrics_file:
        metrics_file.write(
            f"Restoring total time: {restore_end_timestamp - restore_start_timestamp}. Started at {restore_start_timestamp}, ended at {restore_end_timestamp}.\n"
        )
    if result == 0:
        return jsonify({"status": "success"})
    else:
        return jsonify({"status": "error"})

@app.route("/healthd")
def health_check():
    return jsonify({"status": "healthy"}), 200


@app.route("/odte")
def odte():
    odte = processing.get_odte()
    return jsonify({"odte": odte})


@app.route("/delta")
def get_delta():
    delta = processing.get_delta()
    return jsonify(delta)

@app.route("/delta", methods=["POST"])
def process_delta():
    different_items = request.get_json()
    try:
        processing.process_delta(different_items)
        return jsonify({"message": "Success."}), 201
    except Exception:
        return jsonify({"message": "Exception in processing delta."}), 500

if __name__ == "__main__":
    logger.debug("Started main.")

    with open(metrics_file_name, "a") as metrics_file:
        metrics_file.write(
            f"Starting service at {time.time()}.\n"
        )

    app.run(host="0.0.0.0", port=5003)
