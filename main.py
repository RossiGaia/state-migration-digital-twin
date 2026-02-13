from connections import MqttConnection
from process import Processing
import threading
import collections
import signal
from flask import Flask, jsonify, request, Response
import yaml
import logging
import time
import requests
import json
import os
from logging.handlers import RotatingFileHandler
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST

# metrics
odte = Gauge(
    "odte",
    "Overall Digital Twin Entanglement."
)

mqtt_active_ts = Gauge(
    "mqtt_active_ts",
    "Timestamp for MQTT connection. 0 if not set."
)

mqtt_inactive_ts = Gauge(
    "mqtt_inactive_ts",
    "Timestamp for MQTT connection. 0 if not set."
)

processing_active_ts = Gauge(
    "processing_active_ts",
    "Timestamp processing module started. 0 if not set."
)

serializing_time = Gauge(
    "serializing_time",
    "Time required to perform serialization. 0 if not set."
)

deserializing_time = Gauge(
    "deserializing_time",
    "Time required to perform deserialization. 0 if not set."
)

live_migration_restore_time = Gauge(
    "live_migration_restore_time",
    "Time required to perform live migration. 0 if not set."
)

rebuild_duration_time = Gauge(
    "rebuild_duration_time",
    "Time to complete the rebuild phase. 0 if not set."
)

mqtt_active_ts.set(0)
mqtt_inactive_ts.set(0)
processing_active_ts.set(0)
serializing_time.set(0)
deserializing_time.set(0)
live_migration_restore_time.set(0)
rebuild_duration_time.set(0)

# for live migration
migration_done = None

# conf_path = "/app/dt/configs/config.yaml"
conf_path = "./config.yaml"
confs = yaml.safe_load(open(conf_path))
dt_name = confs["name"]
flask_port = int(confs["flask"]["port"])
process_conf = confs["process"]
process_buffer_conf = process_conf["buffer"]["size"]
process_burn_worker = process_conf["burn"]["workers"]
process_burn_work = process_conf["burn"]["work"]
process_do_periodic_checkpoints = process_conf["file_checkpoints"]["use"]
process_periodic_checkpoints_interval = None
process_periodic_checkpoints_file = None

if process_do_periodic_checkpoints:
    process_periodic_checkpoints_interval = process_conf["file_checkpoints"]["interval"]
    process_periodic_checkpoints_file = process_conf["file_checkpoints"]["path"]

connection_conf = confs["connections"]
connection_buffer_conf = connection_conf["buffer"]["size"]
connection_mqtt_conf = connection_conf["mqtt"]
connection_mongo_conf = connection_conf["mongodb"]
connection_use_mongo = connection_mongo_conf["use"]
connection_mongo_url = None
connection_mongo_db = None
connection_mongo_collection = None

if connection_use_mongo:
    connection_mongo_url = connection_mongo_conf["url"]
    connection_mongo_db = connection_mongo_conf["database"]
    connection_mongo_collection = connection_mongo_conf["collection"]

state_max_size = confs["state"]["max_size_mb"]

logger_conf = confs["logger"]
level_conf = logger_conf["level"]
shell_level = level_conf["shell"]
file_level = level_conf["file"]
mongo_level = level_conf["mongo"]

def setup_logging():
    fmt = logging.Formatter(
        fmt="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(getattr(logging, shell_level.upper(), logging.INFO))
    root.addHandler(sh)

    def file_handler(path: str, level=logging.INFO):
        fh = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=5)
        fh.setFormatter(fmt)
        fh.setLevel(level)
        return fh

    process_fh = file_handler(f"dt_process_{dt_name}.log", getattr(logging, file_level.upper(), logging.INFO))
    connections_fh = file_handler(f"dt_connections_{dt_name}.log", getattr(logging, file_level.upper(), logging.INFO))
    main_fh = file_handler(f"dt_main_{dt_name}.log", getattr(logging, file_level.upper(), logging.INFO))

    log_process = logging.getLogger("process")
    log_connections = logging.getLogger("connections")
    log_main = logging.getLogger("__main__")

    log_process.addHandler(process_fh)
    log_connections.addHandler(connections_fh)
    log_main.addHandler(main_fh)

    log_process.propagate = True
    log_connections.propagate = True
    log_main.propagate = True

    logging.getLogger("pymongo").setLevel(getattr(logging, mongo_level.upper(), logging.INFO))


setup_logging()
logger = logging.getLogger(__name__)


def graceful_shutdown(signum, frame):
    processing.stop()
    try:
        processing_t.join()
    except:
        pass

    mqtt_connection.stop()
    try:
        mqtt_t.join()
    except:
        pass

    logger.info(f"Processing stopped at: {time.time()}")
    exit(0)


signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)


app = Flask(__name__)

processing_buffer = collections.deque(maxlen=process_buffer_conf)
connection_buffer = collections.deque(maxlen=connection_buffer_conf)

mqtt_connection = MqttConnection(
    connection_buffer=connection_buffer,
    mqtt_conf=connection_mqtt_conf,
    use_mongo=connection_use_mongo,
    mongo_url=connection_mongo_url,
    mongo_db=connection_mongo_db,
    mongo_collection=connection_mongo_collection,
)
processing = Processing(
    connection_buffer=connection_buffer,
    processing_buffer=processing_buffer,
    state_max_size=state_max_size,
    worker=process_burn_worker,
    work=process_burn_work,
    use_mongo=connection_use_mongo,
    mongo_url=connection_mongo_url,
    mongo_db=connection_mongo_db,
    mongo_collection=connection_mongo_collection,
    do_periodic_dumps=process_do_periodic_checkpoints,
    periodic_dumps_interval=process_periodic_checkpoints_interval,
    periodic_dumps_file_path=process_periodic_checkpoints_file
)

# check if migrated dt
# live replication
source_dt_url_delta = os.environ.get("SOURCE_DT_URL_DELTA")
source_dt_url_disconnect = os.environ.get("SOURCE_DT_URL_DISCONNECT")

if source_dt_url_delta:
    migration_done = False
    restore_start_timestamp = time.time()
    # start requesting delta
    logger.info(f"DT is migrated. Starting live migration.")
    max_rounds = int(confs["migration"]["live"]["rounds"])
    for i in range(max_rounds - 1):
        resp = requests.get(source_dt_url_delta)
        obj = json.loads(resp.text)
        processing.process_delta(obj)
        time.sleep(1)

    # call disconnect on old dt
    logger.debug("Disconnecting source DT.")
    resp = requests.post(source_dt_url_disconnect)
    if resp.status_code != 200:
        logger.error("Error in disconnecting the source DT")
        exit(1)

    # call last delta
    logger.debug("Calling last delta.")
    resp = requests.get(source_dt_url_delta)
    obj = json.loads(resp.text)
    processing.process_delta(obj)

    restore_end_timestamp = time.time()
    live_migration_total_time = restore_end_timestamp - restore_start_timestamp
    logger.info(f"Restoring total time: {live_migration_total_time}. Started at: {restore_start_timestamp}, ended at: {restore_end_timestamp}.")
    live_migration_restore_time.set(live_migration_total_time)
    # with open("test_file_dest.json", "x") as file:
    #     file.writelines(json.dumps(processing.serialize_state()))

startup_mqtt_connection = int(os.environ.get("STARTUP_MQTT_CONNECTION", 1))

# storage rebinding
# serialization file the same of config file
serialize_from_file = bool(os.environ.get("DT_SERIALIZE_FROM_FILE", False))
if serialize_from_file:
    start = time.time()
    with open(process_periodic_checkpoints_file, "r") as file:
        lines = file.readlines()

    serialized_state = json.loads(''.join(lines))
    processing.deserialize_state(serialized_state)
    total = time.time() - start
    deserializing_time.set(total)


@app.route("/rebuild", methods=["POST"])
def rebuild():
    global migration_done
    migration_done = False
    rebuild_start_time = time.time()
    try:
        processing.rebuild()
    except:
        return jsonify({"message": "Error in rebuild process."}), 500

    rebuild_total_time = time.time() - rebuild_start_time
    rebuild_duration_time.set(rebuild_total_time)
    migration_done = True
    return jsonify({"message": f"Rebuild success. Total time: {rebuild_total_time}"})


@app.route("/state", methods=["GET"])
def get_state():
    dump_start_timestamp = time.time()
    state = processing.serialize_state()
    dump_end_timestamp = time.time()
    logger.info(f"Dumping total time: {dump_end_timestamp - dump_start_timestamp}. Started at {dump_start_timestamp}, ended at {dump_end_timestamp}.")
    return jsonify({"state": state})


@app.route("/state", methods=["POST"])
def set_state():
    restore_start_timestamp = time.time()
    serialized_state = request.get_json()["state"]
    result = processing.deserialize_state(serialized_state)
    restore_end_timestamp = time.time()
    logger.info(
            f"Restoring total time: {restore_end_timestamp - restore_start_timestamp}. Started at: {restore_start_timestamp}, ended at: {restore_end_timestamp}.\n"
        )
    if result == 0:
        return jsonify({"status": "success"})
    else:
        return jsonify({"status": "error"})


@app.route("/disconnect", methods=["POST"])
def disconnect():
    disconnection_time = time.time()
    logger.info(f"Disconnecting from mqtt. Time: {disconnection_time}")
    mqtt_inactive_ts.set(disconnection_time)
    # with open("test_file_source.json", "x") as file:
    #     file.writelines(json.dumps(processing.serialize_state()))
    try:
        mqtt_connection.stop()
        mqtt_t.join()
    except:
        return jsonify({"status": "error"})

    return jsonify({"status": "success"})


@app.route("/reconnect", methods=["POST"])
def reconnect():
    global mqtt_t
    try:
        mqtt_connection.mqtt_loop_run = True
        mqtt_t = threading.Thread(target=mqtt_connection.run)
        mqtt_t.start()
        reconnection_time = time.time()
        mqtt_active_ts.set(reconnection_time)
        logger.info(f"Reconnecting to mqtt. Time: {reconnection_time}")
    except:
        return jsonify({"status": "error"})

    return jsonify({"status": "success"})


@app.route("/healthd")
def health_check():
    return jsonify({"status": "healthy"}), 200


@app.route("/odte")
def odte():
    body = f"odte {float(processing.get_odte())}"
    return Response(
        body,
        mimetype="text/plain; version=0.0.4; charset=utf-8"
    )


@app.route("/metrics")
def get_metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

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

@app.route("/migration_status", methods = ["GET"])
def get_migration_status():
    global migration_done
    return jsonify({"status": migration_done})

if __name__ == "__main__":
    logger.debug("Started main.")

    if startup_mqtt_connection != 0:
        mqtt_t = threading.Thread(target=mqtt_connection.run)
        mqtt_t.start()
        mqtt_connected_ts = time.time()
        mqtt_active_ts.set(mqtt_connected_ts)
        logger.info(f"Connected to mqtt. Time: {mqtt_connected_ts}")
        migration_done = True

    processing_t = threading.Thread(target=processing.run)
    processing_t.start()
    processing_start_ts = time.time()
    processing_active_ts.set(processing_start_ts)

    logger.info(f"Processing started at: {processing_start_ts}")

    app.run(host="0.0.0.0", port=flask_port)
