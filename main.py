from connections import MqttConnection
from process import BaseProcessing
from migrate_replication import MigratePatternReplication
import threading
import collections
import signal
from flask import Flask


def graceful_shutdown(signum, frame):
    processing.stop()
    processing_t.join()

    mqtt_connection.stop()
    mqtt_t.join()
    exit(0)


signal.signal(signal.SIGINT, graceful_shutdown)

app = Flask(__name__)

recv_buffer = collections.deque(maxlen=100)

mqtt_connection = MqttConnection(recv_buffer)
processing = BaseProcessing(recv_buffer)

migrate_pattern = MigratePatternReplication(processing)


mqtt_t = threading.Thread(target=mqtt_connection.run)
mqtt_t.start()

processing_t = threading.Thread(target=processing.run)
processing_t.start()


@app.route("/test")
def test_call():
    return {"message": "ok"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
