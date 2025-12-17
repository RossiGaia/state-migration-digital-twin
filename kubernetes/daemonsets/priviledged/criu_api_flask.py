from flask import Flask, request, jsonify
import signal
import subprocess

def graceful_shutdown(signum, frame):
    exit(0)

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

app = Flask(__name__)

@app.route("/checkpoint", methods=["POST"])
def checkpoint():
    ns = request.json["namespace"]
    pod = request.json["pod"]
    container = request.json["container"]
    token = request.json["token"]

    env = {
        "CRIU_DUMP_NAMESPACE": ns,
        "CRIU_DUMP_POD_NAME": pod,
        "CRIU_DUMP_CONTAINER_NAME": container,
        "CRIU_DUMP_TOKEN": token
    }

    try:
        out = subprocess.check_output(
            ["/home/ubuntu/scripts/criu_dump.sh"],
            env=env,
            stderr=subprocess.STDOUT,
            text=True
        )
        return jsonify({"status": "ok", "output": out})
    except subprocess.CalledProcessError as e:
        return jsonify({"status": "error", "output": e.output}), 500

@app.route("/healthsz", methods=["GET"])
def get_health():
    return jsonify({"status": "healthy"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000)