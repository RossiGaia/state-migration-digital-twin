from dataclasses import dataclass, asdict
import collections
import yaml
import time
import logging
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# maybe set it as env var
conf_path = "config.yaml"

# create the virtualized version of the conveyor belt (copy the vars)
# expose the values directly
# expose the calculations

conf_raw = open(conf_path)
process_conf = yaml.safe_load(conf_raw)["process"]

buffer_conf = process_conf["buffer"]


@dataclass
class VirtualizedConveyorPlant:

    name: str = "CNV1"

    load: float = 0.0
    angular_acceleration: float = 0.0
    angular_speed: float = 0.0
    motor_vibration: float = 0.0
    belt_tension: float = 0.0
    ambient_temperature: float = 0.0
    motor_temperature: float = 0.0
    belt_friction: float = 0.0

    wear: float = 5.0


class BaseProcessing:
    def __init__(self, recv_buffer):
        self.recv_buffer = recv_buffer
        self.processing_buffer = collections.deque(maxlen=buffer_conf["size"])
        self.running = True
        self.conveyor_params = VirtualizedConveyorPlant()

    def run(self):
        """
        Drain recv_buffer, update virtual twin, compute KPIs, and append snapshots.
        Each processing_buffer item is a compact dict with raw + derived fields.
        """
        while self.running:

            if len(self.recv_buffer) > 0:
                raw_msg = None
                try:
                    raw_msg = self.recv_buffer.popleft()
                    payload = raw_msg["payload"]
                    logger.debug(
                        f"popped from recv_buffer {raw_msg["payload"]["status"]}."
                    )
                except Exception as e:
                    # Malformed JSON or non-string; skip safely
                    logger.debug(e)
                    continue

                status = payload.get("status", {}) if isinstance(payload, dict) else {}
                # Update the virtualized twin (type-safe coercion)
                vp = self.conveyor_params
                vp.name = status.get("name", vp.name)
                vp.load = status.get("load", vp.load)
                vp.angular_acceleration = status.get(
                    "angular_acceleration", vp.angular_acceleration
                )
                vp.angular_speed = status.get("angular_speed", vp.angular_speed)
                vp.motor_vibration = status.get("motor_vibration", vp.motor_vibration)
                vp.belt_tension = status.get("belt_tension", vp.belt_tension)
                vp.ambient_temperature = status.get(
                    "ambient_temperature", vp.ambient_temperature
                )
                vp.motor_temperature = status.get(
                    "motor_temperature", vp.motor_temperature
                )
                vp.belt_friction = status.get("belt_friction", vp.belt_friction)
                vp.wear = status.get("wear", vp.wear)

                # Build snapshot (raw)
                snap = self._snapshot()

                # Derived metrics (per-sample)
                # Instantaneous mechanical power proxy: P ≈ T * ω (units: N·m * rad/s = W),
                # but we don't have shaft torque; approximate with belt_tension and a nominal pulley radius.
                # If you know the radius, put it in config; we use 0.15 m nominal here.
                R_PULLEY = 0.15
                approx_torque = snap["belt_tension"] * R_PULLEY  # N·m
                approx_power_w = approx_torque * snap["angular_speed"]  # W

                snap["approx_torque_Nm"] = round(approx_torque, 3)
                snap["approx_power_W"] = round(approx_power_w, 2)
                snap["temp_rise_C"] = round(
                    snap["motor_temperature"] - snap["ambient_temperature"], 3
                )

                # Append raw + instant derived first, so rolling stats include this point
                self.processing_buffer.append(snap)

                # Rolling stats (last N points; N from config size or a subset)
                for key in (
                    "angular_speed",
                    "motor_vibration",
                    "belt_tension",
                    "belt_friction",
                    "motor_temperature",
                ):
                    stats = self._rolling_stats(key)
                    snap[f"{key}_mean"] = (
                        None if stats["mean"] is None else round(stats["mean"], 4)
                    )
                    snap[f"{key}_slope_per_s"] = (
                        None
                        if stats["slope_per_s"] is None
                        else round(stats["slope_per_s"], 6)
                    )

                # Health score & simple anomaly flags
                health = self._health_score(snap)
                snap["health"] = health
                snap["anomaly"] = {
                    "vibration_high": (
                        snap["motor_vibration"] is not None
                        and snap["motor_vibration"] > 3.5
                    ),
                    "tension_high": (
                        snap["belt_tension"] is not None and snap["belt_tension"] > 280
                    ),
                    "friction_high": (
                        snap["belt_friction"] is not None
                        and snap["belt_friction"] > 0.06
                    ),
                    "temp_overheat": (
                        snap["temp_rise_C"] is not None and snap["temp_rise_C"] > 35.0
                    ),
                    "speed_drop": (
                        snap["angular_speed"] is not None
                        and snap["angular_speed"] < 1.0
                        and snap["load"] > 0.1
                    ),
                }

                snap["processed_timestamp"] = time.time()
                snap["recv_timestamp"] = raw_msg["recv_timestamp"]
                # Replace the last appended item with enriched snapshot (keep deque length)
                self.processing_buffer[-1] = snap
                logger.info(
                    f"Buffers size in MB:\n\trecv_buffer: {len(json.dumps(list(self.recv_buffer)).encode("utf-8")) / 1024 / 1024}\n\tprocessing_buffer: {len(json.dumps(list(self.processing_buffer)).encode("utf-8")) / 1024 / 1024}"
                )
                logger.debug(json.dumps(snap, indent=4))
                logger.debug(f"time to elaborate the message: {snap["processed_timestamp"] - snap["recv_timestamp"]}")
            time.sleep(0.5)

    def stop(self):
        self.running = False

    def _safe_float(self, value, default=0.0):
        """Coerce to float safely."""
        try:
            if value is None:
                return float(default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _snapshot(self):
        """Return a plain dict snapshot of current params (JSON-friendly)."""
        snap = asdict(self.conveyor_params)
        snap["timestamp"] = time.time()
        return snap

    def _rolling_stats(self, key):
        """Rolling mean + slope over a recent window (optional)."""
        buf = self.processing_buffer
        if not buf:
            return {"mean": None, "slope_per_s": None}

        window = len(buf)

        xs = list(buf)[-window:]
        ys = [row.get(key) for row in xs if row.get(key) is not None]
        ts = [row["timestamp"] for row in xs if row.get(key) is not None]
        if len(ys) < 2:
            return {"mean": sum(ys) / len(ys) if ys else None, "slope_per_s": None}

        mean = sum(ys) / len(ys)

        # simple linear regression slope (y vs t)
        t0 = ts[0]
        xt = [t - t0 for t in ts]
        xbar = sum(xt) / len(xt)
        ybar = mean
        num = sum((xt[i] - xbar) * (ys[i] - ybar) for i in range(len(ys)))
        den = sum((xt[i] - xbar) ** 2 for i in range(len(ys))) or 1.0
        slope = num / den  # units per second
        return {"mean": mean, "slope_per_s": slope}

    def _health_score(self, row):
        """
        Heuristic 0..100 (higher is healthier).
        Penalize high vibration, high friction, high motor temp above ambient, high wear, excessive tension.
        """
        vib = self._safe_float(row.get("motor_vibration"), 0.0)  # mm/s
        mu = self._safe_float(row.get("belt_friction"), 0.02)  # -
        tmo = self._safe_float(row.get("motor_temperature"), 0.0)  # °C
        amb = self._safe_float(row.get("ambient_temperature"), 20.0)
        wear = self._safe_float(row.get("wear"), 0.0)  # 0..100
        tens = self._safe_float(row.get("belt_tension"), 200.0)  # N

        vib_risk = min(vib / 4.5, 1.0)  # >4.5 mm/s is concerning
        mu_risk = min(max((mu - 0.03) / 0.02, 0.0), 1.0)  # 0.03..0.05 window
        temp_risk = min(max((tmo - amb - 25.0) / 35.0, 0.0), 1.0)  # > amb+25 °C
        wear_risk = min(wear / 100.0, 1.0)
        tens_risk = min(max((tens - 260.0) / 100.0, 0.0), 1.0)  # >260 N

        risk = (
            0.30 * vib_risk
            + 0.20 * mu_risk
            + 0.25 * temp_risk
            + 0.15 * wear_risk
            + 0.10 * tens_risk
        )
        health = max(0.0, 100.0 * (1.0 - risk))
        return round(health, 1)
