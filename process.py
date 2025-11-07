from dataclasses import dataclass, asdict
import time
import logging
import json
import collections

logging.basicConfig(
    format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
    handlers=[logging.FileHandler("dt_process.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


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


class Processing:
    def __init__(self, *, connection_buffer, processing_buffer, state_max_size=None):
        self.connection_buffer = connection_buffer
        self.processing_buffer = processing_buffer
        self.observations = collections.deque(maxlen=100)
        self.messages = collections.deque(maxlen=100)
        self.running = True
        self.conveyor_params = VirtualizedConveyorPlant()
        self.state_max_size = state_max_size
        self.odte = None

    def run(self):
        """
        Drain connection_buffer, update virtual twin, compute KPIs, and append snapshots.
        Each processing_buffer item is a compact dict with raw + derived fields.
        """
        while self.running:

            if len(self.connection_buffer) > 0:
                raw_msg = None
                try:
                    raw_msg = self.connection_buffer.popleft()
                    payload = raw_msg["payload"]
                    # logger.debug(
                    #     f"popped from connection_buffer {raw_msg["payload"]["status"]}."
                    # )
                except Exception as e:
                    logger.debug(e)
                    continue

                status = payload.get("status", {})
                pt_metrics = self.conveyor_params
                pt_metrics.name = status.get("name", pt_metrics.name)
                pt_metrics.load = status.get("load", pt_metrics.load)
                pt_metrics.angular_acceleration = status.get(
                    "angular_acceleration", pt_metrics.angular_acceleration
                )
                pt_metrics.angular_speed = status.get(
                    "angular_speed", pt_metrics.angular_speed
                )
                pt_metrics.motor_vibration = status.get(
                    "motor_vibration", pt_metrics.motor_vibration
                )
                pt_metrics.belt_tension = status.get(
                    "belt_tension", pt_metrics.belt_tension
                )
                pt_metrics.ambient_temperature = status.get(
                    "ambient_temperature", pt_metrics.ambient_temperature
                )
                pt_metrics.motor_temperature = status.get(
                    "motor_temperature", pt_metrics.motor_temperature
                )
                pt_metrics.belt_friction = status.get(
                    "belt_friction", pt_metrics.belt_friction
                )
                pt_metrics.wear = status.get("wear", pt_metrics.wear)

                snap = self._snapshot()

                # Derived metrics (per-sample)
                # Instantaneous mechanical power proxy: P ≈ T * ω (units: N·m * rad/s = W),
                # but we don't have shaft torque; approximate with belt_tension and a nominal pulley radius.
                # If you know the radius, put it in config; use 0.15 m nominal.
                R_PULLEY = 0.15
                approx_torque = snap["belt_tension"] * R_PULLEY  # N·m
                approx_power_w = approx_torque * snap["angular_speed"]  # W

                snap["approx_torque_Nm"] = round(approx_torque, 3)
                snap["approx_power_W"] = round(approx_power_w, 2)
                snap["temp_rise_C"] = round(
                    snap["motor_temperature"] - snap["ambient_temperature"], 3
                )

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

                snap_size = len(json.dumps(snap))

                if self.state_max_size:
                    snap["padding"] = self.generate_padding(
                        self.state_max_size, snap_size
                    )

                self.processing_buffer.append(snap)
                logger.info(
                    f"Buffers size in MB:\n\tconnection_buffer: {len(json.dumps(list(self.connection_buffer)).encode("utf-8")) / 1024 / 1024}\n\tprocessing_buffer: {len(json.dumps(list(self.processing_buffer)).encode("utf-8")) / 1024 / 1024} with {len(self.processing_buffer)} number of elements"
                )
                # logger.debug(json.dumps(snap, indent=4))
                logger.debug(
                    f"time to elaborate the message: {snap["processed_timestamp"] - snap["recv_timestamp"]}"
                )

    def generate_padding(self, max_size, current_size):
        buffer_max_length = self.processing_buffer.maxlen
        padding_length = (
            round(max_size * 1024 * 1024 / buffer_max_length) - current_size
        )
        logger.debug(f"Padding size: {padding_length}")
        if padding_length > 0:
            return "0" * padding_length
        else:
            return None

    def compute_timeliness(self, desired_timeliness_sec: float):
        obs_list = list(self.observations)

        if len(obs_list) == 0:
            return 0.0

        count = 0
        for obs in obs_list:
            if obs <= desired_timeliness_sec:
                count += 1

        percentile = float(count / len(obs_list))

        return percentile

    def compute_reliability(
        self, window_length_sec: int, expected_msg_sec: int
    ):
        end_window_time = time.time()
        start_window_time = time.time() - window_length_sec

        msg_list = list(self.messages)
        msg_required = msg_list[-window_length_sec * expected_msg_sec :]

        count = 0
        for msg in msg_required:
            if (
                msg["timestamp"] >= start_window_time
                and msg["timestamp"] <= end_window_time
            ):
                count += 1

        expected_msg_tot = window_length_sec * expected_msg_sec

        return float(count / expected_msg_tot)

    def compute_availability(self):
        return 1.0

    def compute_odte_phytodig(
        self, window_length_sec, desired_timeliness_sec, expected_msg_sec
    ):
        timeliness = self.compute_timeliness(desired_timeliness_sec)
        reliability = self.compute_reliability(window_length_sec, expected_msg_sec)
        availability = self.compute_availability()

        logger.debug(
            f"Availability: {availability}\tReliability: {reliability}\tTimeliness: {timeliness}"
        )

        return timeliness * reliability * availability

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
        slope = num / den
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
