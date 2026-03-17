from dataclasses import dataclass, asdict
import time
import json
import collections
import threading
import queue
import pymongo
import sys
import logging

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


@dataclass(frozen=True)
class ProcessingConfig:
    pulley_radius_m: float = 0.15
    rolling_window: int = 50

    # anomaly thresholds
    vib_high: float = 3.5
    tension_high: float = 280.0
    friction_high: float = 0.06
    temp_rise_overheat: float = 35.0
    speed_drop: float = 1.0
    speed_drop_load_min: float = 0.1

    # ODTE params
    odte_window_length_sec: int = 10
    odte_desired_timeliness_sec: float = 1.0
    odte_expected_msg_sec: int = 1
    odte_refresh_s: float = 0.5


class Processing:
    def __init__(
        self,
        *,
        connection_buffer,
        processing_buffer,
        state_max_size=None,
        worker=0,
        work=0,
        config: ProcessingConfig | None = None,
        redis_client=None,
        messages_buffer
    ):
        self.connection_buffer = connection_buffer
        self.processing_buffer = processing_buffer
        self.observations = collections.deque(maxlen=100)
        self.running = True
        self.conveyor_params = VirtualizedConveyorPlant()
        self.state_max_size = state_max_size
        self.odte = None
        self.lock = threading.Lock()
        self.burn_worker = worker
        self.burn_work = work
        self.burn_queue = queue.Queue(maxsize=self.burn_worker * 2)
        self.burn_threads = []
        self.processing_seq_no = 0
        self.cfg = config or ProcessingConfig()
        self.odte_t = threading.Thread(target=self.odte_computation, daemon=True)
        self.redis_client = redis_client
        self.message_seq_id = None
        self.messages_buffer = messages_buffer
        self.burn_feeder_t = threading.Thread(target=self.burn_feeder_loop, daemon=True)

        if self.burn_worker > 0 and self.burn_work > 0:
            for i in range(self.burn_worker):
                t = threading.Thread(target=self.burn_worker_loop, daemon=True)
                t.start()
                self.burn_threads.append(t)

            self.burn_feeder_t.start()

    def run(self):

        self.odte_t.start()

        while self.running:

            t0 = time.time()
            with self.lock:
                if not self.connection_buffer:
                    raw_msg = None
                else:
                    raw_msg = self.connection_buffer.popleft()

            if raw_msg is None:
                time.sleep(0.001)
                continue

            current_seq_id = raw_msg["payload"]["seq_id"]
            logger.debug(f"Seq_id: {current_seq_id}")

            # get the state from the shared storage
            self._get_state_from_redis()

            self.message_seq_id = current_seq_id
            snap = self._build_snap(raw_msg)
            self._add_derived_metrics(snap)
            self._add_rolling_features(snap)
            self._add_health_and_anomalies(snap)
            self._maybe_pad(snap)

            t1 = time.time()

            processing_time = t1 - t0
            snap["processing_time_s"] = processing_time

            creation_timestamp = raw_msg["payload"]["creation_timestamp"]
            received_timestamp = raw_msg["recv_timestamp"]
            processed_timestamp = t1

            logger.debug(
                f"Processing elaborated message {self.message_seq_id}:\ngenerated at: {creation_timestamp}\narrived at {received_timestamp}\nprocessed at: {processed_timestamp}"
            )

            logger.debug(f"Delay between creation and processing: {processed_timestamp-creation_timestamp} s")
            logger.debug(f"Delay between creation and received: {received_timestamp-creation_timestamp} s")

            with self.lock:
                self.processing_buffer.append(snap)
                self._record_observation(snap, processing_time)

            logger.debug(f"time to elaborate the message: {processing_time}")

            # push the state to the shared storage
            self._set_state_in_redis()

    def _get_state_from_redis(self):
        state_json = self.redis_client.get("digital_twin_state")
        if state_json:
            self.deserialize_state(json.loads(state_json))
            logger.info("Digital Twin state restored from Redis.")

    def _set_state_in_redis(self):
        lock = self.redis_client.lock(f"dt-lock", blocking=False)

        state_json = json.dumps(self.serialize_state())
        acquired = False
        try:
            acquired = lock.acquire()
            if not acquired:
                logger.debug("Lock not acquired, skipping write.")
                return

            self.redis_client.set("digital_twin_state", state_json)
            logger.info("Digital Twin state saved to Redis.")

        except Exception as e:
            logger.warning(f"Redis write failed: {e}")

        finally:
            if acquired:
                try:
                    lock.release()
                except Exception:
                    pass

    def _build_snap(self, raw_msg: dict) -> dict:
        payload = raw_msg["payload"]
        status = payload.get("status", {})

        with self.lock:
            pt = self.conveyor_params
            pt.name = status.get("name", pt.name)
            pt.load = status.get("load", pt.load)
            pt.angular_acceleration = status.get(
                "angular_acceleration", pt.angular_acceleration
            )
            pt.angular_speed = status.get("angular_speed", pt.angular_speed)
            pt.motor_vibration = status.get("motor_vibration", pt.motor_vibration)
            pt.belt_tension = status.get("belt_tension", pt.belt_tension)
            pt.ambient_temperature = status.get(
                "ambient_temperature", pt.ambient_temperature
            )
            pt.motor_temperature = status.get("motor_temperature", pt.motor_temperature)
            pt.belt_friction = status.get("belt_friction", pt.belt_friction)
            pt.wear = status.get("wear", pt.wear)

            snap = asdict(pt)

        snap["recv_timestamp"] = raw_msg["recv_timestamp"]
        snap["creation_timestamp"] = payload["creation_timestamp"]
        snap["key"] = raw_msg["key"]
        snap["seq_id"] = self.processing_seq_no
        self.processing_seq_no += 1
        return snap

    def _add_derived_metrics(self, snap: dict) -> None:
        R = self.cfg.pulley_radius_m
        approx_torque = snap["belt_tension"] * R
        approx_power_w = approx_torque * snap["angular_speed"]

        snap["approx_torque_Nm"] = round(approx_torque, 3)
        snap["approx_power_W"] = round(approx_power_w, 2)
        snap["temp_rise_C"] = round(
            snap["motor_temperature"] - snap["ambient_temperature"], 3
        )

    def _add_rolling_features(self, snap: dict) -> None:
        for key in (
            "angular_speed",
            "motor_vibration",
            "belt_tension",
            "belt_friction",
            "motor_temperature",
        ):
            stats = self._rolling_stats(key, window=self.cfg.rolling_window)
            snap[f"{key}_mean"] = (
                None if stats["mean"] is None else round(stats["mean"], 4)
            )
            snap[f"{key}_slope_per_s"] = (
                None if stats["slope_per_s"] is None else round(stats["slope_per_s"], 6)
            )

    def _add_health_and_anomalies(self, snap: dict) -> None:
        snap["health"] = self._health_score(snap)

        snap["anomaly"] = {
            "vibration_high": snap.get("motor_vibration") is not None
            and snap["motor_vibration"] > self.cfg.vib_high,
            "tension_high": snap.get("belt_tension") is not None
            and snap["belt_tension"] > self.cfg.tension_high,
            "friction_high": snap.get("belt_friction") is not None
            and snap["belt_friction"] > self.cfg.friction_high,
            "temp_overheat": snap.get("temp_rise_C") is not None
            and snap["temp_rise_C"] > self.cfg.temp_rise_overheat,
            "speed_drop": (
                snap.get("angular_speed") is not None
                and snap["angular_speed"] < self.cfg.speed_drop
                and snap.get("load", 0.0) > self.cfg.speed_drop_load_min
            ),
        }

    def _maybe_pad(self, snap: dict) -> None:
        if not self.state_max_size:
            return
        snap_size = len(json.dumps(snap).encode("utf-8"))
        snap["padding"] = self._generate_padding(self.state_max_size, snap_size)

    def _record_observation(self, snap: dict, processing_time: float) -> None:
        self.observations.append(
            {
                "key": snap["key"],
                "received_timestamp": snap["recv_timestamp"],
                "creation_timestamp": snap["creation_timestamp"],
                "execution_time_s": processing_time,
                "obs_value": (snap["recv_timestamp"] - snap["creation_timestamp"])
                + processing_time,
            }
        )

    def _generate_padding(self, max_size, current_size):
        buffer_max_length = self.processing_buffer.maxlen
        padding_length = (
            round(max_size * 1024 * 1024 / buffer_max_length) - current_size
        )
        logger.debug(f"Padding size: {padding_length}")
        if padding_length > 0:
            return "0" * padding_length
        else:
            return None

    def _compute_timeliness(self, desired_timeliness_sec: float):
        with self.lock:
            obs_list = list(self.observations)

        if len(obs_list) == 0:
            return 0.0

        count = 0
        for obs_obj in obs_list:
            if obs_obj["obs_value"] <= desired_timeliness_sec:
                count += 1

        percentile = float(count / len(obs_list))

        return percentile

    def _compute_reliability(self, window_length_sec: int, expected_msg_sec: int):
        end_window_time = time.time()
        start_window_time = time.time() - window_length_sec

        with self.lock:
            msg_list = list(self.messages_buffer)
        
        count = 0
        for msg in msg_list:
            if (
                msg["payload"]["creation_timestamp"] >= start_window_time
                and msg["payload"]["creation_timestamp"] <= end_window_time
            ):
                count += 1

        expected_msg_tot = window_length_sec * expected_msg_sec

        return float(count / expected_msg_tot)

    def _compute_availability(self):
        return 1.0

    def _compute_odte_phytodig(
        self, window_length_sec, desired_timeliness_sec, expected_msg_sec
    ):
        timeliness = self._compute_timeliness(desired_timeliness_sec)
        reliability = self._compute_reliability(window_length_sec, expected_msg_sec)
        availability = self._compute_availability()

        logger.debug(
            f"Availability: {availability}\tReliability: {reliability}\tTimeliness: {timeliness}"
        )

        return timeliness * reliability * availability

    def stop(self):
        self.running = False
        for _ in range(self.burn_worker):
            try:
                self.burn_queue.put(None, timeout=0.1)
            except queue.Full:
                pass
        for t in self.burn_threads:
            t.join(timeout=1.0)

    def _safe_float(self, value, default=0.0):
        """Coerce to float safely."""
        try:
            if value is None:
                return float(default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _rolling_stats(self, key: str, window: int = 50):
        with self.lock:
            if not self.processing_buffer:
                return {"mean": None, "slope_per_s": None}

            rows = list(self.processing_buffer)[-window:]

        pairs = []
        for r in rows:
            y = r.get(key)
            t = r.get("recv_timestamp")
            if y is None or t is None:
                continue
            pairs.append((t, y))

        if not pairs:
            return {"mean": None, "slope_per_s": None}
        if len(pairs) == 1:
            return {"mean": float(pairs[0][1]), "slope_per_s": None}

        ts, ys = zip(*pairs)
        mean = sum(ys) / len(ys)

        t0 = ts[0]
        xt = [t - t0 for t in ts]
        xbar = sum(xt) / len(xt)
        ybar = mean
        num = sum((xt[i] - xbar) * (ys[i] - ybar) for i in range(len(ys)))
        den = sum((xt[i] - xbar) ** 2 for i in range(len(ys))) or 1.0
        return {"mean": mean, "slope_per_s": num / den}

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

    def serialize_state(self):
        start_time = time.time()
        with self.lock:
            state = {
                "processing_buffer": list(self.processing_buffer),
                "conveyor_params": asdict(self.conveyor_params),
            }
        serialization_time = time.time() - start_time
        logger.info(f"Serialization took {serialization_time} seconds.")
        return state


    def deserialize_state(self, serialized_state):
        start_time = time.time()
        with self.lock:
            try:
                self.processing_buffer.clear()
                self.processing_buffer.extend(serialized_state["processing_buffer"])
                self.conveyor_params = VirtualizedConveyorPlant(
                    **serialized_state["conveyor_params"]
                )
            except Exception as e:
                logger.error(f"Error in deserialization. {e}")
                return -1
            
        deserialization_time = time.time() - start_time
        logger.info(f"Deserialization took {deserialization_time} seconds.")
        return 0
    

    def odte_computation(self):
        while self.running:
            new_odte = self._compute_odte_phytodig(
                self.cfg.odte_window_length_sec,
                self.cfg.odte_desired_timeliness_sec,
                self.cfg.odte_expected_msg_sec,
            )
            with self.lock:
                self.odte = new_odte

            time.sleep(self.cfg.odte_refresh_s)

    def get_odte(self):
        with self.lock:
            return self.odte

    def _burn_cpu_primes(self, max_n: int):
        for i in range(3, max_n + 1):
            is_prime = True
            for j in range(2, int(i**0.5) + 1):
                if i % j == 0:
                    is_prime = False
                    break
    
    def burn_worker_loop(self):
        while True:
            work = self.burn_queue.get()
            if work is None:
                self.burn_queue.task_done()
                break
            self._burn_cpu_primes(work)
            self.burn_queue.task_done()

    def burn_feeder_loop(self):
        while self.running:
            try:
                self.burn_queue.put(self.burn_work, timeout=0.1)
            except queue.Full:
                pass
