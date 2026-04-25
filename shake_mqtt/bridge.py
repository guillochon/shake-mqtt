"""Wires UDP ingress → processor → MQTT egress."""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .catalog import (
    enrich_match_distance_and_delta,
    fetch_usgs_nearby_events,
    filter_matches_by_sensitivity,
    pick_closest_catalog_match,
    trigger_time_to_iso_window,
)
from .config import BridgeConfig
from .mqtt_client import MqttPublisher
from .processing import DatagramProcessor, PassthroughJsonNormalizer
from .match_history import MatchHistoryBuffer
from .topic_publish import publish_match_result, publish_sta_lta_event
from .udp import Address, UdpListener

logger = logging.getLogger(__name__)
_FLOAT_CMP_EPS = 1e-9
_MATCH_HISTORY_HYDRATE_TIMEOUT_SEC = 2.0
_KNOWN_ANTHRO_TYPE_TO_SOURCE = {
    "garage_door": "Garage Door",
}
_USGS_EVENT_TYPE_TO_SOURCE = {
    "earthquake": "quake",
    "quarry blast": "quarry",
}


class ShakeMqttBridge:
    def __init__(
        self,
        config: BridgeConfig,
        processor: DatagramProcessor | None = None,
    ) -> None:
        self._config = config
        self._processor: DatagramProcessor = processor or PassthroughJsonNormalizer()
        self._mqtt = MqttPublisher(config)
        self._mqtt.set_message_callback(self._on_mqtt_message)
        self._udp = UdpListener(config)
        self._stop = threading.Event()
        self._catalog_executor: ThreadPoolExecutor | None = None
        if config.catalog_enable:
            self._catalog_executor = ThreadPoolExecutor(
                max_workers=4,
                thread_name_prefix="catalog",
            )
        self._match_history: MatchHistoryBuffer | None = None
        if config.catalog_enable and config.match_history_enable:
            self._match_history = MatchHistoryBuffer(
                window_hours=config.match_history_window_hours,
                max_entries=config.match_history_max_entries,
                shakenet_window_base_url=config.shakenet_window_base_url,
                shakenet_window_before_sec=config.shakenet_window_before_sec,
                shakenet_window_after_sec=config.shakenet_window_after_sec,
            )
        self._catalog_max_query_offset_sec = (
            max(config.catalog_query_offsets_sec)
            if config.catalog_query_offsets_sec
            else 0.0
        )
        self._active_trigger_peaks: dict[tuple[str, float], dict] = {}
        self._active_trigger_peaks_lock = threading.Lock()
        self._known_anthro_window: tuple[str, float, float] | None = None
        self._known_anthro_lock = threading.Lock()

    def _on_datagram(self, data: bytes, addr: Address) -> None:
        result = self._processor.process(data, addr)
        if result.json_payload is not None:
            self._publish_check(
                self._config.mqtt_topic_json(), result.json_payload, addr
            )
        for ev in result.event_payloads:
            try:
                event_obj = json.loads(ev)
            except json.JSONDecodeError:
                continue
            kind = event_obj.get("kind")
            if kind == "trigger":
                self._remember_trigger_peak(event_obj)
            # Leaf topics under `{base}/event/` only reflect trigger start/peak updates.
            if kind == "trigger":
                publish_sta_lta_event(
                    self._config.mqtt_topic_event(),
                    event_obj,
                    lambda t, p: self._publish_check(t, p, addr),
                )
                if self._config.catalog_enable:
                    publish_match_result(
                        self._config.mqtt_topic_match(),
                        event_obj,
                        None,
                        lambda t, p: self._publish_check(t, p, addr),
                    )
                    if self._match_history is not None:
                        hist_json = self._match_history.record_and_dumps(
                            event_obj,
                            None,
                            source=self._source_for_trigger(event_obj),
                        )
                        self._publish_check(
                            self._config.mqtt_topic_match_history_json(),
                            hist_json,
                            addr,
                            retain=True,
                        )
            self._maybe_schedule_catalog(event_obj)

    def _maybe_schedule_catalog(self, trigger: dict) -> None:
        if self._catalog_executor is None:
            return
        if trigger.get("kind") != "trigger":
            return
        if trigger.get("phase") == "update":
            return
        # Known anthropogenic triggers are intentionally excluded from
        # external catalog lookups.
        if self._source_for_trigger(trigger) is not None:
            return
        for off in self._config.catalog_query_offsets_sec:
            self._catalog_executor.submit(
                self._run_delayed_catalog_lookup,
                dict(trigger),
                float(off),
            )

    def _run_delayed_catalog_lookup(self, trigger: dict, delay: float) -> None:
        deadline = time.monotonic() + delay
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if self._stop.wait(timeout=min(remaining, 1.0)):
                return
        self._run_catalog_lookup(trigger)
        # Small epsilon avoids missing cleanup when `delay` differs by tiny FP noise.
        if delay + _FLOAT_CMP_EPS >= self._catalog_max_query_offset_sec:
            self._forget_trigger_peak(trigger)

    def _run_catalog_lookup(self, trigger: dict) -> None:
        trigger = self._latest_trigger_for_lookup(trigger)
        # Re-check at execution time so delayed jobs are skipped if an
        # anthropogenic window is published after scheduling.
        if self._source_for_trigger(trigger) is not None:
            return
        lat = self._config.catalog_latitude
        lon = self._config.catalog_longitude
        if lat is None or lon is None:
            return
        t = trigger.get("time")
        if not isinstance(t, (int, float)):
            return
        start_iso, end_iso = trigger_time_to_iso_window(
            float(t),
            self._config.catalog_time_before_sec,
            self._config.catalog_time_after_sec,
        )
        candidates = fetch_usgs_nearby_events(
            start_iso,
            end_iso,
            lat,
            lon,
            self._config.catalog_max_radius_km,
        )
        candidates = filter_matches_by_sensitivity(
            candidates,
            lat,
            lon,
            enabled=self._config.catalog_sensitivity_filter_enable,
            magnitude_offset=self._config.catalog_sensitivity_magnitude_offset,
        )
        closest = pick_closest_catalog_match(
            float(t),
            lat,
            lon,
            candidates,
            use_traveltime=self._config.catalog_use_traveltime,
            traveltime_model=self._config.catalog_traveltime_model,
            traveltime_timeout_sec=self._config.catalog_traveltime_timeout_sec,
            traveltime_max_workers=self._config.catalog_traveltime_max_workers,
        )
        if closest is not None:
            closest = enrich_match_distance_and_delta(
                lat,
                lon,
                closest,
                magnitude_offset=self._config.catalog_sensitivity_magnitude_offset,
            )
        addr: Address = ("catalog", 0)
        publish_match_result(
            self._config.mqtt_topic_match(),
            trigger,
            closest,
            lambda t, p: self._publish_check(t, p, addr),
        )
        if self._match_history is not None:
            hist_json = self._match_history.record_and_dumps(
                trigger,
                closest,
                source=self._history_source_for_match(trigger, closest),
            )
            self._publish_check(
                self._config.mqtt_topic_match_history_json(),
                hist_json,
                addr,
                retain=True,
            )
        if closest is not None:
            logger.info(
                "USGS match for trigger @ %s (mode=%s)",
                t,
                closest.get("match_mode"),
            )

    @staticmethod
    def _trigger_key(trigger: dict) -> tuple[str, float] | None:
        ch = trigger.get("channel")
        t = trigger.get("time")
        if not isinstance(ch, str):
            return None
        if not isinstance(t, (int, float)):
            return None
        return (ch, float(t))

    def _remember_trigger_peak(self, trigger: dict) -> None:
        key = self._trigger_key(trigger)
        if key is None:
            return
        with self._active_trigger_peaks_lock:
            self._active_trigger_peaks[key] = dict(trigger)

    def _forget_trigger_peak(self, trigger: dict) -> None:
        key = self._trigger_key(trigger)
        if key is None:
            return
        with self._active_trigger_peaks_lock:
            self._active_trigger_peaks.pop(key, None)

    def _latest_trigger_for_lookup(self, trigger: dict) -> dict:
        key = self._trigger_key(trigger)
        if key is None:
            return dict(trigger)
        with self._active_trigger_peaks_lock:
            latest = self._active_trigger_peaks.get(key)
        return dict(latest) if latest is not None else dict(trigger)

    def _on_mqtt_message(self, topic: str, payload: bytes) -> None:
        if topic != self._config.known_anthro_topic:
            return
        try:
            obj = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            logger.warning("Ignoring invalid known anthropogenic payload on %s", topic)
            return
        if not isinstance(obj, dict):
            logger.warning(
                "Ignoring non-object known anthropogenic payload on %s", topic
            )
            return

        raw_type = obj.get("type")
        if not isinstance(raw_type, str):
            logger.warning("Ignoring known anthropogenic payload without string type")
            return
        event_type = raw_type.strip().lower()
        if event_type == "none":
            with self._known_anthro_lock:
                self._known_anthro_window = None
            return

        source = _KNOWN_ANTHRO_TYPE_TO_SOURCE.get(event_type)
        if source is None:
            logger.warning("Ignoring unsupported known anthropogenic type=%s", raw_type)
            return

        duration_raw = obj.get("expected_duration")
        if not isinstance(duration_raw, (int, float)) or float(duration_raw) < 0:
            logger.warning(
                "Ignoring known anthropogenic payload with invalid expected_duration"
            )
            return
        duration = float(duration_raw)
        update_time = time.time()
        with self._known_anthro_lock:
            self._known_anthro_window = (
                source,
                update_time - duration,
                update_time + duration,
            )

    def _source_for_trigger(self, trigger: dict) -> str | None:
        t = trigger.get("time")
        if not isinstance(t, (int, float)):
            return None
        trigger_time = float(t)
        with self._known_anthro_lock:
            window = self._known_anthro_window
        if window is None:
            return None
        source, start_time, end_time = window
        if start_time <= trigger_time <= end_time:
            return source
        return None

    def _history_source_for_match(
        self, trigger: dict, match: dict | None
    ) -> str | None:
        if match is not None:
            raw_event_type = match.get("usgs_event_type")
            if isinstance(raw_event_type, str):
                mapped = _USGS_EVENT_TYPE_TO_SOURCE.get(raw_event_type.strip().lower())
                if mapped is not None:
                    return mapped
        return self._source_for_trigger(trigger)

    def _hydrate_match_history_from_mqtt(self) -> None:
        hist = self._match_history
        if hist is None:
            return
        topic = self._config.mqtt_topic_match_history_json()
        payload = self._mqtt.wait_for_retained(
            topic, _MATCH_HISTORY_HYDRATE_TIMEOUT_SEC
        )
        if payload is None:
            logger.info("No retained match history found on %s", topic)
            return
        try:
            count = hist.load_from_json(payload)
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            logger.warning("Ignoring invalid retained match history on %s", topic)
            return
        logger.info("Hydrated %d match-history rows from %s", count, topic)

    def _publish_check(
        self,
        topic: str,
        payload: str,
        addr: Address,
        *,
        retain: bool = False,
    ) -> None:
        info = self._mqtt.publish(topic, payload, retain=retain)
        if info.rc != 0:
            logger.warning("MQTT publish queue error topic=%s rc=%s", topic, info.rc)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Published %d bytes to %s (from %s:%s)",
                len(payload),
                topic,
                addr[0],
                addr[1],
            )

    def run(self) -> None:
        self._mqtt.connect()
        try:
            self._hydrate_match_history_from_mqtt()
            self._udp.bind()
            self._udp.run_forever(self._on_datagram, self._stop)
        finally:
            self._udp.close()
            if self._catalog_executor is not None:
                self._catalog_executor.shutdown(wait=False, cancel_futures=True)
                self._catalog_executor = None
            self._mqtt.disconnect()

    def stop(self) -> None:
        self._stop.set()
