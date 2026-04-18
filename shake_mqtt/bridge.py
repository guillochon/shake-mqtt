"""Wires UDP ingress → processor → MQTT egress."""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .catalog import (
    fetch_usgs_nearby_events,
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


class ShakeMqttBridge:
    def __init__(
        self,
        config: BridgeConfig,
        processor: DatagramProcessor | None = None,
    ) -> None:
        self._config = config
        self._processor: DatagramProcessor = processor or PassthroughJsonNormalizer()
        self._mqtt = MqttPublisher(config)
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
            )

    def _on_datagram(self, data: bytes, addr: Address) -> None:
        result = self._processor.process(data, addr)
        if result.json_payload is not None:
            self._publish_check(self._config.mqtt_topic_json(), result.json_payload, addr)
        for ev in result.event_payloads:
            try:
                event_obj = json.loads(ev)
            except json.JSONDecodeError:
                continue
            # Leaf topics under `{base}/event/` only reflect triggers so subscribers stay in sync.
            if event_obj.get("kind") == "trigger":
                publish_sta_lta_event(
                    self._config.mqtt_topic_event(),
                    event_obj,
                    lambda t, p: self._publish_check(t, p, addr),
                )
            self._maybe_schedule_catalog(event_obj)

    def _maybe_schedule_catalog(self, trigger: dict) -> None:
        if self._catalog_executor is None:
            return
        if trigger.get("kind") != "trigger":
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

    def _run_catalog_lookup(self, trigger: dict) -> None:
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
        addr: Address = ("catalog", 0)
        publish_match_result(
            self._config.mqtt_topic_match(),
            trigger,
            closest,
            lambda t, p: self._publish_check(t, p, addr),
        )
        if self._match_history is not None:
            hist_json = self._match_history.record_and_dumps(trigger, closest)
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
