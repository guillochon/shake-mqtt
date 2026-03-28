"""Wires UDP ingress → processor → MQTT egress."""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from .catalog import catalog_followup_payload, fetch_usgs_nearby_events, trigger_time_to_iso_window
from .config import BridgeConfig
from .mqtt_client import MqttPublisher
from .processing import DatagramProcessor, PassthroughJsonNormalizer
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
                max_workers=2,
                thread_name_prefix="catalog",
            )

    def _on_datagram(self, data: bytes, addr: Address) -> None:
        result = self._processor.process(data, addr)
        if result.json_payload is not None:
            self._publish_check(self._config.mqtt_topic_json(), result.json_payload, addr)
        for ev in result.event_payloads:
            self._publish_check(self._config.mqtt_topic_event(), ev, addr)
            self._maybe_schedule_catalog(ev)

    def _maybe_schedule_catalog(self, event_json: str) -> None:
        if self._catalog_executor is None:
            return
        try:
            trigger = json.loads(event_json)
        except json.JSONDecodeError:
            return
        if trigger.get("kind") != "trigger":
            return
        self._catalog_executor.submit(self._run_catalog_lookup, dict(trigger))

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
        matches = fetch_usgs_nearby_events(
            start_iso,
            end_iso,
            lat,
            lon,
            self._config.catalog_max_radius_km,
        )
        payload = catalog_followup_payload(trigger, matches)
        addr: Address = ("catalog", 0)
        self._publish_check(self._config.mqtt_topic_event_catalog(), payload, addr)
        if matches:
            logger.info("Catalog: %d USGS event(s) near trigger @ %s", len(matches), t)

    def _publish_check(self, topic: str, payload: str, addr: Address) -> None:
        info = self._mqtt.publish(topic, payload)
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
