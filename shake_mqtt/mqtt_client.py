"""MQTT publisher with background network loop."""

from __future__ import annotations

import logging
import ssl
import threading
from collections.abc import Callable

from paho.mqtt.client import CallbackAPIVersion, Client, MQTTMessage, MQTTMessageInfo

from .config import BridgeConfig

logger = logging.getLogger(__name__)


class MqttPublisher:
    def __init__(self, config: BridgeConfig) -> None:
        self._config = config
        self._on_message_cb: Callable[[str, bytes], None] | None = None
        self._retained_waiters: dict[str, tuple[threading.Event, bytes | None]] = {}
        self._retained_waiters_lock = threading.Lock()
        self._client = Client(
            CallbackAPIVersion.VERSION2,
            client_id=config.mqtt_client_id,
        )
        if config.mqtt_username is not None:
            self._client.username_pw_set(config.mqtt_username, config.mqtt_password)

        if config.mqtt_tls:
            ctx = ssl.create_default_context()
            self._client.tls_set_context(ctx)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def _on_connect(
        self, client, userdata, connect_flags, reason_code, properties
    ) -> None:
        if not reason_code.is_failure:
            logger.info(
                "MQTT connected to %s:%s",
                self._config.mqtt_broker_host,
                self._config.mqtt_broker_port,
            )
            topic = self._config.known_anthro_topic
            if topic:
                result, _mid = self._client.subscribe(topic, qos=0)
                if result != 0:
                    logger.warning(
                        "MQTT subscribe failed topic=%s rc=%s", topic, result
                    )
                else:
                    logger.info("MQTT subscribed to %s", topic)
        else:
            logger.error("MQTT connect failed: %s", reason_code)

    def _on_disconnect(
        self, client, userdata, disconnect_flags, reason_code, properties
    ) -> None:
        if reason_code.is_failure:
            logger.warning("MQTT disconnect: %s", reason_code)

    def _on_message(self, client, userdata, message: MQTTMessage) -> None:
        if message.retain:
            with self._retained_waiters_lock:
                waiter = self._retained_waiters.get(message.topic)
                if waiter is not None:
                    ev, _payload = waiter
                    self._retained_waiters[message.topic] = (ev, bytes(message.payload))
                    ev.set()
        cb = self._on_message_cb
        if cb is None:
            return
        try:
            cb(message.topic, bytes(message.payload))
        except Exception:
            logger.exception("MQTT message callback failed for topic=%s", message.topic)

    def set_message_callback(
        self, callback: Callable[[str, bytes], None] | None
    ) -> None:
        self._on_message_cb = callback

    def connect(self) -> None:
        self._client.connect(
            self._config.mqtt_broker_host,
            self._config.mqtt_broker_port,
            keepalive=60,
        )
        self._client.loop_start()

    def disconnect(self) -> None:
        self._client.loop_stop()
        try:
            self._client.disconnect()
        except Exception:
            pass

    def publish(
        self, topic: str, payload: str, *, retain: bool = False
    ) -> MQTTMessageInfo:
        return self._client.publish(topic, payload, qos=0, retain=retain)

    def wait_for_retained(self, topic: str, timeout_sec: float) -> bytes | None:
        """
        Subscribe to a topic and wait for a retained payload.

        Returns payload bytes when retained data is received, otherwise ``None``.
        """
        ev = threading.Event()
        with self._retained_waiters_lock:
            self._retained_waiters[topic] = (ev, None)
        result, _mid = self._client.subscribe(topic, qos=0)
        if result != 0:
            with self._retained_waiters_lock:
                self._retained_waiters.pop(topic, None)
            logger.warning("MQTT subscribe failed topic=%s rc=%s", topic, result)
            return None
        try:
            if not ev.wait(timeout=max(0.0, float(timeout_sec))):
                return None
            with self._retained_waiters_lock:
                _ev, payload = self._retained_waiters.get(topic, (ev, None))
            return payload
        finally:
            with self._retained_waiters_lock:
                self._retained_waiters.pop(topic, None)
            self._client.unsubscribe(topic)
