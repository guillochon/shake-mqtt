"""Configuration loaded from environment (Docker Compose–friendly)."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass


def _env_str(key: str, default: str) -> str:
    val = os.environ.get(key)
    return val.strip() if val is not None and val.strip() != "" else default


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return int(raw.strip(), 10)


def _env_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return float(raw.strip())


def _env_optional_float(key: str) -> float | None:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return None
    return float(raw.strip())


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _parse_catalog_query_offsets_sec(raw: str) -> tuple[float, ...]:
    """Comma-separated non-negative delays (seconds) from trigger until each USGS query."""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        raise ValueError(
            "SHAKE_CATALOG_QUERY_OFFSETS_SEC must list at least one number"
        )
    out: list[float] = []
    for p in parts:
        v = float(p)
        if math.isnan(v) or math.isinf(v) or v < 0:
            raise ValueError(
                f"SHAKE_CATALOG_QUERY_OFFSETS_SEC values must be finite and >= 0, got {p!r}"
            )
        out.append(v)
    if len(out) > 12:
        raise ValueError("SHAKE_CATALOG_QUERY_OFFSETS_SEC supports at most 12 offsets")
    return tuple(out)


def _catalog_query_offsets_from_env() -> tuple[float, ...]:
    raw = os.environ.get("SHAKE_CATALOG_QUERY_OFFSETS_SEC")
    if raw is not None and raw.strip() != "":
        return _parse_catalog_query_offsets_sec(raw)
    return (60.0, 120.0, 180.0)


def _parse_channel_allowlist(raw: str) -> frozenset[str] | None:
    """None means all channels; otherwise only listed channel codes match."""
    s = raw.strip()
    if not s:
        return None
    parts = {p.strip() for p in s.split(",") if p.strip()}
    return frozenset(parts) if parts else None


@dataclass(frozen=True)
class BridgeConfig:
    shake_udp_bind_host: str
    shake_udp_port: int
    shake_udp_recv_bufsize: int
    mqtt_broker_host: str
    mqtt_broker_port: int
    mqtt_topic: str
    mqtt_client_id: str
    mqtt_username: str | None
    mqtt_password: str | None
    mqtt_tls: bool
    log_level: str
    json_structured: bool
    detect_events: bool
    detect_sta_sec: float
    detect_lta_sec: float
    detect_ratio_on: float
    detect_ratio_off: float
    detect_cooldown_sec: float
    detect_default_fs_hz: float
    detect_channel_allowlist: frozenset[str] | None
    detect_startup_grace_sec: float
    catalog_enable: bool
    catalog_latitude: float | None
    catalog_longitude: float | None
    catalog_max_radius_km: float
    catalog_time_before_sec: float
    catalog_time_after_sec: float
    catalog_query_offsets_sec: tuple[float, ...]
    catalog_use_traveltime: bool
    catalog_traveltime_model: str
    catalog_traveltime_timeout_sec: float
    catalog_traveltime_max_workers: int
    match_history_enable: bool
    match_history_window_hours: float
    match_history_max_entries: int
    shakenet_window_base_url: str
    shakenet_window_before_sec: float
    shakenet_window_after_sec: float

    @classmethod
    def from_env(cls) -> BridgeConfig:
        user = _env_str("MQTT_USERNAME", "")
        pwd = _env_str("MQTT_PASSWORD", "")
        return cls(
            shake_udp_bind_host=_env_str("SHAKE_UDP_BIND_HOST", "0.0.0.0"),
            shake_udp_port=_env_int("SHAKE_UDP_PORT", 8888),
            shake_udp_recv_bufsize=_env_int("SHAKE_UDP_RECV_BUFSIZE", 2048),
            mqtt_broker_host=_env_str("MQTT_BROKER_HOST", "localhost"),
            mqtt_broker_port=_env_int("MQTT_BROKER_PORT", 1883),
            mqtt_topic=_env_str("MQTT_TOPIC", "home/seismic/shake"),
            mqtt_client_id=_env_str("MQTT_CLIENT_ID", "shake-bridge"),
            mqtt_username=user if user else None,
            mqtt_password=pwd if pwd else None,
            mqtt_tls=_env_bool("MQTT_TLS", False),
            log_level=_env_str("LOG_LEVEL", "INFO").upper(),
            json_structured=_env_bool("SHAKE_JSON_STRUCTURED", False),
            detect_events=_env_bool("SHAKE_DETECT_EVENTS", False),
            detect_sta_sec=_env_float("SHAKE_DETECT_STA_SEC", 1.0),
            detect_lta_sec=_env_float("SHAKE_DETECT_LTA_SEC", 30.0),
            detect_ratio_on=_env_float("SHAKE_DETECT_RATIO_ON", 4.0),
            detect_ratio_off=_env_float("SHAKE_DETECT_RATIO_OFF", 2.0),
            detect_cooldown_sec=_env_float("SHAKE_DETECT_COOLDOWN_SEC", 5.0),
            detect_default_fs_hz=_env_float("SHAKE_DETECT_DEFAULT_FS_HZ", 100.0),
            detect_channel_allowlist=_parse_channel_allowlist(
                _env_str("SHAKE_DETECT_CHANNELS", "")
            ),
            detect_startup_grace_sec=_env_float("SHAKE_DETECT_STARTUP_GRACE_SEC", 5.0),
            catalog_enable=_env_bool("SHAKE_CATALOG_ENABLE", False),
            catalog_latitude=_env_optional_float("SHAKE_CATALOG_LATITUDE"),
            catalog_longitude=_env_optional_float("SHAKE_CATALOG_LONGITUDE"),
            catalog_max_radius_km=_env_float("SHAKE_CATALOG_MAX_RADIUS_KM", 500.0),
            catalog_time_before_sec=_env_float("SHAKE_CATALOG_TIME_BEFORE_SEC", 120.0),
            catalog_time_after_sec=_env_float("SHAKE_CATALOG_TIME_AFTER_SEC", 60.0),
            catalog_query_offsets_sec=_catalog_query_offsets_from_env(),
            catalog_use_traveltime=_env_bool("SHAKE_CATALOG_USE_TRAVELTIME", True),
            catalog_traveltime_model=_env_str(
                "SHAKE_CATALOG_TRAVELTIME_MODEL", "iasp91"
            ),
            catalog_traveltime_timeout_sec=_env_float(
                "SHAKE_CATALOG_TRAVELTIME_TIMEOUT_SEC", 6.0
            ),
            catalog_traveltime_max_workers=_env_int(
                "SHAKE_CATALOG_TRAVELTIME_MAX_WORKERS", 6
            ),
            match_history_enable=_env_bool("SHAKE_MATCH_HISTORY_ENABLE", True),
            match_history_window_hours=_env_float(
                "SHAKE_MATCH_HISTORY_WINDOW_HOURS", 24.0
            ),
            match_history_max_entries=_env_int("SHAKE_MATCH_HISTORY_MAX_ENTRIES", 200),
            shakenet_window_base_url=_env_str(
                "SHAKE_SHAKENET_WINDOW_BASE_URL",
                "https://quakelink.raspberryshake.org/events/query",
            ),
            shakenet_window_before_sec=_env_float(
                "SHAKE_SHAKENET_WINDOW_BEFORE_SEC", 60.0
            ),
            shakenet_window_after_sec=_env_float(
                "SHAKE_SHAKENET_WINDOW_AFTER_SEC", 180.0
            ),
        )

    def validate(self) -> None:
        for name, port in (
            ("SHAKE_UDP_PORT", self.shake_udp_port),
            ("MQTT_BROKER_PORT", self.mqtt_broker_port),
        ):
            if not (1 <= port <= 65535):
                raise ValueError(f"{name} must be 1–65535, got {port}")
        if self.shake_udp_recv_bufsize < 1:
            raise ValueError(
                f"SHAKE_UDP_RECV_BUFSIZE must be >= 1, got {self.shake_udp_recv_bufsize}"
            )
        if self.match_history_window_hours <= 0:
            raise ValueError(
                "SHAKE_MATCH_HISTORY_WINDOW_HOURS must be > 0, "
                f"got {self.match_history_window_hours}"
            )
        if self.match_history_max_entries < 1:
            raise ValueError(
                "SHAKE_MATCH_HISTORY_MAX_ENTRIES must be >= 1, "
                f"got {self.match_history_max_entries}"
            )
        if self.shakenet_window_before_sec < 0:
            raise ValueError("SHAKE_SHAKENET_WINDOW_BEFORE_SEC must be >= 0")
        if self.shakenet_window_after_sec < 0:
            raise ValueError("SHAKE_SHAKENET_WINDOW_AFTER_SEC must be >= 0")
        if self.detect_events:
            if self.detect_sta_sec <= 0:
                raise ValueError("SHAKE_DETECT_STA_SEC must be > 0")
            if self.detect_lta_sec <= 0:
                raise ValueError("SHAKE_DETECT_LTA_SEC must be > 0")
            if self.detect_ratio_on <= 0:
                raise ValueError("SHAKE_DETECT_RATIO_ON must be > 0")
            if self.detect_ratio_off <= 0:
                raise ValueError("SHAKE_DETECT_RATIO_OFF must be > 0")
            if self.detect_ratio_off >= self.detect_ratio_on:
                raise ValueError(
                    "SHAKE_DETECT_RATIO_OFF must be < SHAKE_DETECT_RATIO_ON (hysteresis)"
                )
            if self.detect_cooldown_sec < 0:
                raise ValueError("SHAKE_DETECT_COOLDOWN_SEC must be >= 0")
            if self.detect_default_fs_hz <= 0:
                raise ValueError("SHAKE_DETECT_DEFAULT_FS_HZ must be > 0")
            if self.detect_startup_grace_sec < 0:
                raise ValueError("SHAKE_DETECT_STARTUP_GRACE_SEC must be >= 0")
        if self.catalog_enable:
            if self.catalog_latitude is None or self.catalog_longitude is None:
                raise ValueError(
                    "SHAKE_CATALOG_ENABLE requires SHAKE_CATALOG_LATITUDE and SHAKE_CATALOG_LONGITUDE"
                )
            if not (-90.0 <= self.catalog_latitude <= 90.0):
                raise ValueError("SHAKE_CATALOG_LATITUDE must be between -90 and 90")
            if not (-180.0 <= self.catalog_longitude <= 180.0):
                raise ValueError("SHAKE_CATALOG_LONGITUDE must be between -180 and 180")
            if self.catalog_max_radius_km <= 0:
                raise ValueError("SHAKE_CATALOG_MAX_RADIUS_KM must be > 0")
            if not self.catalog_query_offsets_sec:
                raise ValueError("SHAKE_CATALOG_QUERY_OFFSETS_SEC must not be empty")
            if self.catalog_use_traveltime:
                allowed_models = frozenset({"iasp91", "prem", "ak135"})
                if self.catalog_traveltime_model not in allowed_models:
                    raise ValueError(
                        "SHAKE_CATALOG_TRAVELTIME_MODEL must be one of "
                        f"{sorted(allowed_models)}, got {self.catalog_traveltime_model!r}"
                    )
                if self.catalog_traveltime_timeout_sec <= 0:
                    raise ValueError("SHAKE_CATALOG_TRAVELTIME_TIMEOUT_SEC must be > 0")
                if self.catalog_traveltime_max_workers < 1:
                    raise ValueError(
                        "SHAKE_CATALOG_TRAVELTIME_MAX_WORKERS must be >= 1"
                    )

    def mqtt_topic_json(self) -> str:
        """`MQTT_TOPIC` + `/json` (canonical JSON when parse succeeds)."""
        return f"{self.mqtt_topic.rstrip('/')}/json"

    def mqtt_topic_event(self) -> str:
        """Prefix for STA/LTA leaf topics: ``{base}/event/<field>`` (not a single JSON topic)."""
        return f"{self.mqtt_topic.rstrip('/')}/event"

    def mqtt_topic_match(self) -> str:
        """Prefix for catalog match leaf topics: ``{base}/match/<field>``."""
        return f"{self.mqtt_topic.rstrip('/')}/match"

    def mqtt_topic_match_history_json(self) -> str:
        """Retained JSON array of recent matches: ``{base}/match/history_json``."""
        return f"{self.mqtt_topic.rstrip('/')}/match/history_json"
