"""Publish STA/LTA and catalog match data as leaf topics under ``{base}/event/...`` and ``{base}/match/...``."""

from __future__ import annotations

from collections.abc import Callable

PublishFn = Callable[[str, str], None]

# Leaf topic names under ``{MQTT_TOPIC}/event/``
STA_LTA_EVENT_KEYS = ("kind", "channel", "time", "ratio", "sta_rms", "lta_rms")

# Trigger-derived leaf topic names under ``{MQTT_TOPIC}/match/`` (always present).
MATCH_TRIGGER_KEYS = ("ratio", "sta_rms", "lta_rms")

# Leaf topic names under ``{MQTT_TOPIC}/match/`` (catalog row + metadata)
MATCH_CATALOG_KEYS = (
    "magnitude",
    "place",
    "event_time_ms",
    "url",
    "sta_rms",
    "match_mode",
    "travel_sec",
    "predicted_arrival_unix",
)


def mqtt_scalar_str(value: object) -> str:
    """String payload for MQTT (empty string means no value)."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def publish_sta_lta_event(root: str, event: dict[str, object], publish: PublishFn) -> None:
    """
    Publish one STA/LTA event as ``{root}/{key}`` for each known key.
    ``root`` is ``{base}/event`` (no trailing slash).
    """
    for key in STA_LTA_EVENT_KEYS:
        publish(f"{root}/{key}", mqtt_scalar_str(event.get(key)))


def publish_match_result(
    root: str,
    trigger: dict[str, object],
    match: dict[str, object] | None,
    publish: PublishFn,
) -> None:
    """
    Publish catalog match follow-up as leaf topics under ``root`` (``{base}/match``).

    Always sets ``kind``, ``ref/channel``, ``ref/trigger_time``, ``catalog_present``,
    trigger fields (``MATCH_TRIGGER_KEYS``), and every ``MATCH_CATALOG_KEYS`` (empty when no catalog row).
    """
    publish(f"{root}/kind", "match")
    publish(f"{root}/ref/channel", mqtt_scalar_str(trigger.get("channel")))
    publish(f"{root}/ref/trigger_time", mqtt_scalar_str(trigger.get("time")))
    for key in MATCH_TRIGGER_KEYS:
        publish(f"{root}/{key}", mqtt_scalar_str(trigger.get(key)))
    if match is None:
        publish(f"{root}/catalog_present", "0")
        for key in MATCH_CATALOG_KEYS:
            if key in MATCH_TRIGGER_KEYS:
                continue
            publish(f"{root}/{key}", "")
        return

    publish(f"{root}/catalog_present", "1")
    for key in MATCH_CATALOG_KEYS:
        if key in MATCH_TRIGGER_KEYS:
            continue
        publish(f"{root}/{key}", mqtt_scalar_str(match.get(key)))
