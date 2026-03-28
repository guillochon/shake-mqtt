"""Optional USGS FDSN event query to label triggers with nearby catalog earthquakes."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

_JSON_SEPARATORS = (",", ":")


def fetch_usgs_nearby_events(
    start_iso: str,
    end_iso: str,
    latitude: float,
    longitude: float,
    max_radius_km: float,
    timeout_sec: float = 8.0,
) -> list[dict]:
    """
    Query USGS FDSN event service (GeoJSON). Returns list of lightweight dicts for MQTT.
    """
    q = urllib.parse.urlencode(
        {
            "format": "geojson",
            "starttime": start_iso,
            "endtime": end_iso,
            "latitude": str(latitude),
            "longitude": str(longitude),
            "maxradiuskm": str(max_radius_km),
            "orderby": "time-asc",
        }
    )
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?{q}"
    req = urllib.request.Request(url, headers={"User-Agent": "shake-mqtt/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8")
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        logger.warning("Catalog USGS request failed: %s", e)
        return []

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Catalog USGS response not JSON")
        return []

    feats = data.get("features")
    if not isinstance(feats, list):
        return []

    out: list[dict] = []
    for f in feats:
        if not isinstance(f, dict):
            continue
        props = f.get("properties")
        if not isinstance(props, dict):
            continue
        mag = props.get("mag")
        place = props.get("place")
        et = props.get("time")
        detail = props.get("url")
        out.append(
            {
                "magnitude": float(mag) if mag is not None else None,
                "place": str(place) if place is not None else None,
                "event_time_ms": int(et) if et is not None else None,
                "url": str(detail) if detail is not None else None,
            }
        )
    return out


def catalog_followup_payload(trigger: dict, matches: list[dict]) -> str:
    return json.dumps(
        {
            "kind": "catalog",
            "ref": {
                "channel": trigger.get("channel"),
                "trigger_time": trigger.get("time"),
            },
            "matches": matches,
        },
        separators=_JSON_SEPARATORS,
        ensure_ascii=False,
    )


def trigger_time_to_iso_window(
    trigger_time_unix: float,
    before_sec: float,
    after_sec: float,
) -> tuple[str, str]:
    """UTC ISO8601 strings for USGS start/end (second precision)."""
    t = datetime.fromtimestamp(trigger_time_unix, tz=UTC)
    start = t - timedelta(seconds=before_sec)
    end = t + timedelta(seconds=after_sec)
    fmt = "%Y-%m-%dT%H:%M:%S"
    return start.strftime(fmt), end.strftime(fmt)
