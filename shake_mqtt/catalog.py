"""Optional USGS FDSN event query to label triggers with nearby catalog earthquakes."""

from __future__ import annotations

import json
import logging
import math
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

_IRIS_TRAVELTIME_BASE = "https://service.earthscope.org/irisws/traveltime/1/query"

# Great-circle distance (WGS84 sphere).
_EARTH_RADIUS_KM = 6371.0
_KM_PER_MI = 1.609344

# Log–log sensitivity: default anchors (M=2, 50 mi) and (M=4, 300 mi). An optional
# magnitude offset shifts both anchor magnitudes together (e.g. -1 → M1 @ 50 mi).
_SENS_ANCHOR_MAG_LOW = 2.0
_SENS_ANCHOR_MAG_HIGH = 4.0
_SENS_ANCHOR_MI_LOW = 50.0
_SENS_ANCHOR_MI_HIGH = 300.0


def sensitivity_coefficients(magnitude_offset: float) -> tuple[float, float]:
    """
    Return ``(a, b)`` for ``log10(D_mi) = a*M + b``.

    Anchor magnitudes are ``(2 + offset)`` and ``(4 + offset)`` at 50 mi and 300 mi.
    """
    m_lo = _SENS_ANCHOR_MAG_LOW + magnitude_offset
    m_hi = _SENS_ANCHOR_MAG_HIGH + magnitude_offset
    d_mag = m_hi - m_lo
    if d_mag == 0.0:
        raise ValueError("Sensitivity anchor magnitude span is zero")
    a = (math.log10(_SENS_ANCHOR_MI_HIGH) - math.log10(_SENS_ANCHOR_MI_LOW)) / d_mag
    b = math.log10(_SENS_ANCHOR_MI_LOW) - a * m_lo
    return a, b


def haversine_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float | None:
    """Great-circle distance in km; ``None`` if inputs are invalid."""
    try:
        p1 = math.radians(lat1)
        p2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlmb = math.radians(lon2 - lon1)
        a = (
            math.sin(dphi / 2) ** 2
            + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
        return _EARTH_RADIUS_KM * c
    except (TypeError, ValueError):
        return None


def max_distance_miles_for_magnitude(
    magnitude: float,
    *,
    magnitude_offset: float = 0.0,
) -> float:
    """Maximum epicenter distance (mi) at which ``magnitude`` is considered detectable."""
    a, b = sensitivity_coefficients(magnitude_offset)
    return 10.0 ** (a * magnitude + b)


def min_magnitude_for_distance_miles(
    distance_mi: float,
    *,
    magnitude_offset: float = 0.0,
) -> float | None:
    """Inverse sensitivity: minimum magnitude (same curve) needed at ``distance_mi``."""
    if distance_mi <= 0.0 or math.isnan(distance_mi) or math.isinf(distance_mi):
        return None
    a, b = sensitivity_coefficients(magnitude_offset)
    return (math.log10(distance_mi) - b) / a


def event_passes_sensitivity_heuristic(
    m: dict,
    sta_lat: float,
    sta_lon: float,
    *,
    magnitude_offset: float = 0.0,
) -> bool:
    """
    Drop candidates that are too weak for their distance.

    Events with **no magnitude** are kept (USGS sometimes omits mag). If epicenter
    coordinates are missing, the event is kept.
    """
    mag = m.get("magnitude")
    if mag is None:
        return True
    try:
        mag_f = float(mag)
    except (TypeError, ValueError):
        return True
    ev_lat = m.get("ev_latitude")
    ev_lon = m.get("ev_longitude")
    if not isinstance(ev_lat, (int, float)) or not isinstance(ev_lon, (int, float)):
        return True
    d_km = haversine_distance_km(float(ev_lat), float(ev_lon), sta_lat, sta_lon)
    if d_km is None:
        return True
    d_mi = d_km / _KM_PER_MI
    return d_mi <= max_distance_miles_for_magnitude(
        mag_f, magnitude_offset=magnitude_offset
    )


def filter_matches_by_sensitivity(
    matches: list[dict],
    sta_lat: float,
    sta_lon: float,
    *,
    enabled: bool,
    magnitude_offset: float = 0.0,
) -> list[dict]:
    """Apply :func:`event_passes_sensitivity_heuristic` to each match when ``enabled``."""
    if not enabled:
        return list(matches)
    return [
        m
        for m in matches
        if event_passes_sensitivity_heuristic(
            m, sta_lat, sta_lon, magnitude_offset=magnitude_offset
        )
    ]


def enrich_match_distance_and_delta(
    sta_lat: float,
    sta_lon: float,
    m: dict,
    *,
    magnitude_offset: float = 0.0,
) -> dict:
    """
    Copy ``m`` and add ``distance_mi`` and ``delta_magnitude``.

    ``delta_magnitude`` is ``magnitude - M_min(distance)`` on the same log–log
    sensitivity curve (e.g. M5 at 300 mi → about +1 because M≈4 at 300 mi).
    Either field is ``None`` when coordinates or magnitude are unavailable.
    """
    row = dict(m)
    ev_lat = m.get("ev_latitude")
    ev_lon = m.get("ev_longitude")
    if not isinstance(ev_lat, (int, float)) or not isinstance(ev_lon, (int, float)):
        row["distance_mi"] = None
        row["delta_magnitude"] = None
        return row
    d_km = haversine_distance_km(float(ev_lat), float(ev_lon), sta_lat, sta_lon)
    if d_km is None:
        row["distance_mi"] = None
        row["delta_magnitude"] = None
        return row
    d_mi = d_km / _KM_PER_MI
    row["distance_mi"] = round(d_mi, 2)
    mag = m.get("magnitude")
    if mag is None:
        row["delta_magnitude"] = None
        return row
    try:
        mag_f = float(mag)
    except (TypeError, ValueError):
        row["delta_magnitude"] = None
        return row
    m_min = min_magnitude_for_distance_miles(d_mi, magnitude_offset=magnitude_offset)
    if m_min is None:
        row["delta_magnitude"] = None
        return row
    row["delta_magnitude"] = round(mag_f - m_min, 3)
    return row


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
        ev_lat: float | None = None
        ev_lon: float | None = None
        ev_depth_km: float | None = None
        geom = f.get("geometry")
        if isinstance(geom, dict) and geom.get("type") == "Point":
            coords = geom.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                try:
                    ev_lon = float(coords[0])
                    ev_lat = float(coords[1])
                    if len(coords) >= 3:
                        ev_depth_km = float(coords[2])
                except (TypeError, ValueError):
                    pass
        out.append(
            {
                "magnitude": float(mag) if mag is not None else None,
                "place": str(place) if place is not None else None,
                "event_time_ms": int(et) if et is not None else None,
                "url": str(detail) if detail is not None else None,
                "ev_latitude": ev_lat,
                "ev_longitude": ev_lon,
                "ev_depth_km": ev_depth_km,
            }
        )
    return out


def pick_closest_match_by_time(
    trigger_time_unix: float, matches: list[dict]
) -> dict | None:
    """USGS event whose origin time is closest to the trigger (requires ``event_time_ms``)."""
    trigger_ms = trigger_time_unix * 1000.0
    best: dict | None = None
    best_abs_dt_ms: float | None = None
    for m in matches:
        et = m.get("event_time_ms")
        if et is None:
            continue
        try:
            dt = abs(float(et) - trigger_ms)
        except (TypeError, ValueError):
            continue
        if best is None or dt < best_abs_dt_ms:
            best = m
            best_abs_dt_ms = dt
    return best


def fetch_iris_first_arrival_travel_sec(
    ev_lat: float,
    ev_lon: float,
    ev_depth_km: float,
    sta_lat: float,
    sta_lon: float,
    *,
    model: str = "iasp91",
    timeout_sec: float = 8.0,
) -> float | None:
    """
    First-arriving phase travel time (seconds) from hypocenter to station via
    IRIS/EQScope traveltime web service (1-D earth model). USGS catalog does not
    expose per-station travel times; this is the standard HTTP alternative.
    """
    q = urllib.parse.urlencode(
        {
            "staloc": f"[{sta_lat},{sta_lon}]",
            "evloc": f"[{ev_lat},{ev_lon}]",
            "evdepth": str(max(0.0, ev_depth_km)),
            "format": "json",
            "mintimeonly": "true",
            "model": model,
        }
    )
    url = f"{_IRIS_TRAVELTIME_BASE}?{q}"
    req = urllib.request.Request(url, headers={"User-Agent": "shake-mqtt/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("IRIS traveltime HTTP %s: %s", e.code, e.reason)
        return None
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        logger.warning("IRIS traveltime request failed: %s", e)
        return None

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("IRIS traveltime response not JSON")
        return None

    arrivals = data.get("arrivals")
    if not isinstance(arrivals, list) or not arrivals:
        return None
    times: list[float] = []
    for a in arrivals:
        if not isinstance(a, dict):
            continue
        t = a.get("time")
        if t is None:
            continue
        try:
            times.append(float(t))
        except (TypeError, ValueError):
            continue
    if not times:
        return None
    return min(times)


def _score_match_for_pick(
    trigger_time_unix: float,
    sta_lat: float,
    sta_lon: float,
    m: dict,
    *,
    use_traveltime: bool,
    traveltime_model: str,
    traveltime_timeout_sec: float,
) -> tuple[float, dict]:
    """Return (abs_error_seconds, enriched match row)."""
    et = m.get("event_time_ms")
    if et is None:
        row = dict(m)
        row["match_mode"] = "origin_time"
        return (float("inf"), row)
    try:
        origin_unix = float(et) / 1000.0
    except (TypeError, ValueError):
        row = dict(m)
        row["match_mode"] = "origin_time"
        return (float("inf"), row)

    ev_lat = m.get("ev_latitude")
    ev_lon = m.get("ev_longitude")
    depth = m.get("ev_depth_km")
    dep = float(depth) if depth is not None else 0.0

    if (
        use_traveltime
        and isinstance(ev_lat, (int, float))
        and isinstance(ev_lon, (int, float))
    ):
        tt = fetch_iris_first_arrival_travel_sec(
            float(ev_lat),
            float(ev_lon),
            dep,
            sta_lat,
            sta_lon,
            model=traveltime_model,
            timeout_sec=traveltime_timeout_sec,
        )
        if tt is not None:
            predicted = origin_unix + tt
            err = abs(trigger_time_unix - predicted)
            row = dict(m)
            row["travel_sec"] = round(tt, 4)
            row["predicted_arrival_unix"] = round(predicted, 6)
            row["match_mode"] = "traveltime"
            return (err, row)

    err = abs(trigger_time_unix - origin_unix)
    row = dict(m)
    row["match_mode"] = "origin_time"
    return (err, row)


def pick_closest_catalog_match(
    trigger_time_unix: float,
    sta_lat: float,
    sta_lon: float,
    matches: list[dict],
    *,
    use_traveltime: bool,
    traveltime_model: str,
    traveltime_timeout_sec: float,
    traveltime_max_workers: int,
) -> dict | None:
    """
    Choose the catalog event that best explains the trigger time at the station.
    With travel time: minimize |trigger - (origin + first_arrival)| using IRIS.
    Without (or when hypocenter / IRIS unavailable): same as origin-time closeness.
    """
    if not matches:
        return None
    if not use_traveltime:
        best = pick_closest_match_by_time(trigger_time_unix, matches)
        if best is None:
            return None
        row = dict(best)
        row["match_mode"] = "origin_time"
        return row

    workers = max(1, min(traveltime_max_workers, len(matches)))
    best_err = float("inf")
    best_row: dict | None = None
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [
            ex.submit(
                _score_match_for_pick,
                trigger_time_unix,
                sta_lat,
                sta_lon,
                m,
                use_traveltime=True,
                traveltime_model=traveltime_model,
                traveltime_timeout_sec=traveltime_timeout_sec,
            )
            for m in matches
        ]
        for f in as_completed(futs):
            err, row = f.result()
            if err < best_err:
                best_err = err
                best_row = row
    return best_row


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
