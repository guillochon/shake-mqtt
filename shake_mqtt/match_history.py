"""Rolling JSON history of catalog match results for MQTT (e.g. Home Assistant tables)."""

from __future__ import annotations

import json
import threading
import time
from typing import Any

from .topic_publish import MATCH_CATALOG_KEYS


def _json_scalar(value: object) -> Any:
    """Normalize values for JSON (mirrors empty-as-absent semantics of leaf topics)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (
            value != value or value in (float("inf"), float("-inf"))
        ):
            return None
        return value
    if isinstance(value, str):
        return value
    return str(value)


def build_match_history_entry(
    trigger: dict[str, object],
    match: dict[str, object] | None,
) -> dict[str, Any]:
    """
    One history row (snake_case, JSON-serializable).

    Catalog fields mirror ``publish_match_result``. **sta_rms** is always taken from
    the trigger so history tables and plots have sensor strength even when
    ``catalog_present`` is 0 (MQTT leaf topics leave ``sta_rms`` empty in that case).
    """
    t_raw = trigger.get("time")
    ref_trigger_time: float | None
    if isinstance(t_raw, (int, float)):
        ref_trigger_time = float(t_raw)
    else:
        ref_trigger_time = None

    entry: dict[str, Any] = {
        "kind": "match",
        "ref_channel": _json_scalar(trigger.get("channel")),
        "ref_trigger_time": ref_trigger_time,
    }

    if match is None:
        entry["catalog_present"] = 0
        for key in MATCH_CATALOG_KEYS:
            if key == "sta_rms":
                continue
            entry[key] = None
        entry["sta_rms"] = _json_scalar(trigger.get("sta_rms"))
        return entry

    entry["catalog_present"] = 1
    sta = trigger.get("sta_rms")
    for key in MATCH_CATALOG_KEYS:
        val = sta if key == "sta_rms" else match.get(key)
        entry[key] = _json_scalar(val)
    return entry


def _sta_rms_sort_key(entry: dict[str, Any]) -> tuple[int, float]:
    """Descending sta_rms; missing or invalid last."""
    s = entry.get("sta_rms")
    if s is None:
        return (1, 0.0)
    try:
        return (0, -float(s))
    except (TypeError, ValueError):
        return (1, 0.0)


def _catalog_event_key(entry: dict[str, Any]) -> tuple[str, str, str] | None:
    """
    Stable key for deduping catalog-present rows that refer to the same event.
    Prefer URL, fall back to event_time_ms + place.
    """
    if entry.get("catalog_present") != 1:
        return None
    url = entry.get("url")
    if isinstance(url, str) and url:
        return ("url", url, "")
    et = entry.get("event_time_ms")
    place = entry.get("place")
    if et is not None:
        return ("time_place", str(et), str(place) if place is not None else "")
    return None


def _dedupe_catalog_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Keep only the highest-sta_rms row for each catalog event key.
    Unmatched rows (catalog_present != 1) are kept as-is.
    """
    best_by_event: dict[tuple[str, str, str], dict[str, Any]] = {}
    out: list[dict[str, Any]] = []
    for e in entries:
        key = _catalog_event_key(e)
        if key is None:
            out.append(e)
            continue
        prev = best_by_event.get(key)
        if prev is None or _sta_rms_sort_key(e) < _sta_rms_sort_key(prev):
            best_by_event[key] = e
    out.extend(best_by_event.values())
    return out


class MatchHistoryBuffer:
    """Thread-safe buffer; prune by trigger time, sort by sta_rms desc, cap length."""

    def __init__(self, *, window_hours: float, max_entries: int) -> None:
        self._window_sec = float(window_hours) * 3600.0
        self._max_entries = max_entries
        self._entries: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def record_and_dumps(
        self,
        trigger: dict[str, object],
        match: dict[str, object] | None,
    ) -> str:
        """Append one match, prune, sort, cap; return retained JSON payload."""
        row = build_match_history_entry(trigger, match)
        now = time.time()
        with self._lock:
            rt = row.get("ref_trigger_time")
            ch = row.get("ref_channel")
            if isinstance(rt, (int, float)):
                rt_f = float(rt)
                self._entries = [
                    e
                    for e in self._entries
                    if not (
                        isinstance(e.get("ref_trigger_time"), (int, float))
                        and float(e["ref_trigger_time"]) == rt_f
                        and e.get("ref_channel") == ch
                    )
                ]
            self._entries.append(row)
            cutoff = now - self._window_sec
            self._entries = [
                e
                for e in self._entries
                if isinstance(e.get("ref_trigger_time"), (int, float))
                and float(e["ref_trigger_time"]) >= cutoff
            ]
            self._entries = _dedupe_catalog_rows(self._entries)
            self._entries.sort(key=_sta_rms_sort_key)
            if len(self._entries) > self._max_entries:
                self._entries = self._entries[: self._max_entries]
            payload = {
                "matches": list(self._entries),
                "updated_unix": round(now, 3),
            }
            return json.dumps(payload, separators=(",", ":"))
