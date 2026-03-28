"""Pluggable datagram processing (normalize, parse, filter)."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from typing import Protocol, Tuple

from .config import BridgeConfig
from .detection import StaLtaDetector

logger = logging.getLogger(__name__)

Address = Tuple[str, int]

_JSON_SEPARATORS = (",", ":")


@dataclass(frozen=True)
class ParsedPacket:
    channel: str
    time: float
    samples: list[int]


@dataclass(frozen=True)
class ProcessResult:
    """MQTT payloads: one optional stream message plus zero or more event messages."""

    json_payload: str | None = None
    event_payloads: tuple[str, ...] = ()


class DatagramProcessor(Protocol):
    def process(self, data: bytes, addr: Address) -> ProcessResult:
        ...


def _brace_list_to_array_text(normalized: str) -> str | None:
    """Turn Shake `{a, b, c}` UDP line into `[a, b, c]` for `json.loads`."""
    s = normalized.strip()
    if len(s) < 2 or s[0] != "{" or s[-1] != "}":
        return None
    inner = s[1:-1].strip()
    return f"[{inner}]"


def parse_shake_packet(data: bytes, addr: Address) -> ParsedPacket | None:
    """Parse Raspberry Shake UDP text into channel, packet start time, and samples."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning("Non-UTF-8 datagram from %s:%s (%d bytes)", addr[0], addr[1], len(data))
        return None
    normalized = text.replace("'", '"')
    array_text = _brace_list_to_array_text(normalized)
    if array_text is None:
        logger.debug("Payload not `{...}` wrapped; skip")
        return None
    try:
        arr = json.loads(array_text)
    except json.JSONDecodeError:
        logger.debug("After `{…}`→`[…]` conversion, payload still not valid JSON")
        return None
    if not isinstance(arr, list) or len(arr) < 3:
        logger.debug("Parsed array too short for channel+time+samples")
        return None
    ch = arr[0]
    if not isinstance(ch, str):
        logger.debug("Channel is not a string")
        return None
    t_raw = arr[1]
    if isinstance(t_raw, bool) or not isinstance(t_raw, (int, float)):
        logger.debug("Timestamp is not numeric")
        return None
    packet_t = float(t_raw)
    samples: list[int] = []
    for x in arr[2:]:
        if isinstance(x, bool) or not isinstance(x, (int, float)):
            logger.debug("Non-numeric sample in packet")
            return None
        if isinstance(x, float) and not x.is_integer():
            logger.debug("Non-integer sample value")
            return None
        samples.append(int(x))
    if not samples:
        logger.debug("No samples in packet")
        return None
    return ParsedPacket(channel=ch, time=packet_t, samples=samples)


def _packet_stats(samples: list[int]) -> tuple[int, float]:
    peak = max(abs(s) for s in samples)
    mean = sum(samples) / len(samples)
    var = sum((s - mean) ** 2 for s in samples) / len(samples)
    rms = math.sqrt(var)
    return peak, rms


def format_packet_json(
    pkt: ParsedPacket,
    structured: bool,
    sta_rms: float | None = None,
    lta_rms: float | None = None,
) -> str:
    if not structured:
        flat: list[object] = [pkt.channel, pkt.time, *pkt.samples]
        if sta_rms is not None and lta_rms is not None:
            flat.extend([sta_rms, lta_rms])
        return json.dumps(flat, separators=_JSON_SEPARATORS, ensure_ascii=False)
    peak, rms = _packet_stats(pkt.samples)
    obj: dict[str, object] = {
        "channel": pkt.channel,
        "time": pkt.time,
        "n_samples": len(pkt.samples),
        "samples": pkt.samples,
        "packet_peak": peak,
        "packet_rms": round(rms, 4),
    }
    if sta_rms is not None and lta_rms is not None:
        obj["sta_rms"] = sta_rms
        obj["lta_rms"] = lta_rms
    return json.dumps(obj, separators=_JSON_SEPARATORS, ensure_ascii=False)


class PassthroughJsonNormalizer:
    """Map Raspberry Shake UDP `{"CH", ts, samples...}` to JSON for MQTT `/json`."""

    def __init__(self, structured: bool = False) -> None:
        self._structured = structured

    def process(self, data: bytes, addr: Address) -> ProcessResult:
        pkt = parse_shake_packet(data, addr)
        if pkt is None:
            return ProcessResult()
        out = format_packet_json(pkt, self._structured)
        logger.debug("Shake UDP → JSON (%s, %d samples)", pkt.channel, len(pkt.samples))
        return ProcessResult(json_payload=out, event_payloads=())


class EventDetectingProcessor:
    """Passthrough `/json` plus STA/LTA triggers on `{MQTT_TOPIC}/event`."""

    def __init__(self, config: BridgeConfig, structured_json: bool) -> None:
        self._structured = structured_json
        self._detector = StaLtaDetector(config)

    def process(self, data: bytes, addr: Address) -> ProcessResult:
        pkt = parse_shake_packet(data, addr)
        if pkt is None:
            return ProcessResult()
        events = self._detector.feed(pkt.channel, pkt.time, pkt.samples)
        stalta = self._detector.current_rms(pkt.channel)
        sta_rms, lta_rms = stalta if stalta is not None else (None, None)
        json_payload = format_packet_json(pkt, self._structured, sta_rms, lta_rms)
        logger.debug(
            "Shake UDP → JSON + %d event(s) (%s)",
            len(events),
            pkt.channel,
        )
        return ProcessResult(json_payload=json_payload, event_payloads=tuple(events))


def build_processor(config: BridgeConfig) -> DatagramProcessor:
    structured = config.json_structured
    if config.detect_events:
        return EventDetectingProcessor(config, structured_json=structured)
    return PassthroughJsonNormalizer(structured=structured)
