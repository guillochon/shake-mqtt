"""Recursive STA/LTA-style energy ratio for Raspberry Shake UDP samples."""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, field

from .config import BridgeConfig

logger = logging.getLogger(__name__)

_JSON_SEPARATORS = (",", ":")


@dataclass
class _ChannelState:
    fs: float | None = None
    last_packet_t: float | None = None
    dc_ema: float = 0.0
    sta_sq: float = 0.0
    lta_sq: float = 0.0
    initialized: bool = False
    triggered: bool = False
    cooldown_until_mono: float = 0.0


@dataclass
class StaLtaDetector:
    """Per-channel STA/LTA on squared AC samples (deviation from slow DC); emits trigger/reset JSON."""

    config: BridgeConfig
    _channels: dict[str, _ChannelState] = field(default_factory=dict)
    _started_mono: float = field(default_factory=time.monotonic)

    def feed(self, channel: str, packet_t: float, samples: list[int]) -> list[str]:
        if not samples:
            return []
        if not self._channel_allowed(channel):
            return []

        st = self._channels.setdefault(channel, _ChannelState())
        self._update_fs(st, packet_t, len(samples))
        dt = 1.0 / st.fs if st.fs and st.fs > 0 else 0.01

        events: list[str] = []
        sta_sec = max(self.config.detect_sta_sec, 1e-6)
        lta_sec = max(self.config.detect_lta_sec, 1e-6)
        c_sta = math.exp(-dt / sta_sec)
        c_lta = math.exp(-dt / lta_sec)
        on = self.config.detect_ratio_on
        off = self.config.detect_ratio_off
        eps = 1e-12

        for i, raw in enumerate(samples):
            sample_t = packet_t + i * dt
            x = float(raw)
            if not st.initialized:
                st.dc_ema = x
                st.sta_sq = 1e-18
                st.lta_sq = 1e-18
                st.initialized = True
                continue

            # Deviation from slow DC (same LTA time constant as energy) → AC energy, not raw count scale.
            dev = x - st.dc_ema
            e = dev * dev
            st.sta_sq = c_sta * st.sta_sq + (1.0 - c_sta) * e
            st.lta_sq = c_lta * st.lta_sq + (1.0 - c_lta) * e
            st.dc_ema = c_lta * st.dc_ema + (1.0 - c_lta) * x

            sta_rms = math.sqrt(max(st.sta_sq, 0.0))
            lta_rms = math.sqrt(max(st.lta_sq, eps))
            ratio = sta_rms / lta_rms

            now_mono = time.monotonic()
            if not st.triggered:
                armed = (now_mono - self._started_mono) >= self.config.detect_startup_grace_sec
                if ratio >= on and now_mono >= st.cooldown_until_mono and armed:
                    st.triggered = True
                    t_ev = round(sample_t, 6)
                    r_ev = round(ratio, 4)
                    sr_ev = round(sta_rms, 4)
                    lr_ev = round(lta_rms, 4)
                    logger.info(
                        "STA/LTA trigger channel=%s time=%s ratio=%s sta_rms=%s lta_rms=%s",
                        channel,
                        t_ev,
                        r_ev,
                        sr_ev,
                        lr_ev,
                    )
                    events.append(
                        json.dumps(
                            {
                                "kind": "trigger",
                                "channel": channel,
                                "time": t_ev,
                                "ratio": r_ev,
                                "sta_rms": sr_ev,
                                "lta_rms": lr_ev,
                            },
                            separators=_JSON_SEPARATORS,
                            ensure_ascii=False,
                        )
                    )
            elif ratio <= off:
                st.triggered = False
                st.cooldown_until_mono = now_mono + max(0.0, self.config.detect_cooldown_sec)
                events.append(
                    json.dumps(
                        {
                            "kind": "reset",
                            "channel": channel,
                            "time": round(sample_t, 6),
                            "ratio": round(ratio, 4),
                            "sta_rms": round(sta_rms, 4),
                            "lta_rms": round(lta_rms, 4),
                        },
                        separators=_JSON_SEPARATORS,
                        ensure_ascii=False,
                    )
                )

        return events

    def current_rms(self, channel: str) -> tuple[float, float] | None:
        """RMS of short/long smoothed |AC|² (same definition as trigger JSON; comparable to packet_rms scale)."""
        st = self._channels.get(channel)
        if st is None or not st.initialized:
            return None
        eps = 1e-12
        sta_rms = math.sqrt(max(st.sta_sq, 0.0))
        lta_rms = math.sqrt(max(st.lta_sq, eps))
        return (round(sta_rms, 4), round(lta_rms, 4))

    def _channel_allowed(self, channel: str) -> bool:
        allowed = self.config.detect_channel_allowlist
        if allowed is None:
            return True
        return channel in allowed

    def _update_fs(self, st: _ChannelState, packet_t: float, n: int) -> None:
        if n <= 0:
            return
        if st.last_packet_t is not None:
            dt_pkt = packet_t - st.last_packet_t
            if dt_pkt > 1e-6:
                inferred = n / dt_pkt
                if 1.0 <= inferred <= 1000.0:
                    st.fs = inferred
        st.last_packet_t = packet_t
        if st.fs is None:
            st.fs = max(1.0, self.config.detect_default_fs_hz)
