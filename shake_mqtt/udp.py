"""UDP listener for Raspberry Shake datagrams."""

from __future__ import annotations

import logging
import socket
import threading
from collections.abc import Callable
from typing import Tuple

from .config import BridgeConfig

logger = logging.getLogger(__name__)

Address = Tuple[str, int]
DatagramHandler = Callable[[bytes, Address], None]


class UdpListener:
    def __init__(self, config: BridgeConfig) -> None:
        self._config = config
        self._sock: socket.socket | None = None

    def bind(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1.0)
        sock.bind((self._config.shake_udp_bind_host, self._config.shake_udp_port))
        self._sock = sock
        logger.info(
            "UDP listening on %s:%s",
            self._config.shake_udp_bind_host,
            self._config.shake_udp_port,
        )

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def run_forever(self, on_datagram: DatagramHandler, stop_event: threading.Event) -> None:
        if self._sock is None:
            raise RuntimeError("UdpListener.bind() must be called before run_forever")
        sock = self._sock
        bufsize = self._config.shake_udp_recv_bufsize
        while not stop_event.is_set():
            try:
                data, addr = sock.recvfrom(bufsize)
            except TimeoutError:
                continue
            except OSError as e:
                if stop_event.is_set():
                    break
                logger.error("UDP recv error: %s", e)
                continue
            on_datagram(data, addr)
