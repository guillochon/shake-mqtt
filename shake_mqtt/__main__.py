"""Entry point: `python -m shake_mqtt`."""

from __future__ import annotations

import logging
import signal
import sys

from .bridge import ShakeMqttBridge
from .config import BridgeConfig
from .processing import build_processor


def _configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> int:
    try:
        config = BridgeConfig.from_env()
        config.validate()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1

    _configure_logging(config.log_level)
    bridge = ShakeMqttBridge(config, processor=build_processor(config))

    def handle_signal(signum: int, _frame) -> None:
        logging.getLogger(__name__).info("Received signal %s, stopping…", signum)
        bridge.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        bridge.run()
    except Exception:
        logging.exception("Bridge crashed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
