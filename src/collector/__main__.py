"""Entry point for mesh-mqtt-pg-collector.

Usage:
    python -m collector
    mesh-mqtt-pg-collector          (if installed via pip/uv)
"""

from __future__ import annotations

import logging
import sys

from . import database
from .config import load_config
from .mqtt_handler import run


def main() -> None:
    # Configure logging early so config-load messages are visible
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logger = logging.getLogger("collector")

    config = load_config()

    # Adjust log level from config
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)

    logger.info("mesh-mqtt-pg-collector starting up")
    logger.info(
        "MQTT: %s:%d  topic: %s%s",
        config.mqtt.broker_address,
        config.mqtt.port,
        config.mqtt.topic_prefix,
        config.mqtt.topic_suffix,
    )
    logger.info(
        "PostgreSQL: %s:%d/%s",
        config.postgres.host,
        config.postgres.port,
        config.postgres.dbname,
    )

    channel_count = len(config.channel_keys)
    if channel_count:
        logger.info(
            "Loaded %d channel key(s): %s",
            channel_count,
            ", ".join(ck.name for ck in config.channel_keys),
        )

    # Initialize database
    try:
        database.init_pool(config.postgres)
    except Exception as e:
        logger.error("Failed to initialize database: %s", e)
        raise SystemExit(1)

    # Run MQTT loop (blocks forever)
    try:
        run(config)
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down")
    finally:
        database.close_pool()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
