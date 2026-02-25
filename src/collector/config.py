"""Configuration loading from YAML file and environment variables."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_ENV_PREFIX = "COLLECTOR_"


@dataclass(slots=True)
class MqttConfig:
    broker_address: str = "127.0.0.1"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    topic_prefix: str = "msh"
    topic_suffix: str = "/+/+/+/#"
    client_id: str = "mesh-mqtt-pg-collector"


@dataclass(slots=True)
class PostgresConfig:
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    dbname: str = "meshtastic"


@dataclass(slots=True)
class ChannelKey:
    """A channel encryption key mapping."""

    name: str  # Channel name (e.g. "LongFast", "Wx")
    key: str  # Base64-encoded key


@dataclass(slots=True)
class AppConfig:
    """Application configuration."""

    mqtt: MqttConfig = field(default_factory=MqttConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)

    # Default key used when no channel-specific key matches.
    # This is the well-known Meshtastic LongFast default key.
    default_key: str = "1PG7OiApB1nwvP+rz05pAQ=="

    # Per-channel keys. Each entry maps a channel name to a base64 key.
    # Example: [{"name": "Wx", "key": "base64encodedkey=="}]
    channel_keys: list[ChannelKey] = field(default_factory=list)

    log_level: str = "INFO"


def _apply_env_overrides(config: AppConfig) -> None:
    """Override config values from environment variables."""
    env_map = {
        "COLLECTOR_MQTT_BROKER": ("mqtt", "broker_address"),
        "COLLECTOR_MQTT_PORT": ("mqtt", "port"),
        "COLLECTOR_MQTT_USERNAME": ("mqtt", "username"),
        "COLLECTOR_MQTT_PASSWORD": ("mqtt", "password"),
        "COLLECTOR_MQTT_TOPIC_PREFIX": ("mqtt", "topic_prefix"),
        "COLLECTOR_MQTT_TOPIC_SUFFIX": ("mqtt", "topic_suffix"),
        "COLLECTOR_MQTT_CLIENT_ID": ("mqtt", "client_id"),
        "COLLECTOR_POSTGRES_HOST": ("postgres", "host"),
        "COLLECTOR_POSTGRES_PORT": ("postgres", "port"),
        "COLLECTOR_POSTGRES_USER": ("postgres", "user"),
        "COLLECTOR_POSTGRES_PASSWORD": ("postgres", "password"),
        "COLLECTOR_POSTGRES_DBNAME": ("postgres", "dbname"),
        "COLLECTOR_DEFAULT_KEY": (None, "default_key"),
        "COLLECTOR_LOG_LEVEL": (None, "log_level"),
    }

    for env_key, (section, attr) in env_map.items():
        value = os.environ.get(env_key)
        if value is None:
            continue

        if section is None:
            target = config
        else:
            target = getattr(config, section)

        current = getattr(target, attr)
        if isinstance(current, int):
            value = int(value)
        elif isinstance(current, bool):
            value = value.lower() in ("1", "true", "yes", "on")

        setattr(target, attr, value)


def load_config(config_path: str | os.PathLike | None = None) -> AppConfig:
    """Load configuration from YAML file, then override with environment variables.

    Precedence: defaults < YAML file < environment variables.
    """
    config = AppConfig()

    # Determine config file path
    yaml_path = Path(
        config_path
        if config_path is not None
        else os.getenv("COLLECTOR_CONFIG_FILE", "config.yaml")
    )

    if yaml_path.is_file():
        try:
            with yaml_path.open("r", encoding="utf-8") as fp:
                data = yaml.safe_load(fp) or {}

            if not isinstance(data, dict):
                logger.warning("Config file %s must be a YAML mapping, ignoring", yaml_path)
                data = {}

            # MQTT section
            if "mqtt" in data and isinstance(data["mqtt"], dict):
                for k, v in data["mqtt"].items():
                    if hasattr(config.mqtt, k):
                        setattr(config.mqtt, k, v)

            # Postgres section
            if "postgres" in data and isinstance(data["postgres"], dict):
                for k, v in data["postgres"].items():
                    if hasattr(config.postgres, k):
                        setattr(config.postgres, k, v)

            # Top-level settings
            if "default_key" in data:
                config.default_key = str(data["default_key"])

            if "log_level" in data:
                config.log_level = str(data["log_level"])

            # Channel keys
            if "channel_keys" in data and isinstance(data["channel_keys"], list):
                for entry in data["channel_keys"]:
                    if isinstance(entry, dict) and "name" in entry and "key" in entry:
                        config.channel_keys.append(
                            ChannelKey(name=str(entry["name"]), key=str(entry["key"]))
                        )

            logger.info("Loaded configuration from %s", yaml_path)
        except Exception as exc:
            logger.warning("Failed to read config from %s: %s", yaml_path, exc)
    else:
        logger.info("No config file found at %s, using defaults + env vars", yaml_path)

    # Environment variables override everything
    _apply_env_overrides(config)

    return config
