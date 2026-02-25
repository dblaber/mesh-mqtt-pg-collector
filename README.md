# mesh-mqtt-pg-collector

A lightweight Meshtastic MQTT to PostgreSQL collector. Listens to an MQTT broker, decrypts mesh packets, and stores packet history and node information directly into PostgreSQL.

## Features

- Subscribes to Meshtastic MQTT topics and processes protobuf ServiceEnvelopes
- Decrypts encrypted packets using AES-256-CTR (same algorithm as the Meshtastic firmware)
- Supports multiple channel encryption keys (LongFast default + custom per-channel keys)
- Stores full packet history in `packet_history` table
- Tracks node info (name, hardware, role, etc.) in `node_info` table
- Deduplicates packets via `(mesh_packet_id, gateway_id)` unique constraint
- Runs in Docker with docker-compose (PostgreSQL included or bring your own)

## Quick Start

```bash
# Clone the repo
git clone https://github.com/mesh-mqtt-pg-collector/mesh-mqtt-pg-collector.git
cd mesh-mqtt-pg-collector

# Copy and edit the config
cp config.yaml.example config.yaml
# Edit config.yaml with your MQTT broker details, PostgreSQL credentials, and channel keys
```

### With an existing PostgreSQL instance

```bash
# Just the collector — configure postgres connection in config.yaml
docker compose up -d
```

### With a bundled PostgreSQL instance

```bash
# Collector + a local PostgreSQL container
docker compose -f docker-compose.yml -f docker-compose.postgres.yml up -d
```

## Configuration

Configuration is loaded from `config.yaml` (or the path in `COLLECTOR_CONFIG_FILE` env var), with environment variables taking precedence.

See [config.yaml.example](config.yaml.example) for all available options and their corresponding environment variable names.

### Channel Keys

The default Meshtastic LongFast key (`1PG7OiApB1nwvP+rz05pAQ==`) is built-in. To decrypt traffic on other channels, add their keys:

```yaml
channel_keys:
  - name: "Wx"
    key: "your_base64_encoded_key=="
  - name: "MyPrivateChannel"
    key: "another_base64_key=="
```

### Environment Variables

All settings can be overridden via environment variables:

| Variable | Description | Default |
|---|---|---|
| `COLLECTOR_MQTT_BROKER` | MQTT broker address | `127.0.0.1` |
| `COLLECTOR_MQTT_PORT` | MQTT broker port | `1883` |
| `COLLECTOR_MQTT_USERNAME` | MQTT username | (none) |
| `COLLECTOR_MQTT_PASSWORD` | MQTT password | (none) |
| `COLLECTOR_MQTT_TOPIC_PREFIX` | MQTT topic prefix | `msh` |
| `COLLECTOR_MQTT_TOPIC_SUFFIX` | MQTT topic suffix | `/+/+/+/#` |
| `COLLECTOR_POSTGRES_HOST` | PostgreSQL host | `localhost` |
| `COLLECTOR_POSTGRES_PORT` | PostgreSQL port | `5432` |
| `COLLECTOR_POSTGRES_USER` | PostgreSQL user | `postgres` |
| `COLLECTOR_POSTGRES_PASSWORD` | PostgreSQL password | `postgres` |
| `COLLECTOR_POSTGRES_DBNAME` | PostgreSQL database | `meshtastic` |
| `COLLECTOR_DEFAULT_KEY` | Default decryption key (base64) | LongFast key |
| `COLLECTOR_LOG_LEVEL` | Log level | `INFO` |

## Database Schema

### packet_history

Stores every received packet with full metadata, raw payload, and the original ServiceEnvelope bytes.

### node_info

Tracks Meshtastic nodes seen on the mesh. Updated when NODEINFO packets are received, or when a new gateway is encountered. Uses upsert semantics to merge new data while preserving `first_seen`.

## Development

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create venv and install deps
uv sync

# Run locally
uv run python -m collector

# Run tests
uv run pytest

# Lint
uv run ruff check src/
```

## License

MIT
