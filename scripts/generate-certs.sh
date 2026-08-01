#!/usr/bin/env bash
# Generate self-signed TLS certificates for Mosquitto.
#
# Usage:
#   ./scripts/generate-certs.sh [common_name] [days]
#
# Examples:
#   ./scripts/generate-certs.sh mqtt.local 365
#   ./scripts/generate-certs.sh 1825
#   ./scripts/generate.sh           # defaults: localhost, 365
#
# Output files are written to mosquitto/config/.

set -euo pipefail

COMMON_NAME="${1:-localhost}"
DAYS="${2:-365}"
CONFIG_DIR="$(cd "$(dirname "$0")/../mosquitto/config" && pwd)"

echo "Generating TLS certificates for Mosquitto..."
echo "  Common Name : $COMMON_NAME"
echo "  Validity    : $DAYS days"
echo "  Output dir  : $CONFIG_DIR"
echo

# CA key + cert
openssl genrsa -out "$CONFIG_DIR/ca.key" 4096 2>/dev/null
openssl req -new -x509 -days "$DAYS" -key "$CONFIG_DIR/ca.key" \
  -out "$CONFIG_DIR/ca.crt" \
  -subj "/C=US/ST=State/L=City/O=MeshMQTT/CN=$COMMON_NAME CA" 2>/dev/null

# Server key + CSR + cert
openssl genrsa -out "$CONFIG_DIR/server.key" 4096 2>/dev/null
openssl req -new -key "$CONFIG_DIR/server.key" \
  -out "$CONFIG_DIR/server.csr" \
  -subj "/C=US/ST=State/L=City/O=MeshMQTT/CN=$COMMON_NAME" 2>/dev/null
openssl x509 -req -days "$DAYS" \
  -in "$CONFIG_DIR/server.csr" \
  -CA "$CONFIG_DIR/ca.crt" \
  -CAkey "$CONFIG_DIR/ca.key" \
  -CAcreateserial \
  -out "$CONFIG_DIR/server.crt" 2>/dev/null

# Cleanup
rm -f "$CONFIG_DIR/server.csr" "$CONFIG_DIR/ca.srl"

echo "Generated files:"
ls -1 "$CONFIG_DIR"/{ca,server}.{crt,key}

echo
echo "Next steps:"
echo "  1. docker exec -it mosquitto mosquitto_passwd -b /mosquitto/config/pwfile admin yourpassword"
echo "  2. Update collector config: broker=mosquitto, port=8883"
