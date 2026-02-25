"""MQTT message handling: connect, subscribe, process Meshtastic packets.

Follows the same packet extraction and processing logic as malla's mqtt_capture.py.
"""

from __future__ import annotations

import logging
import socket
import time

import paho.mqtt.client as mqtt
from meshtastic import config_pb2, mesh_pb2, mqtt_pb2, portnums_pb2

from . import database
from .config import AppConfig
from .decryption import attempt_decryption

logger = logging.getLogger(__name__)


def _hex_id_to_numeric(hex_id: str) -> int | None:
    """Convert hex node ID (like '!abcdef12') to numeric ID."""
    if not hex_id or not isinstance(hex_id, str):
        return None
    clean = hex_id.lstrip("!")
    try:
        return int(clean, 16)
    except ValueError:
        return None


def _build_packet_data(
    topic: str,
    service_envelope,
    mesh_packet,
    processed_successfully: bool,
    raw_service_envelope_data: bytes | None,
    parsing_error: str | None,
) -> dict:
    """Extract all fields from a mesh packet into a dict for database insertion.

    Field extraction follows the same logic as malla's log_packet_to_database.
    """
    current_time = time.time()

    from_node_id = getattr(mesh_packet, "from", None) if mesh_packet else None
    to_node_id = getattr(mesh_packet, "to", None) if mesh_packet else None
    mesh_packet_id = getattr(mesh_packet, "id", None) if mesh_packet else None

    portnum = (
        mesh_packet.decoded.portnum
        if mesh_packet and hasattr(mesh_packet, "decoded")
        else None
    )
    portnum_name = portnums_pb2.PortNum.Name(portnum) if portnum is not None else None

    gateway_id = (
        getattr(service_envelope, "gateway_id", None) if service_envelope else None
    )
    channel_id = (
        getattr(service_envelope, "channel_id", None) if service_envelope else None
    )

    rssi = (
        getattr(mesh_packet, "rx_rssi", None)
        if mesh_packet and hasattr(mesh_packet, "rx_rssi")
        else None
    )
    snr = (
        getattr(mesh_packet, "rx_snr", None)
        if mesh_packet and hasattr(mesh_packet, "rx_snr")
        else None
    )
    hop_limit = getattr(mesh_packet, "hop_limit", None) if mesh_packet else None
    hop_start = getattr(mesh_packet, "hop_start", None) if mesh_packet else None

    payload_length = (
        len(mesh_packet.decoded.payload)
        if mesh_packet
        and hasattr(mesh_packet, "decoded")
        and hasattr(mesh_packet.decoded, "payload")
        else 0
    )
    raw_payload = (
        bytes(mesh_packet.decoded.payload)
        if mesh_packet
        and hasattr(mesh_packet, "decoded")
        and hasattr(mesh_packet.decoded, "payload")
        else b""
    )

    # Extract message type from topic (e.g., 'e' for encrypted)
    message_type = None
    try:
        topic_parts = topic.split("/")
        if len(topic_parts) >= 4:
            message_type = topic_parts[3]
    except Exception:
        pass

    # Additional MeshPacket fields
    via_mqtt = getattr(mesh_packet, "via_mqtt", None) if mesh_packet else None
    want_ack = getattr(mesh_packet, "want_ack", None) if mesh_packet else None
    priority = getattr(mesh_packet, "priority", None) if mesh_packet else None
    delayed = getattr(mesh_packet, "delayed", None) if mesh_packet else None
    channel_index = getattr(mesh_packet, "channel_index", None) if mesh_packet else None
    rx_time = getattr(mesh_packet, "rx_time", None) if mesh_packet else None
    pki_encrypted = getattr(mesh_packet, "pki_encrypted", None) if mesh_packet else None
    next_hop = getattr(mesh_packet, "next_hop", None) if mesh_packet else None
    relay_node = getattr(mesh_packet, "relay_node", None) if mesh_packet else None
    tx_after = getattr(mesh_packet, "tx_after", None) if mesh_packet else None

    return {
        "timestamp": current_time,
        "topic": topic,
        "from_node_id": from_node_id,
        "to_node_id": to_node_id,
        "portnum": portnum,
        "portnum_name": portnum_name,
        "gateway_id": gateway_id,
        "channel_id": channel_id,
        "mesh_packet_id": mesh_packet_id,
        "rssi": rssi,
        "snr": snr,
        "hop_limit": hop_limit,
        "hop_start": hop_start,
        "payload_length": payload_length,
        "raw_payload": raw_payload,
        "processed_successfully": processed_successfully,
        "via_mqtt": via_mqtt,
        "want_ack": want_ack,
        "priority": priority,
        "delayed": delayed,
        "channel_index": channel_index,
        "rx_time": rx_time,
        "pki_encrypted": pki_encrypted,
        "next_hop": next_hop,
        "relay_node": relay_node,
        "tx_after": tx_after,
        "message_type": message_type,
        "raw_service_envelope": raw_service_envelope_data,
        "parsing_error": parsing_error,
    }


def _process_nodeinfo(mesh_packet, from_node_id: int, service_envelope) -> None:
    """Process a NODEINFO_APP packet and upsert node info.

    Follows the same field extraction as malla's on_message NODEINFO handler.
    """
    try:
        user = mesh_pb2.User()
        user.ParseFromString(mesh_packet.decoded.payload)

        hw_model_str = mesh_pb2.HardwareModel.Name(user.hw_model).replace("UNSET", "Unknown")
        role_str = config_pb2.Config.DeviceConfig.Role.Name(user.role)

        mac_address = (
            user.macaddr.hex(":") if hasattr(user, "macaddr") and user.macaddr else None
        )

        now = time.time()
        node_data = {
            "node_id": from_node_id,
            "hex_id": user.id if user.id else None,
            "long_name": user.long_name if user.long_name else None,
            "short_name": user.short_name if user.short_name else None,
            "hw_model": hw_model_str,
            "role": role_str,
            "is_licensed": user.is_licensed,
            "mac_address": mac_address,
            "primary_channel": (
                service_envelope.channel_id if service_envelope else None
            ),
            "first_seen": now,
            "last_updated": now,
        }

        database.upsert_node(node_data)

        logger.info(
            "NodeInfo from %s: %s",
            user.id or f"!{from_node_id:08x}",
            user.long_name or user.short_name or "No name",
        )
    except Exception as e:
        logger.warning("Failed to process NODEINFO packet: %s", e)


def _upsert_gateway_node(gateway_id_str: str) -> None:
    """Ensure the gateway node exists in the node_info table."""
    numeric_id = _hex_id_to_numeric(gateway_id_str)
    if numeric_id is None:
        return

    now = time.time()
    node_data = {
        "node_id": numeric_id,
        "hex_id": gateway_id_str,
        "long_name": None,
        "short_name": None,
        "hw_model": None,
        "role": None,
        "is_licensed": None,
        "mac_address": None,
        "primary_channel": None,
        "first_seen": now,
        "last_updated": now,
    }
    database.upsert_node(node_data)


def _on_message(config: AppConfig):
    """Return a closure for the MQTT on_message callback."""

    def callback(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
        logger.debug("Received %d bytes on %s", len(msg.payload), msg.topic)

        # Skip JSON messages
        if "/json/" in msg.topic:
            return

        raw_service_envelope_data = msg.payload
        service_envelope = None
        mesh_packet = None
        processed_successfully = False
        parsing_error: str | None = None

        try:
            # Parse the ServiceEnvelope
            service_envelope = mqtt_pb2.ServiceEnvelope()
            service_envelope.ParseFromString(msg.payload)
            mesh_packet = service_envelope.packet

            from_node_id = getattr(mesh_packet, "from")
            to_node_id = mesh_packet.to

            # Attempt decryption if packet appears encrypted
            is_encrypted = (
                hasattr(mesh_packet, "decoded")
                and mesh_packet.decoded.portnum == portnums_pb2.PortNum.UNKNOWN_APP
                and hasattr(mesh_packet, "encrypted")
                and mesh_packet.encrypted
            )

            if is_encrypted:
                decrypted = attempt_decryption(mesh_packet, msg.topic, config)
                if decrypted:
                    logger.info(
                        "Decrypted packet from !%08x (%s)",
                        from_node_id,
                        portnums_pb2.PortNum.Name(mesh_packet.decoded.portnum),
                    )
                else:
                    logger.debug(
                        "Could not decrypt packet %d from !%08x",
                        mesh_packet.id,
                        from_node_id,
                    )

            # Track gateway as a node
            if service_envelope.gateway_id:
                _upsert_gateway_node(service_envelope.gateway_id)

            # Process NODEINFO_APP to populate node_info table
            if mesh_packet.decoded.portnum == portnums_pb2.PortNum.NODEINFO_APP:
                _process_nodeinfo(mesh_packet, from_node_id, service_envelope)

            processed_successfully = True

            # Log packet type
            portnum_name = portnums_pb2.PortNum.Name(mesh_packet.decoded.portnum)
            logger.info(
                "Packet %s from !%08x to !%08x via %s",
                portnum_name,
                from_node_id,
                to_node_id,
                service_envelope.gateway_id or "unknown",
            )

        except UnicodeDecodeError as e:
            parsing_error = f"Unicode decode error: {e}"
            logger.warning("Unicode decode error on %s: %s", msg.topic, e)
        except Exception as e:
            parsing_error = f"Parsing error: {e}"
            logger.error("Error processing message on %s: %s", msg.topic, e)

        # Always log the packet to the database, even on failure
        try:
            packet_data = _build_packet_data(
                msg.topic,
                service_envelope,
                mesh_packet,
                processed_successfully,
                raw_service_envelope_data,
                parsing_error,
            )
            database.insert_packet(packet_data)
        except Exception as db_err:
            logger.error("Failed to log packet to database: %s", db_err)

    return callback


def _on_connect(client: mqtt.Client, userdata, flags, rc, properties=None) -> None:
    """Callback for successful MQTT connection."""
    topic = userdata.get("topic", "msh/#")
    logger.info("Connected to MQTT broker, subscribing to %s", topic)
    client.subscribe(topic)


def _on_disconnect(client: mqtt.Client, userdata, flags, rc, properties=None) -> None:
    """Callback for MQTT disconnection."""
    if rc != 0:
        logger.warning("Unexpected MQTT disconnect (rc=%d), will auto-reconnect", rc)
    else:
        logger.info("Disconnected from MQTT broker")


def run(config: AppConfig) -> None:
    """Connect to MQTT and run the message loop forever.

    Handles reconnection with exponential backoff.
    """
    topic = f"{config.mqtt.topic_prefix}{config.mqtt.topic_suffix}"

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=config.mqtt.client_id,
        userdata={"topic": topic},
    )

    if config.mqtt.username:
        client.username_pw_set(config.mqtt.username, config.mqtt.password)

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message(config)

    # Connect with retry
    max_retries = 10
    base_delay = 2

    for attempt in range(max_retries):
        try:
            logger.info(
                "Connecting to MQTT broker at %s:%d ...",
                config.mqtt.broker_address,
                config.mqtt.port,
            )
            client.connect(config.mqtt.broker_address, config.mqtt.port, keepalive=60)
            break
        except (ConnectionRefusedError, socket.gaierror, OSError) as e:
            delay = min(base_delay * (2**attempt), 60)
            logger.warning(
                "Connection attempt %d/%d failed: %s. Retrying in %ds ...",
                attempt + 1,
                max_retries,
                e,
                delay,
            )
            time.sleep(delay)
    else:
        logger.error("Could not connect to MQTT broker after %d attempts", max_retries)
        raise SystemExit(1)

    logger.info("MQTT loop started, listening for Meshtastic packets ...")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down MQTT client ...")
        client.disconnect()
