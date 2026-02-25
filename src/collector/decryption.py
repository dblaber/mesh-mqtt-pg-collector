"""Decryption utilities for Meshtastic packets.

This module provides functions to decrypt encrypted Meshtastic packets using
the standard AES256-CTR encryption with channel-specific key derivation.

Algorithm follows the same approach as malla/meshtop:
  1. Derive key: SHA256(base_key_bytes + channel_name_bytes) for named channels,
     or raw key bytes for primary channel.
  2. Build 16-byte nonce: packet_id (8 bytes LE) + sender_id (8 bytes LE).
  3. Decrypt with AES-CTR using the derived key and nonce.
  4. Parse decrypted bytes as mesh_pb2.Data protobuf.
"""

from __future__ import annotations

import base64
import hashlib
import logging

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from meshtastic import mesh_pb2, portnums_pb2

from .config import AppConfig, ChannelKey

logger = logging.getLogger(__name__)


def derive_key_from_channel_name(channel_name: str, key_base64: str) -> bytes:
    """Derive encryption key from channel name and base key.

    This follows Meshtastic's key derivation algorithm:
    - If a channel name is provided, derive via SHA256(key_bytes + channel_name_bytes).
    - Otherwise, use the raw decoded key bytes.

    Args:
        channel_name: Channel name for key derivation (empty for primary channel).
        key_base64: Base64-encoded encryption key.

    Returns:
        Encryption key bytes (32 bytes if derived, or raw length if primary).
    """
    try:
        key_bytes = base64.b64decode(key_base64)

        if channel_name and channel_name != "":
            channel_bytes = channel_name.encode("utf-8")
            hasher = hashlib.sha256()
            hasher.update(key_bytes)
            hasher.update(channel_bytes)
            return hasher.digest()
        else:
            return key_bytes
    except Exception as e:
        logger.warning("Error deriving key: %s", e)
        return b"\x00" * 32


def decrypt_packet_payload(
    encrypted_payload: bytes,
    packet_id: int,
    sender_id: int,
    key: bytes,
) -> bytes:
    """Decrypt a Meshtastic packet using AES-CTR.

    Args:
        encrypted_payload: The encrypted payload bytes.
        packet_id: The packet ID for nonce construction.
        sender_id: The sender node ID for nonce construction.
        key: The encryption key.

    Returns:
        Decrypted payload bytes, or empty bytes on failure.
    """
    try:
        if len(encrypted_payload) == 0:
            return b""

        # Construct nonce: packet_id (8 bytes LE) + sender_id (8 bytes LE) = 16 bytes
        packet_id_bytes = packet_id.to_bytes(8, byteorder="little")
        sender_id_bytes = sender_id.to_bytes(8, byteorder="little")
        nonce = packet_id_bytes + sender_id_bytes

        if len(nonce) != 16:
            logger.warning("Invalid nonce length: %d, expected 16 bytes", len(nonce))
            return b""

        # AES-CTR decryption
        cipher = Cipher(
            algorithms.AES(key),
            modes.CTR(nonce),
            backend=default_backend(),
        )
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(encrypted_payload) + decryptor.finalize()

        return decrypted

    except Exception as e:
        logger.warning("Decryption failed: %s", e)
        return b""


def try_decrypt_mesh_packet(
    mesh_packet,
    channel_name: str = "",
    key_base64: str = "1PG7OiApB1nwvP+rz05pAQ==",
) -> bool:
    """Try to decrypt an encrypted MeshPacket and update it with decoded content.

    Args:
        mesh_packet: The MeshPacket protobuf object.
        channel_name: Channel name for key derivation (empty for primary channel).
        key_base64: Base64-encoded encryption key.

    Returns:
        True if decryption was successful and packet was updated.
    """
    try:
        # Skip if already decoded
        if (
            hasattr(mesh_packet, "decoded")
            and mesh_packet.decoded.portnum != portnums_pb2.PortNum.UNKNOWN_APP
        ):
            return False

        # Must have encrypted data
        if not hasattr(mesh_packet, "encrypted") or not mesh_packet.encrypted:
            return False

        encrypted_payload = mesh_packet.encrypted
        packet_id = mesh_packet.id
        sender_id = getattr(mesh_packet, "from")  # 'from' is a Python keyword

        # Derive the decryption key
        key = derive_key_from_channel_name(channel_name, key_base64)

        # Decrypt the payload
        decrypted_payload = decrypt_packet_payload(
            encrypted_payload, packet_id, sender_id, key
        )

        if not decrypted_payload:
            return False

        # Parse as Data protobuf
        decoded_data = mesh_pb2.Data()
        decoded_data.ParseFromString(decrypted_payload)

        # Validate
        if decoded_data.portnum == portnums_pb2.PortNum.UNKNOWN_APP:
            return False

        # Update the mesh packet with decoded data
        mesh_packet.decoded.CopyFrom(decoded_data)

        portnum_name = portnums_pb2.PortNum.Name(decoded_data.portnum)
        logger.debug(
            "Decrypted packet %d from %d: %s", packet_id, sender_id, portnum_name
        )
        return True

    except Exception as e:
        logger.debug("Decryption attempt failed: %s", e)
        return False


def extract_channel_name_from_topic(topic: str) -> str:
    """Extract channel name from MQTT topic for key derivation.

    Topic format: msh/region/gateway_id/message_type/channel_name/gateway_hex

    Args:
        topic: MQTT topic string.

    Returns:
        Channel name or empty string for primary channel.
    """
    try:
        parts = topic.split("/")
        if len(parts) >= 5:
            candidate = parts[4]
            if candidate not in ("e", "c") and not candidate.startswith("!"):
                return candidate
    except Exception:
        pass
    return ""


def attempt_decryption(mesh_packet, topic: str, config: AppConfig) -> bool:
    """Try all available keys to decrypt a packet.

    Strategy (matching malla):
    1. Try default key with no channel derivation (primary channel).
    2. Try default key with channel name derivation.
    3. Try each configured channel-specific key.

    Args:
        mesh_packet: The MeshPacket protobuf object.
        topic: The MQTT topic the packet was received on.
        config: Application configuration with keys.

    Returns:
        True if decryption succeeded.
    """
    channel_name = extract_channel_name_from_topic(topic)

    # 1. Try default key with primary channel (no derivation)
    if try_decrypt_mesh_packet(mesh_packet, channel_name="", key_base64=config.default_key):
        return True

    # 2. Try default key with channel name derivation
    if channel_name:
        if try_decrypt_mesh_packet(
            mesh_packet, channel_name=channel_name, key_base64=config.default_key
        ):
            return True

    # 3. Try channel-specific keys
    for ck in config.channel_keys:
        # Try without derivation
        if try_decrypt_mesh_packet(mesh_packet, channel_name="", key_base64=ck.key):
            return True
        # Try with channel name derivation
        if channel_name:
            if try_decrypt_mesh_packet(
                mesh_packet, channel_name=channel_name, key_base64=ck.key
            ):
                return True
        # Try with the configured channel name
        if ck.name:
            if try_decrypt_mesh_packet(
                mesh_packet, channel_name=ck.name, key_base64=ck.key
            ):
                return True

    return False
