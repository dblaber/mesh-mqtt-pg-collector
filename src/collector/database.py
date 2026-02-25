"""PostgreSQL database operations for packet history and node info.

Follows the same schema and upsert logic as malla's postgres_writer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2
from psycopg2 import pool

from .config import PostgresConfig

logger = logging.getLogger(__name__)

_db_pool: pool.SimpleConnectionPool | None = None


def init_pool(config: PostgresConfig) -> None:
    """Initialize the PostgreSQL connection pool and ensure tables exist."""
    global _db_pool
    if _db_pool is not None:
        return

    try:
        _db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            dbname=config.dbname,
        )
        logger.info("PostgreSQL connection pool initialized")

        conn = get_connection()
        if conn:
            try:
                _create_tables(conn)
            finally:
                release_connection(conn)

    except psycopg2.OperationalError as e:
        logger.error("Failed to connect to PostgreSQL: %s", e)
        _db_pool = None
        raise


def get_connection():
    """Get a connection from the pool."""
    if not _db_pool:
        return None
    try:
        return _db_pool.getconn()
    except Exception as e:
        logger.error("Failed to get connection from pool: %s", e)
        return None


def release_connection(conn) -> None:
    """Release a connection back to the pool."""
    if _db_pool and conn:
        _db_pool.putconn(conn)


def close_pool() -> None:
    """Close all connections in the pool."""
    global _db_pool
    if _db_pool:
        _db_pool.closeall()
        _db_pool = None
        logger.info("PostgreSQL connection pool closed")


def _create_tables(conn) -> None:
    """Create tables if they do not already exist.

    Schema matches the user-provided DDL with BIGINT node IDs.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS packet_history (
                    id SERIAL PRIMARY KEY,
                    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
                    topic TEXT NOT NULL,
                    from_node_id BIGINT,
                    to_node_id BIGINT,
                    portnum BIGINT,
                    portnum_name TEXT,
                    gateway_id TEXT,
                    channel_id TEXT,
                    mesh_packet_id BIGINT,
                    rssi BIGINT,
                    snr REAL,
                    hop_limit INTEGER,
                    hop_start INTEGER,
                    payload_length INTEGER,
                    raw_payload BYTEA,
                    processed_successfully BOOLEAN DEFAULT TRUE,
                    message_type TEXT,
                    raw_service_envelope BYTEA,
                    parsing_error TEXT,
                    via_mqtt BOOLEAN,
                    want_ack BOOLEAN,
                    priority INTEGER,
                    delayed INTEGER,
                    channel_index INTEGER,
                    rx_time INTEGER,
                    pki_encrypted BOOLEAN,
                    next_hop BIGINT,
                    relay_node BIGINT,
                    tx_after BIGINT,
                    UNIQUE(mesh_packet_id, gateway_id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS node_info (
                    node_id BIGINT PRIMARY KEY,
                    hex_id TEXT,
                    long_name TEXT,
                    short_name TEXT,
                    hw_model TEXT,
                    role TEXT,
                    primary_channel TEXT,
                    is_licensed BOOLEAN,
                    mac_address TEXT,
                    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
                    last_updated TIMESTAMP WITH TIME ZONE NOT NULL
                );
            """)
        conn.commit()
        logger.info("PostgreSQL tables checked/created successfully")
    except Exception as e:
        conn.rollback()
        logger.error("Error creating tables: %s", e)


def insert_packet(packet_data: dict) -> None:
    """Insert a packet into the packet_history table.

    Uses ON CONFLICT (mesh_packet_id, gateway_id) DO NOTHING to deduplicate.
    """
    conn = get_connection()
    if not conn:
        logger.warning("No database connection available, dropping packet")
        return

    # Convert numeric timestamp to datetime if needed
    ts = packet_data.get("timestamp")
    if isinstance(ts, (int, float)):
        packet_data["timestamp"] = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif ts is None:
        packet_data["timestamp"] = datetime.now(tz=timezone.utc)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO packet_history
                (
                    "timestamp", topic, from_node_id, to_node_id, portnum, portnum_name,
                    gateway_id, channel_id, mesh_packet_id, rssi, snr, hop_limit, hop_start,
                    payload_length, raw_payload, processed_successfully, via_mqtt, want_ack,
                    priority, delayed, channel_index, rx_time, pki_encrypted, next_hop,
                    relay_node, tx_after, message_type, raw_service_envelope, parsing_error
                )
                VALUES (
                    %(timestamp)s, %(topic)s, %(from_node_id)s, %(to_node_id)s, %(portnum)s,
                    %(portnum_name)s, %(gateway_id)s, %(channel_id)s, %(mesh_packet_id)s,
                    %(rssi)s, %(snr)s, %(hop_limit)s, %(hop_start)s, %(payload_length)s,
                    %(raw_payload)s, %(processed_successfully)s, %(via_mqtt)s, %(want_ack)s,
                    %(priority)s, %(delayed)s, %(channel_index)s, %(rx_time)s,
                    %(pki_encrypted)s, %(next_hop)s, %(relay_node)s, %(tx_after)s,
                    %(message_type)s, %(raw_service_envelope)s, %(parsing_error)s
                ) ON CONFLICT (mesh_packet_id, gateway_id) DO NOTHING;
                """,
                packet_data,
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Error inserting packet: %s", e)
    finally:
        release_connection(conn)


def upsert_node(node_data: dict) -> None:
    """Insert a new node or update an existing one in the node_info table.

    Uses ON CONFLICT (node_id) DO UPDATE to merge new data while
    preserving first_seen.
    """
    conn = get_connection()
    if not conn:
        logger.warning("No database connection available, dropping node upsert")
        return

    # Convert numeric timestamps to datetime if needed
    for key in ("first_seen", "last_updated"):
        val = node_data.get(key)
        if isinstance(val, (int, float)):
            node_data[key] = datetime.fromtimestamp(val, tz=timezone.utc)
        elif val is None:
            node_data[key] = datetime.now(tz=timezone.utc)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO node_info
                (
                    node_id, hex_id, long_name, short_name, hw_model, role,
                    is_licensed, mac_address, primary_channel, first_seen, last_updated
                )
                VALUES (
                    %(node_id)s, %(hex_id)s, %(long_name)s, %(short_name)s, %(hw_model)s,
                    %(role)s, %(is_licensed)s, %(mac_address)s, %(primary_channel)s,
                    %(first_seen)s, %(last_updated)s
                ) ON CONFLICT (node_id) DO UPDATE SET
                    hex_id = COALESCE(EXCLUDED.hex_id, node_info.hex_id),
                    long_name = COALESCE(EXCLUDED.long_name, node_info.long_name),
                    short_name = COALESCE(EXCLUDED.short_name, node_info.short_name),
                    hw_model = COALESCE(EXCLUDED.hw_model, node_info.hw_model),
                    role = COALESCE(EXCLUDED.role, node_info.role),
                    is_licensed = COALESCE(EXCLUDED.is_licensed, node_info.is_licensed),
                    mac_address = COALESCE(EXCLUDED.mac_address, node_info.mac_address),
                    primary_channel = COALESCE(EXCLUDED.primary_channel, node_info.primary_channel),
                    last_updated = EXCLUDED.last_updated;
                """,
                node_data,
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Error upserting node: %s", e)
    finally:
        release_connection(conn)
