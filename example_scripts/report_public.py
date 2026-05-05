#!/usr/bin/env python3
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import argparse
import time
import os
from rich.console import Console
from rich.table import Table

# Database configuration - set via environment variables or .env file
DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'mesh_capture'),
    'user': os.environ.get('DB_USER', 'mesh_capture'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost')
}

# Email configuration - set via environment variables or .env file
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
SMTP_USER = os.environ.get('SMTP_USER', '')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
TO_EMAIL = os.environ.get('TO_EMAIL', '')
FROM_EMAIL = os.environ.get('FROM_EMAIL', '')

def get_coverage_gaps(cursor, gateway_list, hours=24):
    """Get coverage gaps for each gateway - messages they missed that others heard."""
    gaps_by_gateway = {}

    for gw_id, gw_name in gateway_list:
        query = """
        WITH target_gateway_packets AS (
            SELECT DISTINCT mesh_packet_id
            FROM packet_history
            WHERE gateway_id = %s
                AND mesh_packet_id IS NOT NULL
                AND timestamp > NOW() - INTERVAL %s
        ),
        missed_packets AS (
            SELECT
                ph.mesh_packet_id,
                ph.from_node_id,
                ni.short_name as from_node,
                ni.hex_id as from_hex,
                ph.rssi,
                ph.snr
            FROM packet_history ph
            LEFT JOIN node_info ni ON ni.node_id = ph.from_node_id
            WHERE ph.gateway_id != %s
                AND ph.mesh_packet_id IS NOT NULL
                AND ph.timestamp > NOW() - INTERVAL %s
                AND ph.rssi IS NOT NULL
                AND ph.mesh_packet_id NOT IN (SELECT mesh_packet_id FROM target_gateway_packets)
        )
        SELECT
            from_node,
            from_hex,
            COUNT(*) as missed_packets,
            ROUND(AVG(rssi)::numeric, 2) as avg_rssi,
            ROUND(AVG(snr)::numeric, 2) as avg_snr
        FROM missed_packets
        GROUP BY from_node, from_hex
        ORDER BY missed_packets DESC
        LIMIT 5;
        """

        interval_str = f'{hours} hours'
        cursor.execute(query, (gw_id, interval_str, gw_id, interval_str))
        gaps_by_gateway[gw_name] = cursor.fetchall()

    return gaps_by_gateway

def get_relay_node_stats(cursor, hours=24):
    """Get statistics on relay node usage."""
    query = """
    WITH gateway_nodes AS (
        SELECT DISTINCT ni.node_id, ni.short_name
        FROM packet_history ph
        INNER JOIN node_info ni ON ni.hex_id = ph.gateway_id
        WHERE ph.gateway_id IS NOT NULL AND ni.node_id IS NOT NULL
    ),
    excluded_nodes AS (
        SELECT node_id, short_name
        FROM node_info
        WHERE short_name IN ('dmbm', 'DrMe')
    ),
    direct_packets AS (
        SELECT
            0 AS relay_node,
            '00' AS relay_node_hex,
            COUNT(*) AS packets_relayed
        FROM packet_history ph
        WHERE (ph.relay_node IS NULL OR ph.relay_node = 0)
            AND ph.timestamp > NOW() - INTERVAL %s
    ),
    relay_aggregated AS (
        SELECT
            ph.relay_node,
            lpad(to_hex(ph.relay_node & 255), 2, '0') AS relay_node_hex,
            COUNT(*) AS packets_relayed
        FROM packet_history ph
        WHERE ph.relay_node IS NOT NULL
            AND ph.relay_node != 0
            AND ph.timestamp > NOW() - INTERVAL %s
            AND (ph.hop_start - ph.hop_limit) > 0
            AND ph.from_node_id != ph.relay_node
        GROUP BY ph.relay_node
    ),
    all_relays AS (
        SELECT * FROM relay_aggregated
        UNION ALL
        SELECT * FROM direct_packets
    ),
    relay_with_candidates AS (
        SELECT
            ra.relay_node,
            ra.relay_node_hex,
            COALESCE(byte_match.short_name, 'Unknown') AS relay_short_name,
            ra.packets_relayed,
            CASE
                WHEN ra.relay_node = 0 THEN 'Direct'
                WHEN byte_match.hex_id IS NOT NULL THEN 'Byte Match'
                ELSE 'Unknown'
            END AS match_type,
            CASE
                WHEN ra.relay_node = 0 THEN NULL
                ELSE
                    (
                        SELECT string_agg(DISTINCT ni2.short_name, ', ')
                        FROM node_info ni2
                        WHERE RIGHT(ni2.hex_id, 2) = ra.relay_node_hex
                    )
            END AS other_candidates,
            CASE
                WHEN ra.relay_node = 0 THEN FALSE
                ELSE EXISTS (
                    SELECT 1
                    FROM node_info ni2
                    INNER JOIN gateway_nodes gn ON gn.node_id = ni2.node_id
                    WHERE RIGHT(ni2.hex_id, 2) = ra.relay_node_hex
                ) OR EXISTS (
                    SELECT 1
                    FROM node_info ni2
                    INNER JOIN excluded_nodes en ON en.node_id = ni2.node_id
                    WHERE RIGHT(ni2.hex_id, 2) = ra.relay_node_hex
                )
            END AS has_excluded_candidate
        FROM all_relays ra
        LEFT JOIN LATERAL (
            SELECT short_name, hex_id
            FROM node_info
            WHERE RIGHT(hex_id, 2) = ra.relay_node_hex
            ORDER BY short_name
            LIMIT 1
        ) byte_match ON true
    )
    SELECT
        CASE WHEN relay_node = 0 THEN NULL ELSE relay_node END as relay_node,
        relay_node_hex,
        CASE WHEN relay_node = 0 THEN 'Direct (no relay)' ELSE relay_short_name END as relay_short_name,
        packets_relayed,
        match_type,
        other_candidates,
        byte_match.hex_id as relay_hex_id
    FROM relay_with_candidates
    LEFT JOIN LATERAL (
        SELECT hex_id
        FROM node_info
        WHERE RIGHT(hex_id, 2) = relay_node_hex
        ORDER BY short_name
        LIMIT 1
    ) byte_match ON true
    WHERE has_excluded_candidate = FALSE
    ORDER BY packets_relayed DESC
    LIMIT 10;
    """

    interval_str = f'{hours} hours'
    cursor.execute(query, (interval_str, interval_str))
    return cursor.fetchall()

def get_relay_type_breakdown(cursor, hours=24):
    """Get breakdown of packets by relay type (modern, legacy, direct)."""
    query = """
    WITH gateway_nodes AS (
        SELECT DISTINCT ni.node_id
        FROM packet_history ph
        INNER JOIN node_info ni ON ni.hex_id = ph.gateway_id
        WHERE ph.gateway_id IS NOT NULL AND ni.node_id IS NOT NULL
    )
    SELECT
        CASE
            WHEN ph.relay_node IS NOT NULL AND ph.relay_node != 0 THEN 'Modern Relay'
            WHEN (ph.relay_node IS NULL OR ph.relay_node = 0)
                AND (ph.hop_start - ph.hop_limit) > 0 THEN 'Legacy Relay (swrd_rpt)'
            WHEN (ph.relay_node IS NULL OR ph.relay_node = 0)
                AND (ph.hop_start - ph.hop_limit) = 0 THEN 'Direct (no relay)'
            ELSE 'Unknown'
        END AS relay_type,
        COUNT(*) AS packet_count,
        COUNT(DISTINCT ph.from_node_id) AS unique_senders,
        ROUND(AVG(ph.rssi)::numeric, 2) AS avg_rssi,
        ROUND(AVG(ph.snr)::numeric, 2) AS avg_snr,
        ROUND(AVG(ph.hop_start - ph.hop_limit)::numeric, 2) AS avg_hops
    FROM packet_history ph
    WHERE ph.timestamp > NOW() - INTERVAL %s
        AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
    GROUP BY relay_type
    ORDER BY packet_count DESC;
    """

    interval_str = f'{hours} hours'
    cursor.execute(query, (interval_str,))
    results = list(cursor.fetchall())

    # Calculate non-legacy total (Modern Relay + Direct)
    modern_count = 0
    modern_rssi_sum = 0
    modern_snr_sum = 0
    modern_hops_sum = 0
    modern_total = 0

    for row in results:
        relay_type = row[0]
        if relay_type == 'Modern Relay' or relay_type == 'Direct (no relay)':
            modern_count += row[1]
            if row[3] is not None:  # avg_rssi
                modern_rssi_sum += row[3] * row[1]  # weighted by packet count
                modern_total += row[1]
            if row[4] is not None:  # avg_snr
                modern_snr_sum += row[4] * row[1]
            if row[5] is not None:  # avg_hops
                modern_hops_sum += row[5] * row[1]

    # Add summary row if we have non-legacy packets
    if modern_count > 0:
        avg_rssi = round(modern_rssi_sum / modern_total, 2) if modern_total > 0 else None
        avg_snr = round(modern_snr_sum / modern_total, 2) if modern_total > 0 else None
        avg_hops = round(modern_hops_sum / modern_total, 2) if modern_total > 0 else None

        # Query to get unique senders for non-legacy packets
        unique_senders_query = """
        WITH gateway_nodes AS (
            SELECT DISTINCT ni.node_id
            FROM packet_history ph
            INNER JOIN node_info ni ON ni.hex_id = ph.gateway_id
            WHERE ph.gateway_id IS NOT NULL AND ni.node_id IS NOT NULL
        )
        SELECT COUNT(DISTINCT ph.from_node_id) AS unique_senders
        FROM packet_history ph
        WHERE ph.timestamp > NOW() - INTERVAL %s
            AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
            AND (
                (ph.relay_node IS NOT NULL AND ph.relay_node != 0)
                OR ((ph.relay_node IS NULL OR ph.relay_node = 0)
                    AND (ph.hop_start - ph.hop_limit) = 0)
            );
        """
        cursor.execute(unique_senders_query, (interval_str,))
        unique_senders = cursor.fetchone()[0]

        # Add a separator row and summary row
        results.append(('--- Non-Legacy Total (Modern + Direct) ---', modern_count, unique_senders, avg_rssi, avg_snr, avg_hops))

    return results

def get_direct_packets(cursor, hours=24):
    """Get direct (no relay) packet records, excluding Swrd and misconfigured 0/0 hop packets."""
    query = """
    WITH gateway_nodes AS (
        SELECT DISTINCT ni.node_id
        FROM packet_history ph
        INNER JOIN node_info ni ON ni.hex_id = ph.gateway_id
        WHERE ph.gateway_id IS NOT NULL AND ni.node_id IS NOT NULL
    )
    SELECT
        ni.short_name as from_node,
        ni.hex_id as from_hex,
        (ph.timestamp AT TIME ZONE 'America/New_York') as packet_time,
        gw.short_name as gateway,
        gw.hex_id as gateway_hex,
        ph.rssi,
        ph.snr,
        ph.portnum_name,
        ph.mesh_packet_id,
        ph.hop_start,
        ph.hop_limit
    FROM packet_history ph
    LEFT JOIN node_info ni ON ni.node_id = ph.from_node_id
    LEFT JOIN node_info gw ON gw.hex_id = ph.gateway_id
    WHERE ph.timestamp > NOW() - INTERVAL %s
        AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
        AND (ph.relay_node IS NULL OR ph.relay_node = 0)
        AND (ph.hop_start - ph.hop_limit) = 0
        AND ph.hop_start > 0
        AND ph.rssi IS NOT NULL
        AND ni.short_name != 'Swrd'
    ORDER BY ph.timestamp DESC
    LIMIT 100;
    """

    interval_str = f'{hours} hours'
    cursor.execute(query, (interval_str,))
    return cursor.fetchall()

def get_specific_relay_breakdown(cursor, hours=24):
    """Get relay breakdown for specific nodes: dmbw, dmbs, mtkm, wb1w."""
    query = """
    WITH gateway_nodes AS (
        SELECT DISTINCT ni.node_id
        FROM packet_history ph
        INNER JOIN node_info ni ON ni.hex_id = ph.gateway_id
        WHERE ph.gateway_id IS NOT NULL AND ni.node_id IS NOT NULL
    ),
    target_nodes AS (
        SELECT
            node_id,
            short_name,
            hex_id,
            ('x' || RIGHT(hex_id, 2))::bit(8)::int as last_byte
        FROM node_info
        WHERE short_name IN ('dmbw', 'dmbs', 'mtkm', 'wb1w')
    )
    SELECT
        tn.short_name as relay_name,
        tn.hex_id,
        tn.last_byte as relay_byte,
        COUNT(*) as packet_count,
        COUNT(DISTINCT ph.from_node_id) as unique_senders,
        ROUND(AVG(ph.rssi)::numeric, 2) as avg_rssi,
        ROUND(AVG(ph.snr)::numeric, 2) as avg_snr,
        ROUND(AVG(ph.hop_start - ph.hop_limit)::numeric, 2) as avg_hops
    FROM packet_history ph
    INNER JOIN target_nodes tn ON ph.relay_node = tn.last_byte
    WHERE ph.timestamp > NOW() - INTERVAL %s
        AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
        AND ph.relay_node IS NOT NULL
        AND ph.relay_node != 0
    GROUP BY tn.short_name, tn.hex_id, tn.last_byte
    ORDER BY packet_count DESC;
    """

    interval_str = f'{hours} hours'
    cursor.execute(query, (interval_str,))
    return cursor.fetchall()

def get_node_hearers(cursor, node_short_name, hours=24):
    """Get statistics on which gateways heard packets from/relayed by a node (counting only first gateway to hear each packet)."""
    query = """
    WITH target_node AS (
        SELECT
            node_id,
            short_name,
            hex_id,
            ('x' || RIGHT(hex_id, 2))::bit(8)::int as last_byte
        FROM node_info
        WHERE short_name = %s
        LIMIT 1
    ),
    packets_from_or_relayed_by_target AS (
        SELECT
            ph.mesh_packet_id,
            ph.gateway_id,
            ph.timestamp,
            ph.rssi,
            ph.snr,
            ph.from_node_id,
            ph.hop_start,
            ph.hop_limit,
            ph.relay_node,
            (ph.hop_start - ph.hop_limit) as hops_taken,
            CASE
                -- Direct packets from target node (0 hops, no relay)
                WHEN ph.from_node_id = (SELECT node_id FROM target_node)
                    AND (ph.hop_start - ph.hop_limit) = 0
                    THEN 'Direct from node'
                -- Packets from target node that were relayed (hops > 0)
                -- This includes both modern relay (relay_node populated) and legacy (relay_node NULL)
                WHEN ph.from_node_id = (SELECT node_id FROM target_node)
                    AND (ph.hop_start - ph.hop_limit) > 0
                    THEN 'Relayed by node'
                -- Packets relayed BY target node (not from it)
                WHEN ph.relay_node = (SELECT last_byte FROM target_node)
                    THEN 'Relayed by node'
                ELSE 'Other'
            END as packet_type,
            ROW_NUMBER() OVER (
                PARTITION BY ph.mesh_packet_id
                ORDER BY (ph.hop_start - ph.hop_limit) ASC
            ) as hop_rank
        FROM packet_history ph
        CROSS JOIN target_node tn
        WHERE ph.timestamp > NOW() - INTERVAL %s
            AND ph.rssi IS NOT NULL
            AND ph.mesh_packet_id IS NOT NULL
            AND (
                -- All packets from target node (direct or relayed)
                (ph.from_node_id = tn.node_id)
                OR
                -- Packets relayed by target node (modern firmware)
                (ph.relay_node = tn.last_byte)
            )
    ),
    first_hearers AS (
        SELECT *
        FROM packets_from_or_relayed_by_target
        WHERE hop_rank = 1
    )
    SELECT
        gw.short_name as gateway_name,
        gw.hex_id as gateway_hex,
        COUNT(*) as total_packets,
        SUM(CASE WHEN packet_type = 'Direct from node' THEN 1 ELSE 0 END) as direct_packets,
        SUM(CASE WHEN packet_type = 'Relayed by node' THEN 1 ELSE 0 END) as relayed_packets,
        ROUND(AVG(rssi)::numeric, 2) as avg_rssi,
        ROUND(AVG(snr)::numeric, 2) as avg_snr,
        MAX(timestamp AT TIME ZONE 'America/New_York') as last_heard
    FROM first_hearers p
    LEFT JOIN node_info gw ON gw.hex_id = p.gateway_id
    GROUP BY gw.short_name, gw.hex_id
    ORDER BY total_packets DESC
    LIMIT 20;
    """

    interval_str = f'{hours} hours'
    cursor.execute(query, (node_short_name, interval_str))
    return cursor.fetchall()

def get_missed_text_messages(cursor, target_gateway_hex, hours=24):
    """Get text messages missed by target gateway with details of which gateways heard them."""
    query = """
    WITH target_gateway_packets AS (
        SELECT DISTINCT mesh_packet_id
        FROM packet_history
        WHERE gateway_id = %s
            AND mesh_packet_id IS NOT NULL
            AND timestamp > NOW() - INTERVAL %s
    ),
    missed_text_messages AS (
        SELECT DISTINCT ON (ph.mesh_packet_id)
            ph.mesh_packet_id,
            ph.from_node_id,
            ni.short_name as from_node,
            ni.hex_id as from_hex,
            (ph.timestamp AT TIME ZONE 'America/New_York') as msg_time,
            ph.portnum_name,
            encode(ph.raw_payload, 'escape') as message_text
        FROM packet_history ph
        LEFT JOIN node_info ni ON ni.node_id = ph.from_node_id
        WHERE ph.gateway_id != %s
            AND ph.mesh_packet_id IS NOT NULL
            AND ph.portnum_name = 'TEXT_MESSAGE_APP'
            AND ph.timestamp > NOW() - INTERVAL %s
            AND ph.mesh_packet_id NOT IN (SELECT mesh_packet_id FROM target_gateway_packets)
            AND ph.raw_payload IS NOT NULL
    ),
    receivers AS (
        SELECT
            ph.mesh_packet_id,
            gw.short_name as gateway_name,
            ph.rssi,
            ph.snr
        FROM packet_history ph
        LEFT JOIN node_info gw ON gw.hex_id = ph.gateway_id
        WHERE ph.mesh_packet_id IN (SELECT mesh_packet_id FROM missed_text_messages)
            AND ph.rssi IS NOT NULL
        ORDER BY ph.mesh_packet_id, ph.rssi DESC
    )
    SELECT
        mtm.from_node,
        mtm.from_hex,
        mtm.msg_time,
        mtm.mesh_packet_id,
        string_agg(
            r.gateway_name || ' (' || r.rssi || 'dBm, ' || ROUND(r.snr::numeric, 1) || 'dB)',
            ', '
            ORDER BY r.rssi DESC
        ) as heard_by,
        mtm.message_text
    FROM missed_text_messages mtm
    LEFT JOIN receivers r ON r.mesh_packet_id = mtm.mesh_packet_id
    GROUP BY mtm.from_node, mtm.from_hex, mtm.msg_time, mtm.mesh_packet_id, mtm.message_text
    ORDER BY mtm.msg_time DESC
    LIMIT 20;
    """

    interval_str = f'{hours} hours'
    cursor.execute(query, (target_gateway_hex, interval_str, target_gateway_hex, interval_str))
    return cursor.fetchall()

def get_top_relays_per_gateway(cursor, hours=24):
    """Get top relay nodes heard by each gateway (including legacy relays like Swrd).

    Uses RSSI/SNR to determine the most likely relay node when multiple nodes share the same last byte.
    """
    query = """
    WITH gateway_nodes AS (
        SELECT DISTINCT ni.node_id, ni.short_name, ni.hex_id
        FROM packet_history ph
        INNER JOIN node_info ni ON ni.hex_id = ph.gateway_id
        WHERE ph.gateway_id IS NOT NULL AND ni.node_id IS NOT NULL
    ),
    -- Precompute direct signal strengths for candidates (7-day lookback for infrequent transmitters)
    candidate_signals AS (
        SELECT
            ph.gateway_id,
            ph.from_node_id,
            AVG(ph.rssi) as avg_rssi,
            COUNT(*) as packet_count
        FROM packet_history ph
        WHERE ph.timestamp > NOW() - INTERVAL '7 days'
            AND (ph.hop_start - ph.hop_limit) = 0  -- Direct packets only
            AND ph.rssi IS NOT NULL
            AND ph.rssi != 0.0
        GROUP BY ph.gateway_id, ph.from_node_id
    ),
    -- Find best candidate for each relay_node byte at each gateway based on direct packet RSSI
    best_relay_candidates AS (
        SELECT DISTINCT ON (gw_id, relay_byte)
            gw_id as gateway_id,
            relay_byte as relay_node,
            best_name as best_candidate_name,
            best_hex as best_candidate_hex
        FROM (
            SELECT
                cs.gateway_id as gw_id,
                ('x' || RIGHT(ni.hex_id, 2))::bit(8)::int as relay_byte,
                ni.short_name as best_name,
                ni.hex_id as best_hex,
                cs.avg_rssi as rssi_to_gw,
                cs.packet_count as packets_to_gw
            FROM candidate_signals cs
            INNER JOIN node_info ni ON ni.node_id = cs.from_node_id
            WHERE ni.role IS NULL OR ni.role != 'CLIENT_MUTE'

            UNION ALL

            -- For relay bytes with no direct reception, get all candidates for alphabetical fallback
            SELECT DISTINCT
                NULL::text as gw_id,
                ('x' || RIGHT(ni.hex_id, 2))::bit(8)::int as relay_byte,
                ni.short_name as best_name,
                ni.hex_id as best_hex,
                NULL::numeric as rssi_to_gw,
                0 as packets_to_gw
            FROM node_info ni
            WHERE ni.role IS NULL OR ni.role != 'CLIENT_MUTE'
        ) candidates
        ORDER BY gw_id, relay_byte,
                 rssi_to_gw DESC NULLS LAST,
                 packets_to_gw DESC,
                 best_name ASC
    ),
    gateway_relays AS (
        SELECT
            gw.short_name as gateway_name,
            ph.gateway_id,
            CASE
                -- Modern relay - use best candidate based on RSSI
                WHEN ph.relay_node IS NOT NULL AND ph.relay_node != 0 THEN
                    COALESCE(brc.best_candidate_name, '!' || to_hex(ph.relay_node))
                -- Legacy relay (likely Swrd)
                WHEN (ph.relay_node IS NULL OR ph.relay_node = 0)
                     AND (ph.hop_start - ph.hop_limit) > 0 THEN
                    'Swrd (legacy)'
                ELSE 'Unknown'
            END as relay_node_name,
            ph.relay_node,
            lpad(to_hex(ph.relay_node), 2, '0') as relay_node_hex,
            COUNT(*) as packet_count,
            CASE
                WHEN ph.relay_node IS NOT NULL AND ph.relay_node != 0 THEN 'Modern'
                WHEN (ph.relay_node IS NULL OR ph.relay_node = 0)
                     AND (ph.hop_start - ph.hop_limit) > 0 THEN 'Legacy'
                ELSE 'Unknown'
            END as relay_type
        FROM packet_history ph
        LEFT JOIN node_info gw ON gw.hex_id = ph.gateway_id
        LEFT JOIN best_relay_candidates brc ON
            brc.gateway_id = ph.gateway_id
            AND brc.relay_node = ph.relay_node
        WHERE ph.timestamp > NOW() - INTERVAL %s
            AND ph.gateway_id IS NOT NULL
            AND (ph.hop_start - ph.hop_limit) > 0  -- Only relayed packets
            AND ph.rssi IS NOT NULL
            AND ph.rssi != 0.0
            -- Exclude packets FROM gateway nodes
            AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
        GROUP BY gw.short_name, ph.gateway_id, relay_node_name, ph.relay_node, relay_node_hex, relay_type
    ),
    ranked_relays AS (
        SELECT
            gateway_name,
            gateway_id,
            relay_node_name,
            relay_node_hex,
            relay_type,
            packet_count,
            ROW_NUMBER() OVER (PARTITION BY gateway_name ORDER BY packet_count DESC) as rank
        FROM gateway_relays
    )
    SELECT
        gateway_name,
        gateway_id,
        relay_node_name,
        relay_node_hex,
        relay_type,
        packet_count
    FROM ranked_relays
    WHERE rank <= 5  -- Top 5 relays per gateway
    ORDER BY gateway_name, packet_count DESC;
    """

    interval_str = f'{hours} hours'
    cursor.execute(query, (interval_str,))
    return cursor.fetchall()

def get_query(hours=24):
    """Generate the query with the specified time range in hours."""
    return f"""
WITH gateway_nodes AS (
    SELECT DISTINCT ni.node_id
    FROM packet_history ph
    INNER JOIN node_info ni ON ni.hex_id = ph.gateway_id
    WHERE ph.gateway_id IS NOT NULL AND ni.node_id IS NOT NULL
),
excluded_relay_nodes AS (
    SELECT
        node_id,
        short_name,
        hex_id,
        ('x' || RIGHT(hex_id, 2))::bit(8)::int as last_byte
    FROM node_info
    WHERE short_name IN ('mtkm', 'wb1w', 'dmbs', 'dmbw')
),
gateway_performance AS (
    SELECT
        ph.gateway_id,
        gw.short_name AS gateway_short_name,
        gw.long_name AS gateway_long_name,
        gw.hw_model,
        -- Total direct packets (hops = 0)
        COUNT(*) FILTER (WHERE (ph.hop_start - ph.hop_limit) = 0) AS total_direct_packets,
        -- Total relayed packets (hops > 0)
        COUNT(*) FILTER (WHERE (ph.hop_start - ph.hop_limit) > 0) AS total_relayed_packets,
        -- Clean direct packets (excluding from gateway nodes)
        COUNT(*) FILTER (
            WHERE (ph.hop_start - ph.hop_limit) = 0
            AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
        ) AS clean_direct_packets,
        -- Clean relayed packets (excluding from/relayed by gateway nodes and excluded relay nodes)
        COUNT(*) FILTER (
            WHERE (ph.hop_start - ph.hop_limit) > 0
            AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
            AND (ph.relay_node IS NULL OR ph.relay_node NOT IN (SELECT last_byte FROM excluded_relay_nodes))
        ) AS clean_relayed_packets,
        -- RSSI/SNR calculated on clean packets (both direct + relayed)
        AVG(ph.rssi) FILTER (
            WHERE (
                ((ph.hop_start - ph.hop_limit) = 0 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes))
                OR
                ((ph.hop_start - ph.hop_limit) > 0
                 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
                 AND (ph.relay_node IS NULL OR ph.relay_node NOT IN (SELECT last_byte FROM excluded_relay_nodes)))
            )
        ) AS avg_rssi,
        AVG(ph.snr) FILTER (
            WHERE (
                ((ph.hop_start - ph.hop_limit) = 0 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes))
                OR
                ((ph.hop_start - ph.hop_limit) > 0
                 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
                 AND (ph.relay_node IS NULL OR ph.relay_node NOT IN (SELECT last_byte FROM excluded_relay_nodes)))
            )
        ) AS avg_snr,
        STDDEV_POP(ph.rssi) FILTER (
            WHERE (
                ((ph.hop_start - ph.hop_limit) = 0 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes))
                OR
                ((ph.hop_start - ph.hop_limit) > 0
                 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
                 AND (ph.relay_node IS NULL OR ph.relay_node NOT IN (SELECT last_byte FROM excluded_relay_nodes)))
            )
        ) AS rssi_consistency,
        -- Unique nodes from clean packets (both direct + relayed)
        COUNT(DISTINCT ph.from_node_id) FILTER (
            WHERE (
                ((ph.hop_start - ph.hop_limit) = 0 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes))
                OR
                ((ph.hop_start - ph.hop_limit) > 0
                 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
                 AND (ph.relay_node IS NULL OR ph.relay_node NOT IN (SELECT last_byte FROM excluded_relay_nodes)))
            )
        ) AS unique_nodes_heard
    FROM packet_history ph
    LEFT JOIN node_info gw ON gw.hex_id = ph.gateway_id
    WHERE ph.gateway_id IS NOT NULL
        AND ph.rssi IS NOT NULL
        AND ph.rssi != 0.0
        AND ph.timestamp > NOW() - INTERVAL '{hours} hours'
    GROUP BY ph.gateway_id, gw.short_name, gw.long_name, gw.hw_model
    HAVING (
        COUNT(*) FILTER (WHERE (ph.hop_start - ph.hop_limit) = 0 AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)) +
        COUNT(*) FILTER (
            WHERE (ph.hop_start - ph.hop_limit) > 0
            AND ph.from_node_id NOT IN (SELECT node_id FROM gateway_nodes)
            AND (ph.relay_node IS NULL OR ph.relay_node NOT IN (SELECT last_byte FROM excluded_relay_nodes))
        )
    ) >= 5
)
SELECT
    gateway_id,
    gateway_short_name,
    gateway_long_name,
    hw_model,
    total_direct_packets,
    total_relayed_packets,
    clean_direct_packets,
    clean_relayed_packets,
    (clean_direct_packets + clean_relayed_packets) AS total_clean_packets,
    ROUND(avg_rssi::numeric, 2) AS avg_rssi,
    ROUND(avg_snr::numeric, 2) AS avg_snr,
    ROUND(rssi_consistency::numeric, 2) AS rssi_stddev,
    unique_nodes_heard,
    RANK() OVER (ORDER BY avg_rssi DESC) AS rssi_rank,
    RANK() OVER (ORDER BY avg_snr DESC) AS snr_rank,
    RANK() OVER (ORDER BY unique_nodes_heard DESC) AS coverage_rank,
    RANK() OVER (ORDER BY (clean_direct_packets + clean_relayed_packets) DESC) AS volume_rank,
    ROUND(
        (((1 - PERCENT_RANK() OVER (ORDER BY avg_rssi DESC)) * 0.10 +
          (1 - PERCENT_RANK() OVER (ORDER BY avg_snr DESC)) * 0.05 +
          (1 - PERCENT_RANK() OVER (ORDER BY unique_nodes_heard DESC)) * 0.45 +
          (1 - PERCENT_RANK() OVER (ORDER BY (clean_direct_packets + clean_relayed_packets) DESC)) * 0.40) * 100)::numeric
    , 2) AS performance_score
FROM gateway_performance
ORDER BY performance_score DESC;
"""

def make_node_link(node_id, node_name):
    """Create a deep link to malla.app for a node."""
    if not node_id or node_id == 'N/A':
        return node_name or 'Unknown'
    return f'<a href="https://malla.app.da4.org/node/{node_id}" target="_blank" style="color: #0066cc; text-decoration: none;">{node_name or node_id}</a>'

def generate_html_table(rows, columns, hours=24, coverage_gaps=None, missed_texts=None, target_gateway='dmbf', relay_stats=None, relay_type_breakdown=None, direct_packets=None, specific_relay_breakdown=None, c98c_hearers=None, swrd_hearers=None, mtkm_hearers=None, wb1w_hearers=None, top_relays_per_gateway=None):
    """Generate an HTML table from query results."""
    from datetime import timedelta

    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)

    html = """
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            table { border-collapse: collapse; width: 100%; margin: 20px 0; }
            th { background-color: #4CAF50; color: white; padding: 12px; text-align: left; }
            td { border: 1px solid #ddd; padding: 8px; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            tr:hover { background-color: #ddd; }
            .header { background-color: #333; color: white; padding: 20px; }
            .footer { margin-top: 20px; color: #666; font-size: 12px; }
            .rank-1 { background-color: #ffd700 !important; font-weight: bold; }
            a { color: #0066cc; text-decoration: none; }
            a:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <div class="header">
            <h2>Gateway Performance Report - """ + end_time.strftime('%Y-%m-%d %I:%M %p') + """</h2>
            <p>Time Range: Last """ + str(hours) + """ hours</p>
            <p>From: """ + start_time.strftime('%Y-%m-%d %I:%M:%S %p') + """ to """ + end_time.strftime('%Y-%m-%d %I:%M:%S %p') + """</p>
            <p><a href="https://malla.app.da4.org/nodes" target="_blank">View All Nodes</a> |
               <a href="https://malla.app.da4.org/packets" target="_blank">View Packets</a> |
               <a href="https://malla.app.da4.org/gateway/compare" target="_blank">Compare Gateways</a> |
               <a href="https://malla.app.da4.org/map" target="_blank">View Map</a></p>
        </div>
        <table>
            <thead>
                <tr>
    """

    # Add headers
    for col in columns:
        html += f"<th>{col.replace('_', ' ').title()}</th>"
    html += "</tr></thead><tbody>"

    # Add rows with gateway links (first column is gateway_id, second is gateway_short_name)
    for i, row in enumerate(rows):
        row_class = 'rank-1' if i == 0 else ''
        html += f"<tr class='{row_class}'>"
        for j, value in enumerate(row):
            # Link the gateway_id (column 0) and gateway_short_name (column 1) to node page
            if j == 0 and value:  # gateway_id column
                html += f"<td>{make_node_link(value, value)}</td>"
            elif j == 1 and value and i < len(rows):  # gateway_short_name column
                gateway_id = row[0] if row[0] else value
                html += f"<td>{make_node_link(gateway_id, value)}</td>"
            else:
                html += f"<td>{value if value is not None else 'N/A'}</td>"
        html += "</tr>"

    html += """
            </tbody>
        </table>
    """

    # Add top relays per gateway section if available
    if top_relays_per_gateway:
        html += """
        <h3 style="margin-top: 30px; color: #333;">Top Relays Per Gateway (Excluding Own Infrastructure)</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>Gateway</th>
                    <th>Top Relays (Packets)</th>
                </tr>
            </thead>
            <tbody>
        """

        # Organize data by gateway
        relays_by_gateway = {}
        for row in top_relays_per_gateway:
            gateway_name = row[0]
            relay_node_name = row[2]
            relay_node_hex = row[3]
            relay_type = row[4]
            packet_count = row[5]

            if gateway_name not in relays_by_gateway:
                relays_by_gateway[gateway_name] = []

            relays_by_gateway[gateway_name].append((relay_node_name, relay_node_hex, relay_type, packet_count))

        # Add rows for each gateway
        for gateway_name in sorted(relays_by_gateway.keys()):
            relay_items = []
            for relay_name, relay_hex, relay_type, count in relays_by_gateway[gateway_name]:
                type_indicator = " <span style='color: #999;'>[Legacy]</span>" if relay_type == "Legacy" else ""
                hex_display = f"<span style='color: #666;'>(!{relay_hex})</span>" if relay_type == "Modern" else ""
                name_display = f"{relay_name}{type_indicator}" if relay_type == "Legacy" else f"{relay_name} {hex_display}"
                relay_items.append(f"{name_display} ({count})")

            relay_str = ", ".join(relay_items)
            html += f"<tr><td>{gateway_name}</td><td>{relay_str}</td></tr>"

        html += """
            </tbody>
        </table>
        <p style="font-size: 12px; color: #666;">[Legacy] indicates legacy relay (likely Swrd). Hex shows relay node byte (e.g., !f6).</p>
        """

    # Add coverage gaps section if available
    if coverage_gaps:
        html += """
        <h3 style="margin-top: 30px; color: #333;">Coverage Gaps - Top 5 Nodes Missed by Each Gateway</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>Gateway</th>
                    <th>Missed Nodes (Count, Avg RSSI, Avg SNR)</th>
                </tr>
            </thead>
            <tbody>
        """
        for gw_name, gaps in sorted(coverage_gaps.items()):
            if gaps:
                gap_items = []
                for node, hex_id, count, rssi, snr in gaps:
                    node_link = make_node_link(hex_id, node or 'Unknown')
                    gap_items.append(f"{node_link} ({count}, {rssi}dBm, {snr}dB)")
                gap_list = ", ".join(gap_items)
            else:
                gap_list = "None - heard all messages"
            html += f"<tr><td>{gw_name}</td><td>{gap_list}</td></tr>"

        html += """
            </tbody>
        </table>
        """

    # Add relay type breakdown if available
    if relay_type_breakdown:
        html += generate_html_relay_type_breakdown(relay_type_breakdown)

    # Add relay node statistics if available
    if relay_stats:
        html += generate_html_relay_stats(relay_stats)

    # Add missed text messages section if available
    if missed_texts:
        html += generate_html_missed_texts(missed_texts, target_gateway)

    # Add specific relay breakdown if available
    if specific_relay_breakdown:
        html += generate_html_specific_relay_breakdown(specific_relay_breakdown)

    # Add direct packets section if available
    if direct_packets:
        html += generate_html_direct_packets(direct_packets)

    # Add c98c hearers section if available
    if c98c_hearers:
        html += generate_html_node_hearers(c98c_hearers, 'c98c')

    # Add Swrd hearers section if available
    if swrd_hearers:
        html += generate_html_node_hearers(swrd_hearers, 'Swrd')

    # Add mtkm hearers section if available
    if mtkm_hearers:
        html += generate_html_node_hearers(mtkm_hearers, 'mtkm')

    # Add wb1w hearers section if available
    if wb1w_hearers:
        html += generate_html_node_hearers(wb1w_hearers, 'wb1w')

    html += f"""
        <div class="footer">
            <p>Report generated from last {hours} hours of direct mesh packet receptions (excluding inter-gateway traffic).</p>
            <p>Performance Score: Weighted combination of Coverage (45%), Volume (40%), RSSI (10%), and SNR (5%).</p>
        </div>
    </body>
    </html>
    """
    return html

def generate_html_relay_type_breakdown(relay_type_data):
    """Generate HTML for relay type breakdown section."""
    if not relay_type_data:
        return ""

    html = """
        <h3 style="margin-top: 30px; color: #333;">Packet Relay Type Breakdown</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>Relay Type</th>
                    <th>Packet Count</th>
                    <th>Unique Senders</th>
                    <th>Avg RSSI</th>
                    <th>Avg SNR</th>
                    <th>Avg Hops</th>
                </tr>
            </thead>
            <tbody>
    """

    for relay_type, count, unique_senders, avg_rssi, avg_snr, avg_hops in relay_type_data:
        html += f"<tr><td>{relay_type}</td><td>{count}</td><td>{unique_senders}</td><td>{avg_rssi}</td><td>{avg_snr}</td><td>{avg_hops}</td></tr>"

    html += """
            </tbody>
        </table>
    """
    return html

def generate_html_relay_stats(relay_stats):
    """Generate HTML for relay node statistics section."""
    if not relay_stats:
        return ""

    html = """
        <h3 style="margin-top: 30px; color: #333;">Top 10 Relay Nodes</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Relay Node</th>
                    <th>Name</th>
                    <th>Packets</th>
                    <th>Match Type</th>
                    <th>Other Candidates</th>
                </tr>
            </thead>
            <tbody>
    """

    for rank, (relay_id, relay_hex, relay_name, count, match_type, candidates, hex_id) in enumerate(relay_stats, 1):
        display_node = f"!{relay_hex}" if relay_hex != '00' else '-'
        candidates_display = candidates if candidates else '-'
        relay_name_link = make_node_link(hex_id, relay_name) if hex_id and relay_hex != '00' else relay_name
        html += f"<tr><td>{rank}</td><td>{display_node}</td><td>{relay_name_link}</td><td>{count}</td><td>{match_type}</td><td>{candidates_display}</td></tr>"

    html += """
            </tbody>
        </table>
    """
    return html

def generate_html_specific_relay_breakdown(specific_relay_data):
    """Generate HTML for specific relay nodes breakdown section."""
    if not specific_relay_data:
        return ""

    html = """
        <h3 style="margin-top: 30px; color: #333;">Relay Breakdown: dmbw, dmbs, mtkm, wb1w</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>Relay Node</th>
                    <th>Hex ID</th>
                    <th>Packet Count</th>
                    <th>Unique Senders</th>
                    <th>Avg RSSI</th>
                    <th>Avg SNR</th>
                    <th>Avg Hops</th>
                </tr>
            </thead>
            <tbody>
    """

    for relay_name, hex_id, relay_byte, count, unique_senders, avg_rssi, avg_snr, avg_hops in specific_relay_data:
        rssi_display = f"{avg_rssi} dBm" if avg_rssi is not None else 'N/A'
        snr_display = f"{avg_snr} dB" if avg_snr is not None else 'N/A'
        hops_display = str(avg_hops) if avg_hops is not None else 'N/A'
        relay_name_link = make_node_link(hex_id, relay_name)
        html += f"<tr><td>{relay_name_link}</td><td>{hex_id}</td><td>{count}</td><td>{unique_senders}</td><td>{rssi_display}</td><td>{snr_display}</td><td>{hops_display}</td></tr>"

    html += """
            </tbody>
        </table>
    """
    return html

def generate_html_direct_packets(direct_packets):
    """Generate HTML for direct packets section."""
    if not direct_packets:
        return ""

    html = """
        <h3 style="margin-top: 30px; color: #333;">Direct (No Relay) Packet Records - Excluding Swrd (Last 100)</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>From Node</th>
                    <th>Time</th>
                    <th>Gateway</th>
                    <th>RSSI</th>
                    <th>SNR</th>
                    <th>Message Type</th>
                    <th>Hops</th>
                </tr>
            </thead>
            <tbody>
    """

    for row in direct_packets:
        from_node = row[0] or 'Unknown'
        from_hex = row[1]
        packet_time = row[2].strftime('%Y-%m-%d %I:%M:%S %p') if row[2] else 'N/A'
        gateway = row[3] or 'Unknown'
        gateway_hex = row[4]
        rssi = f"{row[5]} dBm" if row[5] is not None else 'N/A'
        snr = f"{round(row[6], 1)} dB" if row[6] is not None else 'N/A'
        portnum = row[7] or 'Unknown'
        hops = f"{row[9] - row[10]}" if row[9] is not None and row[10] is not None else 'N/A'

        from_node_link = make_node_link(from_hex, from_node)
        gateway_link = make_node_link(gateway_hex, gateway)

        html += f"<tr><td>{from_node_link}</td><td>{packet_time}</td><td>{gateway_link}</td><td>{rssi}</td><td>{snr}</td><td>{portnum}</td><td>{hops}</td></tr>"

    html += """
            </tbody>
        </table>
    """
    return html

def generate_html_missed_texts(missed_texts, target_gateway):
    """Generate HTML for missed text messages section."""
    if not missed_texts:
        return ""

    html = f"""
        <h3 style="margin-top: 30px; color: #333;">Text Messages Missed by {target_gateway} (Last 20)</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>From Node</th>
                    <th>Time</th>
                    <th>Message</th>
                    <th>Heard By (RSSI, SNR)</th>
                </tr>
            </thead>
            <tbody>
    """

    for row in missed_texts:
        from_node = row[0] or 'Unknown'
        from_hex = row[1]
        msg_time = row[2].strftime('%Y-%m-%d %I:%M:%S %p') if row[2] else 'N/A'
        heard_by = row[4] or 'None'
        message_text = row[5] or '[No text content]'
        # Escape HTML characters
        message_text = message_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        from_node_link = make_node_link(from_hex, from_node)
        html += f"<tr><td>{from_node_link}</td><td>{msg_time}</td><td>{message_text}</td><td>{heard_by}</td></tr>"

    html += """
            </tbody>
        </table>
    """
    return html

def generate_html_node_hearers(node_hearers_data, node_name):
    """Generate HTML for node hearers section."""
    if not node_hearers_data:
        return ""

    html = f"""
        <h3 style="margin-top: 30px; color: #333;">Gateways Hearing {node_name} (Direct + Relayed Packets)</h3>
        <table style="width: 100%; margin-top: 10px;">
            <thead>
                <tr>
                    <th>Gateway</th>
                    <th>Total Packets</th>
                    <th>Direct Packets</th>
                    <th>Relayed Packets</th>
                    <th>Avg RSSI</th>
                    <th>Avg SNR</th>
                    <th>Last Heard</th>
                </tr>
            </thead>
            <tbody>
    """

    for row in node_hearers_data:
        gateway_name = row[0] or 'Unknown'
        gateway_hex = row[1]
        total_packets = row[2]
        direct_packets = row[3]
        relayed_packets = row[4]
        avg_rssi = f"{row[5]} dBm" if row[5] is not None else 'N/A'
        avg_snr = f"{row[6]} dB" if row[6] is not None else 'N/A'
        last_heard = row[7].strftime('%Y-%m-%d %I:%M:%S %p') if row[7] else 'N/A'

        gateway_link = make_node_link(gateway_hex, gateway_name)
        html += f"<tr><td>{gateway_link}</td><td>{total_packets}</td><td>{direct_packets}</td><td>{relayed_packets}</td><td>{avg_rssi}</td><td>{avg_snr}</td><td>{last_heard}</td></tr>"

    html += """
            </tbody>
        </table>
    """
    return html

def display_terminal_table(rows, columns, hours, coverage_gaps=None, missed_texts=None, target_gateway='dmbf', relay_stats=None, relay_type_breakdown=None, direct_packets=None, specific_relay_breakdown=None, c98c_hearers=None, swrd_hearers=None, mtkm_hearers=None, wb1w_hearers=None, top_relays_per_gateway=None):
    """Display a formatted table using Rich library."""
    from datetime import timedelta

    console = Console()

    if not rows:
        console.print("[yellow]No data available for the specified time range.[/yellow]")
        return

    # Calculate time range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)

    # Create table with title
    table = Table(
        title=f"Gateway Performance Report - {end_time.strftime('%Y-%m-%d %I:%M %p')}\nTime Range: Last {hours} hours\nFrom: {start_time.strftime('%Y-%m-%d %I:%M:%S %p')} to {end_time.strftime('%Y-%m-%d %I:%M:%S %p')}",
        show_header=True,
        header_style="bold green",
        border_style="blue",
        title_style="bold white on blue",
        show_lines=False
    )

    # Define numeric columns for right alignment
    numeric_columns = ['total_direct_packets', 'total_relayed_packets', 'clean_direct_packets',
                      'clean_relayed_packets', 'total_clean_packets', 'avg_rssi', 'avg_snr', 'rssi_stddev',
                      'unique_nodes_heard', 'rssi_rank', 'snr_rank', 'coverage_rank',
                      'volume_rank', 'performance_score']

    # Custom column names for better display
    column_names = {
        'gateway_short_name': 'Gateway',
        'gateway_long_name': 'Gateway Long Name',
        'total_direct_packets': 'Total Direct',
        'total_relayed_packets': 'Total Relayed',
        'clean_direct_packets': 'Clean Direct',
        'clean_relayed_packets': 'Clean Relayed',
        'total_clean_packets': 'Total Clean',
        'avg_rssi': 'Avg RSSI',
        'avg_snr': 'Avg SNR',
        'unique_nodes_heard': 'Unique Nodes',
        'performance_score': 'Score'
    }

    # Add columns with appropriate alignment
    for col in columns:
        justify = "right" if col in numeric_columns else "left"
        col_name = column_names.get(col, col.replace('_', ' ').title())
        table.add_column(col_name, justify=justify, style="cyan", no_wrap=True)

    # Add rows with conditional styling (highlight top performer)
    for i, row in enumerate(rows):
        style = "bold yellow" if i == 0 else None
        table.add_row(*[str(val) if val is not None else 'N/A' for val in row], style=style)

    # Print table
    console.print()
    console.print(table)
    console.print()

    # Print top relays per gateway if available
    if top_relays_per_gateway:
        console.print("[bold blue]Top Relays Per Gateway (Excluding Own Infrastructure)[/bold blue]\n")

        # Organize data by gateway
        relays_by_gateway = {}
        for row in top_relays_per_gateway:
            gateway_name = row[0]
            relay_node_name = row[2]
            relay_node_hex = row[3]
            relay_type = row[4]
            packet_count = row[5]

            if gateway_name not in relays_by_gateway:
                relays_by_gateway[gateway_name] = []

            relays_by_gateway[gateway_name].append((relay_node_name, relay_node_hex, relay_type, packet_count))

        # Create table showing relays for each gateway
        relays_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        relays_table.add_column("Gateway", style="cyan", no_wrap=True)
        relays_table.add_column("Top Relays (Packets)", style="yellow")

        for gateway_name in sorted(relays_by_gateway.keys()):
            relay_list = []
            for relay_name, relay_hex, relay_type, count in relays_by_gateway[gateway_name]:
                type_indicator = " [L]" if relay_type == "Legacy" else ""
                hex_display = f"!{relay_hex}" if relay_type == "Modern" else ""
                name_display = f"{relay_name}{type_indicator}" if relay_type == "Legacy" else f"{relay_name} ({hex_display})"
                relay_list.append(f"{name_display} ({count})")

            relay_str = ", ".join(relay_list)
            relays_table.add_row(gateway_name, relay_str)

        console.print(relays_table)
        console.print("[dim]Note: [L] indicates legacy relay (likely Swrd). Hex shows relay node byte (e.g., !f6).[/dim]")
        console.print()

    # Print coverage gaps if available
    if coverage_gaps:
        console.print("[bold blue]Coverage Gaps - Top 5 Nodes Missed by Each Gateway[/bold blue]\n")

        gap_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        gap_table.add_column("Gateway", style="cyan", no_wrap=True)
        gap_table.add_column("Missed Nodes (Count, Avg RSSI, Avg SNR)", style="yellow")

        for gw_name, gaps in sorted(coverage_gaps.items()):
            if gaps:
                gap_list = ", ".join([f"{node or 'Unknown'} ({count}, {rssi}dBm, {snr}dB)"
                                     for node, hex_id, count, rssi, snr in gaps])
            else:
                gap_list = "None - heard all messages"
            gap_table.add_row(gw_name, gap_list)

        console.print(gap_table)
        console.print()

    # Print relay type breakdown if available
    if relay_type_breakdown:
        console.print("[bold blue]Packet Relay Type Breakdown[/bold blue]\n")

        breakdown_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        breakdown_table.add_column("Relay Type", style="cyan")
        breakdown_table.add_column("Packets", style="yellow", justify="right")
        breakdown_table.add_column("Unique Senders", style="yellow", justify="right")
        breakdown_table.add_column("Avg RSSI", style="green", justify="right")
        breakdown_table.add_column("Avg SNR", style="green", justify="right")
        breakdown_table.add_column("Avg Hops", style="magenta", justify="right")

        for relay_type, count, unique_senders, avg_rssi, avg_snr, avg_hops in relay_type_breakdown:
            breakdown_table.add_row(
                relay_type,
                str(count),
                str(unique_senders),
                str(avg_rssi) if avg_rssi is not None else 'N/A',
                str(avg_snr) if avg_snr is not None else 'N/A',
                str(avg_hops) if avg_hops is not None else 'N/A'
            )

        console.print(breakdown_table)
        console.print()

    # Print relay node statistics if available
    if relay_stats:
        console.print("[bold blue]Top 10 Relay Nodes[/bold blue]\n")

        relay_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        relay_table.add_column("Rank", style="cyan", justify="right", no_wrap=True)
        relay_table.add_column("Relay Node", style="cyan", no_wrap=True)
        relay_table.add_column("Name", style="white")
        relay_table.add_column("Packets", style="yellow", justify="right")
        relay_table.add_column("Match", style="green", no_wrap=True)
        relay_table.add_column("Other Candidates", style="magenta")

        for rank, (relay_id, relay_hex, relay_name, count, match_type, candidates, hex_id) in enumerate(relay_stats, 1):
            display_node = f"!{relay_hex}" if relay_hex != '00' else '-'
            candidates_display = candidates if candidates else '-'
            relay_table.add_row(str(rank), display_node, relay_name, str(count), match_type, candidates_display)

        console.print(relay_table)
        console.print()

    # Print missed text messages if available
    if missed_texts:
        console.print(f"[bold blue]Text Messages Missed by {target_gateway} (Last 20)[/bold blue]\n")

        text_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        text_table.add_column("From Node", style="cyan", no_wrap=True)
        text_table.add_column("Time", style="magenta", no_wrap=True)
        text_table.add_column("Message", style="white")
        text_table.add_column("Heard By (RSSI, SNR)", style="yellow")

        for row in missed_texts:
            from_node = row[0] or 'Unknown'
            msg_time = row[2].strftime('%Y-%m-%d %I:%M:%S %p') if row[2] else 'N/A'
            heard_by = row[4] or 'None'
            message_text = row[5] or '[No text content]'
            text_table.add_row(from_node, msg_time, message_text, heard_by)

        console.print(text_table)
        console.print()

    # Print specific relay breakdown if available
    if specific_relay_breakdown:
        console.print("[bold blue]Relay Breakdown: dmbw, dmbs, mtkm, wb1w[/bold blue]\n")

        specific_relay_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        specific_relay_table.add_column("Relay Node", style="cyan", no_wrap=True)
        specific_relay_table.add_column("Hex ID", style="yellow", no_wrap=True)
        specific_relay_table.add_column("Packets", style="green", justify="right")
        specific_relay_table.add_column("Unique Senders", style="green", justify="right")
        specific_relay_table.add_column("Avg RSSI", style="magenta", justify="right")
        specific_relay_table.add_column("Avg SNR", style="magenta", justify="right")
        specific_relay_table.add_column("Avg Hops", style="white", justify="right")

        for relay_name, hex_id, relay_byte, count, unique_senders, avg_rssi, avg_snr, avg_hops in specific_relay_breakdown:
            rssi_display = f"{avg_rssi} dBm" if avg_rssi is not None else 'N/A'
            snr_display = f"{avg_snr} dB" if avg_snr is not None else 'N/A'
            hops_display = str(avg_hops) if avg_hops is not None else 'N/A'
            specific_relay_table.add_row(
                relay_name,
                hex_id,
                str(count),
                str(unique_senders),
                rssi_display,
                snr_display,
                hops_display
            )

        console.print(specific_relay_table)
        console.print()

    # Print direct packets if available
    if direct_packets:
        console.print("[bold blue]Direct (No Relay) Packet Records - Excluding Swrd (Last 100)[/bold blue]\n")

        direct_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        direct_table.add_column("From Node", style="cyan", no_wrap=True)
        direct_table.add_column("Time", style="magenta", no_wrap=True)
        direct_table.add_column("Gateway", style="yellow", no_wrap=True)
        direct_table.add_column("RSSI", style="green", justify="right", no_wrap=True)
        direct_table.add_column("SNR", style="green", justify="right", no_wrap=True)
        direct_table.add_column("Message Type", style="white")
        direct_table.add_column("Hops", style="cyan", justify="right", no_wrap=True)

        for row in direct_packets:
            from_node = row[0] or 'Unknown'
            packet_time = row[2].strftime('%Y-%m-%d %I:%M:%S %p') if row[2] else 'N/A'
            gateway = row[3] or 'Unknown'
            rssi = f"{row[5]} dBm" if row[5] is not None else 'N/A'
            snr = f"{round(row[6], 1)} dB" if row[6] is not None else 'N/A'
            portnum = row[7] or 'Unknown'
            hops = f"{row[9] - row[10]}" if row[9] is not None and row[10] is not None else 'N/A'

            direct_table.add_row(from_node, packet_time, gateway, rssi, snr, portnum, hops)

        console.print(direct_table)
        console.print()

    # Print c98c hearers if available
    if c98c_hearers:
        console.print("[bold blue]Gateways Hearing c98c (Direct + Relayed Packets)[/bold blue]\n")

        c98c_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        c98c_table.add_column("Gateway", style="cyan", no_wrap=True)
        c98c_table.add_column("Total", style="yellow", justify="right")
        c98c_table.add_column("Direct", style="green", justify="right")
        c98c_table.add_column("Relayed", style="magenta", justify="right")
        c98c_table.add_column("Avg RSSI", style="green", justify="right")
        c98c_table.add_column("Avg SNR", style="green", justify="right")
        c98c_table.add_column("Last Heard", style="white", no_wrap=True)

        for row in c98c_hearers:
            gateway_name = row[0] or 'Unknown'
            total_packets = str(row[2])
            direct_packets = str(row[3])
            relayed_packets = str(row[4])
            avg_rssi = f"{row[5]} dBm" if row[5] is not None else 'N/A'
            avg_snr = f"{row[6]} dB" if row[6] is not None else 'N/A'
            last_heard = row[7].strftime('%Y-%m-%d %I:%M:%S %p') if row[7] else 'N/A'

            c98c_table.add_row(gateway_name, total_packets, direct_packets, relayed_packets, avg_rssi, avg_snr, last_heard)

        console.print(c98c_table)
        console.print()

    # Print Swrd hearers if available
    if swrd_hearers:
        console.print("[bold blue]Gateways Hearing Swrd (Direct + Relayed Packets)[/bold blue]\n")

        swrd_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        swrd_table.add_column("Gateway", style="cyan", no_wrap=True)
        swrd_table.add_column("Total", style="yellow", justify="right")
        swrd_table.add_column("Direct", style="green", justify="right")
        swrd_table.add_column("Relayed", style="magenta", justify="right")
        swrd_table.add_column("Avg RSSI", style="green", justify="right")
        swrd_table.add_column("Avg SNR", style="green", justify="right")
        swrd_table.add_column("Last Heard", style="white", no_wrap=True)

        for row in swrd_hearers:
            gateway_name = row[0] or 'Unknown'
            total_packets = str(row[2])
            direct_packets = str(row[3])
            relayed_packets = str(row[4])
            avg_rssi = f"{row[5]} dBm" if row[5] is not None else 'N/A'
            avg_snr = f"{row[6]} dB" if row[6] is not None else 'N/A'
            last_heard = row[7].strftime('%Y-%m-%d %I:%M:%S %p') if row[7] else 'N/A'

            swrd_table.add_row(gateway_name, total_packets, direct_packets, relayed_packets, avg_rssi, avg_snr, last_heard)

        console.print(swrd_table)
        console.print()

    # Print mtkm hearers if available
    if mtkm_hearers:
        console.print("[bold blue]Gateways Hearing mtkm (Direct + Relayed Packets)[/bold blue]\n")

        mtkm_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        mtkm_table.add_column("Gateway", style="cyan", no_wrap=True)
        mtkm_table.add_column("Total", style="yellow", justify="right")
        mtkm_table.add_column("Direct", style="green", justify="right")
        mtkm_table.add_column("Relayed", style="magenta", justify="right")
        mtkm_table.add_column("Avg RSSI", style="green", justify="right")
        mtkm_table.add_column("Avg SNR", style="green", justify="right")
        mtkm_table.add_column("Last Heard", style="white", no_wrap=True)

        for row in mtkm_hearers:
            gateway_name = row[0] or 'Unknown'
            total_packets = str(row[2])
            direct_packets = str(row[3])
            relayed_packets = str(row[4])
            avg_rssi = f"{row[5]} dBm" if row[5] is not None else 'N/A'
            avg_snr = f"{row[6]} dB" if row[6] is not None else 'N/A'
            last_heard = row[7].strftime('%Y-%m-%d %I:%M:%S %p') if row[7] else 'N/A'

            mtkm_table.add_row(gateway_name, total_packets, direct_packets, relayed_packets, avg_rssi, avg_snr, last_heard)

        console.print(mtkm_table)
        console.print()

    # Print wb1w hearers if available
    if wb1w_hearers:
        console.print("[bold blue]Gateways Hearing wb1w (Direct + Relayed Packets)[/bold blue]\n")

        wb1w_table = Table(
            show_header=True,
            header_style="bold green",
            border_style="blue",
            show_lines=False
        )
        wb1w_table.add_column("Gateway", style="cyan", no_wrap=True)
        wb1w_table.add_column("Total", style="yellow", justify="right")
        wb1w_table.add_column("Direct", style="green", justify="right")
        wb1w_table.add_column("Relayed", style="magenta", justify="right")
        wb1w_table.add_column("Avg RSSI", style="green", justify="right")
        wb1w_table.add_column("Avg SNR", style="green", justify="right")
        wb1w_table.add_column("Last Heard", style="white", no_wrap=True)

        for row in wb1w_hearers:
            gateway_name = row[0] or 'Unknown'
            total_packets = str(row[2])
            direct_packets = str(row[3])
            relayed_packets = str(row[4])
            avg_rssi = f"{row[5]} dBm" if row[5] is not None else 'N/A'
            avg_snr = f"{row[6]} dB" if row[6] is not None else 'N/A'
            last_heard = row[7].strftime('%Y-%m-%d %I:%M:%S %p') if row[7] else 'N/A'

            wb1w_table.add_row(gateway_name, total_packets, direct_packets, relayed_packets, avg_rssi, avg_snr, last_heard)

        console.print(wb1w_table)
        console.print()

    # Print footer information
    console.print("[dim]Report generated from direct mesh packet receptions (excluding inter-gateway traffic).[/dim]")
    console.print("[dim]Performance Score: Weighted combination of Coverage (45%), Volume (40%), RSSI (10%), and SNR (5%).[/dim]")
    console.print()

def send_email(subject, html_body):
    """Send email with HTML content."""
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL
    
    html_part = MIMEText(html_body, 'html')
    msg.attach(html_part)
    
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        print(f"Email sent successfully to {TO_EMAIL}")
    except Exception as e:
        print(f"Failed to send email: {e}")
        raise

def run_report_display(cursor, hours):
    """Run report generation and display in terminal."""
    # Execute query with time range
    query = get_query(hours)
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if not rows:
        print("No data returned from query")
        return

    # Get gateway list for coverage gap analysis
    gateway_list = [(row[0], row[1]) for row in rows]  # (gateway_id, gateway_short_name)

    # Get coverage gaps
    coverage_gaps = get_coverage_gaps(cursor, gateway_list, hours)

    # Get dmbf gateway hex_id for missed text messages (query directly from node_info)
    target_gateway_name = 'dmbf'
    cursor.execute("SELECT hex_id FROM node_info WHERE short_name = %s LIMIT 1", (target_gateway_name,))
    result = cursor.fetchone()
    target_gateway_hex = result[0] if result else None

    # Get missed text messages for face
    missed_texts = None
    if target_gateway_hex:
        missed_texts = get_missed_text_messages(cursor, target_gateway_hex, hours)

    # Get relay node statistics
    relay_stats = get_relay_node_stats(cursor, hours)

    # Get relay type breakdown
    relay_type_breakdown = get_relay_type_breakdown(cursor, hours)

    # Get direct packets
    direct_packets = get_direct_packets(cursor, hours)

    # Get specific relay breakdown for dmbr, dmbs, mtkm, wb1w
    specific_relay_breakdown = get_specific_relay_breakdown(cursor, hours)

    # Get node hearers for c98c, Swrd, mtkm, and wb1w
    c98c_hearers = get_node_hearers(cursor, 'c98c', hours)
    swrd_hearers = get_node_hearers(cursor, 'Swrd', hours)
    mtkm_hearers = get_node_hearers(cursor, 'mtkm', hours)
    wb1w_hearers = get_node_hearers(cursor, 'wb1w', hours)

    # Get top relays per gateway
    top_relays_per_gateway = get_top_relays_per_gateway(cursor, hours)

    # Display in terminal
    display_terminal_table(rows, columns, hours, coverage_gaps, missed_texts, target_gateway_name,
                          relay_stats, relay_type_breakdown, direct_packets,
                          specific_relay_breakdown, c98c_hearers, swrd_hearers, mtkm_hearers, wb1w_hearers,
                          top_relays_per_gateway)

def watch_mode(refresh_interval, hours):
    """Continuously display report with auto-refresh."""
    console = Console()

    try:
        while True:
            # Clear screen
            os.system('clear' if os.name == 'posix' else 'cls')

            # Show watch mode header
            console.print(f"\n[bold green]Live Monitoring Mode[/bold green] - Refreshing every {refresh_interval//60} minute(s)")
            console.print(f"[cyan]Last Update: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}[/cyan]")
            console.print(f"[yellow]Press Ctrl+C to exit[/yellow]\n")

            try:
                # Connect to database
                conn = psycopg2.connect(**DB_CONFIG)
                cursor = conn.cursor()

                # Run report display
                run_report_display(cursor, hours)

                cursor.close()
                conn.close()

            except Exception as e:
                console.print(f"[red]Error generating report: {e}[/red]")

            # Wait for next refresh
            time.sleep(refresh_interval)

    except KeyboardInterrupt:
        console.print("\n[yellow]Watch mode stopped[/yellow]")

def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate Meshtastic Gateway Performance Report')
    parser.add_argument('--display', action='store_true',
                        help='Display report in terminal instead of sending email')
    parser.add_argument('--hours', type=int, default=24,
                        help='Time range in hours for the report (default: 24)')
    parser.add_argument('--watch', action='store_true',
                        help='Enable watch mode - continuously refresh display')
    parser.add_argument('--refresh-interval', type=int, default=300,
                        help='Refresh interval in seconds for watch mode (default: 300 = 5 minutes)')

    args = parser.parse_args()

    # Check for watch mode
    if args.watch:
        # Override hours to 1 for watch mode (last hour of data)
        watch_hours = 1
        watch_mode(args.refresh_interval, watch_hours)
        return

    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Display or email based on arguments
        if args.display:
            # Display in terminal
            run_report_display(cursor, args.hours)
        else:
            # Execute query with time range
            query = get_query(args.hours)
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if not rows:
                print("No data returned from query")
                return

            # Get gateway list for coverage gap analysis
            gateway_list = [(row[0], row[1]) for row in rows]  # (gateway_id, gateway_short_name)

            # Get coverage gaps
            coverage_gaps = get_coverage_gaps(cursor, gateway_list, args.hours)

            # Get dmbf gateway hex_id for missed text messages (query directly from node_info)
            target_gateway_name = 'dmbf'
            cursor.execute("SELECT hex_id FROM node_info WHERE short_name = %s LIMIT 1", (target_gateway_name,))
            result = cursor.fetchone()
            target_gateway_hex = result[0] if result else None

            # Get missed text messages for face
            missed_texts = None
            if target_gateway_hex:
                missed_texts = get_missed_text_messages(cursor, target_gateway_hex, args.hours)

            # Get relay node statistics
            relay_stats = get_relay_node_stats(cursor, args.hours)

            # Get relay type breakdown
            relay_type_breakdown = get_relay_type_breakdown(cursor, args.hours)

            # Get direct packets
            direct_packets = get_direct_packets(cursor, args.hours)

            # Get specific relay breakdown for dmbr, dmbs, mtkm, wb1w
            specific_relay_breakdown = get_specific_relay_breakdown(cursor, args.hours)

            # Get node hearers for c98c, Swrd, mtkm, and wb1w
            c98c_hearers = get_node_hearers(cursor, 'c98c', args.hours)
            swrd_hearers = get_node_hearers(cursor, 'Swrd', args.hours)
            mtkm_hearers = get_node_hearers(cursor, 'mtkm', args.hours)
            wb1w_hearers = get_node_hearers(cursor, 'wb1w', args.hours)

            # Get top relays per gateway
            top_relays_per_gateway = get_top_relays_per_gateway(cursor, args.hours)

            # Generate HTML table and send email (default behavior)
            html_body = generate_html_table(rows, columns, args.hours, coverage_gaps, missed_texts, target_gateway_name, relay_stats, relay_type_breakdown, direct_packets, specific_relay_breakdown, c98c_hearers, swrd_hearers, mtkm_hearers, wb1w_hearers, top_relays_per_gateway)
            subject = f"Meshtastic Gateway Performance Report - {datetime.now().strftime('%Y-%m-%d')}"
            send_email(subject, html_body)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
