#!/usr/bin/env python3
"""Modbus TCP cache + write-through proxy for Huawei SUN2000 inverters.

The SUN2000's Modbus TCP interface (via the SDongle) accepts only a very small
number of concurrent connections. Point Home Assistant, a heating controller and
a dashboard at it at the same time and it starts refusing connections and
dropping reads.

This proxy sits in front of it: it holds exactly ONE connection to the inverter,
polls a configured set of registers on an interval, and serves those cached
values to as many Modbus TCP clients as you like. Write requests (FC6) are
forwarded straight through to the inverter, so control still works.

Pure Python standard library — no dependencies.

Configuration is via environment variables:
    SUN2000_HOST        inverter / SDongle address        (required)
    SUN2000_PORT        inverter Modbus port              (default 502)
    SUN2000_UNIT_IDS    Modbus unit/slave ids, comma-sep  (default 1)
    SUN2000_UNIT_ID     single unit id (fallback)         (default 1)
    LISTEN_HOST         address to serve on               (default 0.0.0.0)
    LISTEN_PORT         port to serve on                  (default 5502)
    POLL_INTERVAL       seconds between polls             (default 10)
    LOG_LEVEL           INFO / DEBUG / WARNING            (default INFO)
"""

import asyncio
import logging
import os
import signal
import struct
import time

# === Configuration (environment-driven) ===
SDONGLE_HOST = os.environ.get("SUN2000_HOST")
SDONGLE_PORT = int(os.environ.get("SUN2000_PORT", 502))
# One or more Modbus unit/slave ids to poll, comma-separated (e.g. "1,2,3,4,5").
# All listed ids share the same REGISTER_BATCHES map. SUN2000_UNIT_ID is accepted
# as a fallback so the single-slave setup keeps working unchanged.
_unit_ids = os.environ.get("SUN2000_UNIT_IDS") or os.environ.get("SUN2000_UNIT_ID", "1")
DEVICE_IDS = [int(x) for x in _unit_ids.split(",") if x.strip()]

SERVER_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("LISTEN_PORT", 5502))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 10))

if not SDONGLE_HOST:
    raise SystemExit(
        "SUN2000_HOST is not set.\n"
        "Point it at your inverter's SDongle, e.g.  SUN2000_HOST=10.0.0.50 python3 modbus_cache_server.py"
    )

# Register batches to read: (start_address, count). This same map is polled for
# every unit id in DEVICE_IDS.
# Defaults cover a SUN2000 with a LUNA2000 battery and a grid meter. Trim or
# extend to match your hardware — reading registers your inverter doesn't expose
# just logs a failed batch.
REGISTER_BATCHES = [
    (32016, 4),   # PV string 1/2 voltage + current
    (32064, 2),   # Input power (int32)
    (32069, 3),   # Phase A/B/C voltages
    (32080, 2),   # Active power (int32)
    (32085, 1),   # Grid frequency
    (32087, 1),   # Inverter temperature
    (32106, 2),   # Cumulative energy yield (uint32)
    (32114, 2),   # Daily energy yield (uint32)
    (37113, 2),   # Active grid power (int32)
    (37119, 4),   # Grid export (2) + grid import (2)
    (37760, 1),   # Battery SOC
    (37765, 2),   # Battery power (int32)
    (37780, 8),   # Battery total charge/discharge + daily
]

# === Logging ===
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("modbus_cache")

# === Register Cache ===
# Dict mapping (unit_id, register address) -> uint16 value
register_cache = {}
cache_lock = asyncio.Lock()
last_update = 0


def init_cache():
    """Seed every (unit_id, register) pair with 0."""
    for uid in DEVICE_IDS:
        for start, count in REGISTER_BATCHES:
            for i in range(count):
                register_cache[(uid, start + i)] = 0


# === SDongle Reader ===
async def read_batch(reader, writer, unit_id, start, count):
    """Read a single register batch from one slave. Returns list of values or None."""
    req = struct.pack(">HHHBBHH", 0, 0, 6, unit_id, 3, start, count)
    writer.write(req)
    await writer.drain()

    # Read full response in one go (max 9 header + 125*2 data)
    resp = await asyncio.wait_for(reader.read(9 + count * 2), timeout=3)

    if len(resp) < 9:
        return None

    resp_tx, resp_proto, resp_len, resp_unit, resp_fc, byte_count = struct.unpack(
        ">HHHBBB", resp[:9]
    )

    if resp_fc >= 0x80:  # Exception
        return None

    data = resp[9:]
    # If we didn't get all data yet, read more
    while len(data) < byte_count:
        more = await asyncio.wait_for(reader.read(byte_count - len(data)), timeout=2)
        if not more:
            break
        data += more

    if len(data) >= count * 2:
        return struct.unpack(">" + "H" * count, data[:count * 2])
    return None


async def read_sdongle():
    """Connect to SDongle and read all register batches in one session."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(SDONGLE_HOST, SDONGLE_PORT),
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"SDongle connect failed: {e}")
        return False

    success_count = 0
    fail_count = 0
    broken = False

    for uid in DEVICE_IDS:
        for start, count in REGISTER_BATCHES:
            try:
                values = await read_batch(reader, writer, uid, start, count)
                if values:
                    async with cache_lock:
                        for i, val in enumerate(values):
                            register_cache[(uid, start + i)] = val
                    success_count += 1
                else:
                    fail_count += 1
                await asyncio.sleep(0.05)  # 50ms between reads
            except asyncio.TimeoutError:
                fail_count += 1
            except Exception as e:
                logger.warning(f"Error reading unit {uid} reg {start}: {type(e).__name__}")
                fail_count += 1
                broken = True
                break  # Connection likely broken
        if broken:
            break  # socket is dead — abandon this poll, reader_loop will reconnect

    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass

    if success_count > 0:
        logger.info(f"Poll: {success_count}/{success_count + fail_count} batches OK")
    return success_count > 0


async def reader_loop():
    """Periodically read from SDongle and update cache."""
    global last_update

    # Try rapidly at first to fill cache, then slow down
    retry_delay = 5

    while True:
        success = await read_sdongle()
        if success:
            last_update = time.time()
            retry_delay = POLL_INTERVAL  # Normal interval after success
        else:
            retry_delay = min(retry_delay, 10)  # Retry faster on failure
            age = time.time() - last_update if last_update > 0 else -1
            if age > 120:
                logger.warning(f"Cache stale for {age:.0f}s")

        await asyncio.sleep(retry_delay)


# === Modbus TCP Server ===
async def handle_client(client_reader, client_writer):
    """Handle a single Modbus TCP client connection."""
    addr = client_writer.get_extra_info("peername")
    logger.info(f"Client connected: {addr}")

    try:
        while True:
            # Read MBAP header (7 bytes)
            header = await asyncio.wait_for(client_reader.read(7), timeout=60)
            if len(header) < 7:
                break  # Client disconnected

            tx_id, proto, length, unit_id = struct.unpack(">HHHB", header)

            # Read remaining PDU
            pdu = await asyncio.wait_for(client_reader.read(length - 1), timeout=5)
            if len(pdu) < 1:
                break

            fc = pdu[0]

            if fc == 3 and len(pdu) >= 5:
                # FC3: Read Holding Registers
                reg_addr, reg_count = struct.unpack(">HH", pdu[1:5])

                # Serve from cache for the slave the client addressed
                values = []
                async with cache_lock:
                    for i in range(reg_count):
                        values.append(register_cache.get((unit_id, reg_addr + i), 0))

                # Build response
                byte_count = reg_count * 2
                resp_pdu = struct.pack(">BB", fc, byte_count)
                resp_pdu += struct.pack(">" + "H" * reg_count, *values)
                resp_header = struct.pack(">HHHB", tx_id, 0, len(resp_pdu) + 1, unit_id)
                client_writer.write(resp_header + resp_pdu)
                await client_writer.drain()

            elif fc == 6 and len(pdu) >= 5:
                # FC6: Write Single Register — pass through to SDongle
                reg_addr, reg_value = struct.unpack(">HH", pdu[1:5])
                logger.info(f"Write request from {addr}: register {reg_addr} = {reg_value}")

                # Forward to SDongle
                write_ok = await forward_write(tx_id, unit_id, reg_addr, reg_value)
                if write_ok:
                    # Echo back (standard Modbus write response)
                    resp_pdu = struct.pack(">BHH", fc, reg_addr, reg_value)
                    resp_header = struct.pack(">HHHB", tx_id, 0, len(resp_pdu) + 1, unit_id)
                    client_writer.write(resp_header + resp_pdu)
                else:
                    # Error response
                    resp_pdu = struct.pack(">BB", fc + 0x80, 4)  # Device failure
                    resp_header = struct.pack(">HHHB", tx_id, 0, len(resp_pdu) + 1, unit_id)
                    client_writer.write(resp_header + resp_pdu)
                await client_writer.drain()

            else:
                # Unsupported function code — return exception
                resp_pdu = struct.pack(">BB", fc + 0x80, 1)  # Illegal function
                resp_header = struct.pack(">HHHB", tx_id, 0, len(resp_pdu) + 1, unit_id)
                client_writer.write(resp_header + resp_pdu)
                await client_writer.drain()

    except asyncio.TimeoutError:
        pass  # Client idle timeout
    except ConnectionResetError:
        pass
    except Exception as e:
        logger.warning(f"Client {addr} error: {type(e).__name__}: {e}")
    finally:
        logger.info(f"Client disconnected: {addr}")
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass


async def forward_write(tx_id, unit_id, reg_addr, reg_value):
    """Forward a write request directly to the SDongle."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(SDONGLE_HOST, SDONGLE_PORT),
            timeout=10,
        )
        req = struct.pack(">HHHBBHH", 0, 0, 6, unit_id, 6, reg_addr, reg_value)
        writer.write(req)
        await writer.drain()

        resp = await asyncio.wait_for(reader.read(12), timeout=10)
        writer.close()
        await writer.wait_closed()

        if len(resp) >= 12:
            logger.info(f"Write forwarded OK: register {reg_addr} = {reg_value}")
            return True
        else:
            logger.warning(f"Write forward: short response ({len(resp)} bytes)")
            return False

    except Exception as e:
        logger.warning(f"Write forward failed: {type(e).__name__}: {e}")
        return False


# === Main ===
async def main():
    init_cache()

    # Start reader loop
    reader_task = asyncio.create_task(reader_loop())

    # Start Modbus TCP server
    server = await asyncio.start_server(handle_client, SERVER_HOST, SERVER_PORT)
    per_unit = sum(c for _, c in REGISTER_BATCHES)
    logger.info(f"Modbus Cache Server listening on {SERVER_HOST}:{SERVER_PORT}")
    logger.info(f"Polling SDongle at {SDONGLE_HOST}:{SDONGLE_PORT} every {POLL_INTERVAL}s")
    logger.info(
        f"Caching {per_unit} registers/unit x {len(DEVICE_IDS)} unit(s) "
        f"{DEVICE_IDS} in {len(REGISTER_BATCHES)} batches each"
    )

    # Handle shutdown signals
    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    logger.info("Shutting down...")
    reader_task.cancel()
    server.close()
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
