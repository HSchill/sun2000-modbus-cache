#!/usr/bin/env python3
"""On-demand Modbus TCP cache + write-through proxy for Huawei SUN2000 inverters.

Unlike the polling variant (`modbus_cache_server.py`), this server does NOT read a
fixed register set on a timer. It is demand-driven: it relays exactly what clients
ask for, caching each register briefly so repeated/concurrent reads don't all hit
the inverter.

    - A client FC3 read is served from cache when the requested registers are fresh
      (younger than CACHE_TTL). On a miss, the missing registers are read from the
      inverter, cached, and returned. There is no register map to configure — the
      cache discovers what's actually used.
    - All inverter traffic goes through EXACTLY ONE persistent connection, serialized
      by a lock, shared by reads and writes. This is the whole point of the proxy:
      the SUN2000/SDongle tolerates only a couple of connections, so we occupy one
      and fan out to any number of clients.
    - Concurrent identical reads are coalesced into a single inverter transaction.
    - Writes (FC6) are relayed straight through; the written register is invalidated
      afterwards so the next read reflects the new value.
    - If the inverter is briefly unreachable, previously-seen registers keep being
      served (stale, logged) instead of failing; never-seen registers return a
      Modbus "gateway target failed to respond" exception.

The unit/slave id comes from each client request and is relayed as-is, so cascaded
multi-inverter setups work with no configuration.

Pure Python standard library — no dependencies.

Configuration is via environment variables:
    SUN2000_HOST        inverter / SDongle address        (required)
    SUN2000_PORT        inverter Modbus port              (default 502)
    LISTEN_HOST         address to serve on               (default 0.0.0.0)
    LISTEN_PORT         port to serve on                  (default 5502)
    CACHE_TTL           seconds a cached register is fresh (default 10)
    RECONNECT_BACKOFF   seconds to wait after a failed connect (default 5)
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

SERVER_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("LISTEN_PORT", 5502))

CACHE_TTL = float(os.environ.get("CACHE_TTL", 10))
RECONNECT_BACKOFF = float(os.environ.get("RECONNECT_BACKOFF", 5))

if not SDONGLE_HOST:
    raise SystemExit(
        "SUN2000_HOST is not set.\n"
        "Point it at your inverter's SDongle, e.g.  "
        "SUN2000_HOST=10.0.0.50 python3 ondemand_modbus_cache_server.py"
    )

# Protocol / timeout constants
MAX_READ = 125            # Modbus FC3 maximum registers per request
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 3
WRITE_TIMEOUT = 10
CLIENT_IDLE_TIMEOUT = 300

# Modbus exception codes we emit
EXC_ILLEGAL_FUNCTION = 0x01
EXC_ILLEGAL_DATA_VALUE = 0x03
EXC_DEVICE_FAILURE = 0x04
EXC_GATEWAY_NO_RESPONSE = 0x0B

# === Logging ===
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ondemand_modbus_cache")


# === Errors ===
class ModbusException(Exception):
    """The inverter answered with a Modbus exception PDU (connection still good)."""

    def __init__(self, code):
        super().__init__(f"modbus exception 0x{code:02X}")
        self.code = code


class CannotServe(Exception):
    """A client request cannot be satisfied; carries the Modbus code to return."""

    def __init__(self, code):
        super().__init__(f"cannot serve (0x{code:02X})")
        self.code = code


# === Inverter link: the ONE serialized connection ===
class InverterLink:
    """Owns the single connection to the inverter. All reads and writes are
    serialized through one lock so only one transaction is ever in flight, and a
    transport error drops the socket so the next call reconnects (after a backoff)."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._reader = None
        self._writer = None
        self._lock = asyncio.Lock()
        self._txid = 0
        self._next_connect_at = 0.0  # monotonic; don't reconnect before this

    def _next_tx(self):
        self._txid = (self._txid + 1) & 0xFFFF
        return self._txid

    async def _connect(self):
        """Open the connection. Caller holds the lock."""
        if time.monotonic() < self._next_connect_at:
            raise ConnectionError("inverter link in reconnect backoff")
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=CONNECT_TIMEOUT
            )
            logger.info(f"Connected to inverter {self.host}:{self.port}")
        except Exception as e:
            self._reader = self._writer = None
            self._next_connect_at = time.monotonic() + RECONNECT_BACKOFF
            raise ConnectionError(f"connect failed: {type(e).__name__}: {e}")

    async def _drop(self):
        """Close and forget the socket. Caller holds the lock."""
        w, self._writer, self._reader = self._writer, None, None
        if w is not None:
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass

    async def read(self, unit, addr, count):
        """Read `count` holding registers starting at `addr` from `unit`.
        Returns a list of ints. Raises ModbusException (inverter said no) or
        ConnectionError (transport / link down)."""
        async with self._lock:
            if self._writer is None:
                await self._connect()
            req = struct.pack(">HHHBBHH", self._next_tx(), 0, 6, unit, 3, addr, count)
            try:
                self._writer.write(req)
                await self._writer.drain()
                return await self._read_response(expect=count * 2)
            except ModbusException:
                raise  # inverter rejected the request; connection is still fine
            except Exception as e:
                await self._drop()
                raise ConnectionError(f"read failed: {type(e).__name__}: {e}")

    async def write(self, unit, addr, value):
        """Write a single register (FC6). Raises on failure."""
        async with self._lock:
            if self._writer is None:
                await self._connect()
            req = struct.pack(">HHHBBHH", self._next_tx(), 0, 6, unit, 6, addr, value)
            try:
                self._writer.write(req)
                await self._writer.drain()
                r = self._reader
                head = await asyncio.wait_for(r.readexactly(8), timeout=WRITE_TIMEOUT)
                _tx, _proto, length, _unit, fc = struct.unpack(">HHHBB", head)
                if fc >= 0x80:
                    exc = await asyncio.wait_for(r.readexactly(1), timeout=WRITE_TIMEOUT)
                    raise ModbusException(exc[0])
                # Normal echo: addr(2) + value(2) follow the fc; length counts unit+PDU.
                await asyncio.wait_for(r.readexactly(length - 2), timeout=WRITE_TIMEOUT)
                return True
            except ModbusException:
                raise
            except Exception as e:
                await self._drop()
                raise ConnectionError(f"write failed: {type(e).__name__}: {e}")

    async def _read_response(self, expect):
        """Parse one FC3 response off the wire. Caller holds the lock and has just
        sent the request. `expect` is the number of data bytes we asked for."""
        r = self._reader
        head = await asyncio.wait_for(r.readexactly(8), timeout=READ_TIMEOUT)  # MBAP + fc
        _tx, _proto, _length, _unit, fc = struct.unpack(">HHHBB", head)
        if fc >= 0x80:
            exc = await asyncio.wait_for(r.readexactly(1), timeout=READ_TIMEOUT)
            raise ModbusException(exc[0])
        byte_count = (await asyncio.wait_for(r.readexactly(1), timeout=READ_TIMEOUT))[0]
        data = await asyncio.wait_for(r.readexactly(byte_count), timeout=READ_TIMEOUT)
        if byte_count != expect:
            raise ConnectionError(f"unexpected byte count {byte_count} (wanted {expect})")
        return list(struct.unpack(">" + "H" * (byte_count // 2), data))

    async def close(self):
        async with self._lock:
            await self._drop()


LINK = None  # set in main()

# === Cache ===
# (unit_id, register address) -> (value, monotonic_timestamp)
CACHE = {}
CACHE_LOCK = asyncio.Lock()
# (unit_id, start, count) -> Future: fetches in flight, for coalescing
INFLIGHT = {}


def _fresh(entry, now):
    return entry is not None and (now - entry[1]) <= CACHE_TTL


def _contiguous_runs(addrs, max_run):
    """Group a sorted list of addresses into (start, count) runs, splitting on gaps
    and at `max_run` registers (the Modbus per-read limit)."""
    runs = []
    for a in addrs:
        if runs:
            start, cnt = runs[-1]
            if a == start + cnt and cnt < max_run:
                runs[-1] = (start, cnt + 1)
                continue
        runs.append((a, 1))
    return runs


async def _fetch_run(unit, start, count):
    """Read [start, start+count) for `unit` through the link, coalescing concurrent
    identical fetches into a single inverter transaction."""
    key = (unit, start, count)
    existing = INFLIGHT.get(key)
    if existing is not None:
        return await existing  # someone is already fetching exactly this

    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    INFLIGHT[key] = fut
    try:
        logger.debug(f"Inverter read: unit {unit} [{start}..{start + count})")
        values = await LINK.read(unit, start, count)
    except BaseException as e:
        fut.set_exception(e)
        INFLIGHT.pop(key, None)
        fut.exception()  # mark retrieved so asyncio doesn't warn when there are no waiters
        raise
    fut.set_result(values)
    INFLIGHT.pop(key, None)
    return values


async def serve_read(unit, addr, count):
    """Return `count` register values for `unit` starting at `addr`, reading through
    to the inverter for anything missing or expired. Raises CannotServe when the
    request can't be satisfied. Returns (values, served_stale)."""
    now = time.monotonic()

    async with CACHE_LOCK:
        needed = [addr + i for i in range(count)
                  if not _fresh(CACHE.get((unit, addr + i)), now)]

    fetch_failed = False
    for start, run_count in _contiguous_runs(needed, MAX_READ):
        try:
            values = await _fetch_run(unit, start, run_count)
        except ModbusException as e:
            # Inverter rejected the range (e.g. illegal address) — pass the code back.
            raise CannotServe(e.code)
        except ConnectionError:
            fetch_failed = True  # link down; fall back to stale where we can
            continue
        ts = time.monotonic()
        async with CACHE_LOCK:
            for i, v in enumerate(values):
                CACHE[(unit, start + i)] = (v, ts)

    async with CACHE_LOCK:
        out = []
        for i in range(count):
            entry = CACHE.get((unit, addr + i))
            if entry is None:
                # never seen and we couldn't reach the inverter
                raise CannotServe(EXC_GATEWAY_NO_RESPONSE)
            out.append(entry[0])

    if fetch_failed:
        logger.warning(
            f"Serving stale: unit {unit} [{addr}..{addr + count}) — inverter unreachable"
        )
    return out, fetch_failed


# === Modbus TCP server ===
def _frame(tx_id, unit_id, resp_pdu):
    return struct.pack(">HHHB", tx_id, 0, len(resp_pdu) + 1, unit_id) + resp_pdu


def _exception_pdu(fc, code):
    return struct.pack(">BB", (fc | 0x80) & 0xFF, code)


async def handle_client(client_reader, client_writer):
    """Handle a single Modbus TCP client connection."""
    addr = client_writer.get_extra_info("peername")
    logger.info(f"Client connected: {addr}")

    try:
        while True:
            try:
                header = await asyncio.wait_for(
                    client_reader.readexactly(7), timeout=CLIENT_IDLE_TIMEOUT
                )
            except asyncio.IncompleteReadError:
                break  # client closed

            tx_id, proto, length, unit_id = struct.unpack(">HHHB", header)
            if not (2 <= length <= 254):
                break  # malformed frame
            pdu = await asyncio.wait_for(client_reader.readexactly(length - 1), timeout=5)
            fc = pdu[0]

            if fc == 3 and len(pdu) >= 5:
                # FC3: Read Holding Registers
                reg_addr, reg_count = struct.unpack(">HH", pdu[1:5])
                if not (1 <= reg_count <= MAX_READ):
                    client_writer.write(_frame(tx_id, unit_id,
                                               _exception_pdu(fc, EXC_ILLEGAL_DATA_VALUE)))
                    await client_writer.drain()
                    continue
                try:
                    values, _stale = await serve_read(unit_id, reg_addr, reg_count)
                    resp_pdu = (struct.pack(">BB", fc, reg_count * 2)
                                + struct.pack(">" + "H" * reg_count, *values))
                    client_writer.write(_frame(tx_id, unit_id, resp_pdu))
                except CannotServe as e:
                    client_writer.write(_frame(tx_id, unit_id, _exception_pdu(fc, e.code)))
                await client_writer.drain()

            elif fc == 6 and len(pdu) >= 5:
                # FC6: Write Single Register — relay to the inverter
                reg_addr, reg_value = struct.unpack(">HH", pdu[1:5])
                logger.info(f"Write from {addr}: unit {unit_id} reg {reg_addr} = {reg_value}")
                try:
                    await LINK.write(unit_id, reg_addr, reg_value)
                    async with CACHE_LOCK:
                        CACHE.pop((unit_id, reg_addr), None)  # invalidate; force re-read
                    resp_pdu = struct.pack(">BHH", fc, reg_addr, reg_value)
                    client_writer.write(_frame(tx_id, unit_id, resp_pdu))
                except ModbusException as e:
                    client_writer.write(_frame(tx_id, unit_id, _exception_pdu(fc, e.code)))
                except ConnectionError:
                    client_writer.write(_frame(tx_id, unit_id,
                                               _exception_pdu(fc, EXC_DEVICE_FAILURE)))
                await client_writer.drain()

            else:
                # Unsupported function code
                client_writer.write(_frame(tx_id, unit_id,
                                           _exception_pdu(fc, EXC_ILLEGAL_FUNCTION)))
                await client_writer.drain()

    except asyncio.IncompleteReadError:
        pass  # client disconnected mid-frame
    except asyncio.TimeoutError:
        pass  # idle timeout
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


# === Main ===
async def main():
    global LINK
    LINK = InverterLink(SDONGLE_HOST, SDONGLE_PORT)

    server = await asyncio.start_server(handle_client, SERVER_HOST, SERVER_PORT)
    logger.info(f"On-demand Modbus Cache Server listening on {SERVER_HOST}:{SERVER_PORT}")
    logger.info(f"Relaying to inverter {SDONGLE_HOST}:{SDONGLE_PORT}, cache TTL {CACHE_TTL:g}s")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    logger.info("Shutting down...")
    server.close()
    await server.wait_closed()
    await LINK.close()


if __name__ == "__main__":
    asyncio.run(main())
