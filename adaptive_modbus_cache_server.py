#!/usr/bin/env python3
"""Adaptive read-ahead Modbus TCP cache + write-through proxy for Huawei SUN2000.

A hybrid of the polling and on-demand variants. It *learns* from client traffic which
registers are read and how often, then keeps a warm cache by polling exactly that set
at the learned cadence — in short connect -> read -> disconnect bursts that leave the
SDongle free for its FusionSolar cloud push between reads.

    - No register map to configure. The set of polled registers is discovered from the
      FC3 requests clients actually make; the Modbus unit id is taken from each request.
    - For every requested register it tracks an EMA of the client's request period, and
      a read-ahead scheduler refreshes each register slightly faster than that, so a
      client read is almost always an instant cache hit.
    - All inverter traffic (scheduled reads, on-demand fallbacks, writes) goes through
      ONE connection, serialized by a lock, and that connection is dropped after
      IDLE_CLOSE seconds of inactivity — so the dongle is idle (free for the cloud)
      whenever nothing is due.
    - Cold or mispredicted reads fall back to fetching on demand, so the cache is never
      wrong — at worst a single read pays the inverter round-trip.
    - Writes (FC6) are relayed immediately and the register is invalidated. Each write
      logs the interval since the previous one, so you can see how often the controller
      actually writes.
    - The learned model (register set + periods) is persisted to a JSON state file
      periodically when it has changed, and reloaded at startup, so restarts don't
      cold-start.

Pure Python standard library — no dependencies.

Configuration is via environment variables:
    SUN2000_HOST        inverter / SDongle address           (required)
    SUN2000_PORT        inverter Modbus port                 (default 502)
    LISTEN_HOST         address to serve on                  (default 0.0.0.0)
    LISTEN_PORT         port to serve on                     (default 5502)
    MIN_PERIOD          floor on learned poll period, s      (default 2)
    MAX_PERIOD          ceiling on learned poll period, s    (default 60)
    READAHEAD_LEAD      refresh this fraction early (0..1)    (default 0.2)
    EVICT_AFTER         drop a register unrequested this long (default 300)
    IDLE_CLOSE          drop the inverter link after idle, s (default 5)
    RECONNECT_BACKOFF   wait after a failed connect, s        (default 5)
    STATE_FILE          learned-model JSON path              (default adaptive_cache_state.json)
    STATE_SAVE_INTERVAL flush the model every N s if changed (default 30)
    LOG_LEVEL           INFO / DEBUG / WARNING               (default INFO)
"""

import asyncio
import json
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

MIN_PERIOD = float(os.environ.get("MIN_PERIOD", 2))
MAX_PERIOD = float(os.environ.get("MAX_PERIOD", 60))
READAHEAD_LEAD = float(os.environ.get("READAHEAD_LEAD", 0.2))
EVICT_AFTER = float(os.environ.get("EVICT_AFTER", 300))
IDLE_CLOSE = float(os.environ.get("IDLE_CLOSE", 5))
RECONNECT_BACKOFF = float(os.environ.get("RECONNECT_BACKOFF", 5))
STATE_FILE = os.environ.get("STATE_FILE", "adaptive_cache_state.json")
STATE_SAVE_INTERVAL = float(os.environ.get("STATE_SAVE_INTERVAL", 30))

if not SDONGLE_HOST:
    raise SystemExit(
        "SUN2000_HOST is not set.\n"
        "Point it at your inverter's SDongle, e.g.  "
        "SUN2000_HOST=10.0.0.50 python3 adaptive_modbus_cache_server.py"
    )

# Protocol / behaviour constants
MAX_READ = 125            # Modbus FC3 maximum registers per request
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 3
WRITE_TIMEOUT = 10
CLIENT_IDLE_TIMEOUT = 300
EMA_ALPHA = 0.3           # weight of the newest interval in the period estimate
SCHED_MAX_SLEEP = 5.0     # re-evaluate at least this often (for eviction / new demand)

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
logger = logging.getLogger("adaptive_modbus_cache")


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


# === Inverter link: the ONE serialized connection, with idle-close ===
class InverterLink:
    """Owns the single connection to the inverter. All reads and writes are serialized
    through one lock so only one transaction is ever in flight; a transport error drops
    the socket (next call reconnects after a backoff); and the socket is closed after
    IDLE_CLOSE seconds of inactivity so the dongle is free for its cloud push."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._reader = None
        self._writer = None
        self._lock = asyncio.Lock()
        self._txid = 0
        self._next_connect_at = 0.0   # monotonic; don't reconnect before this
        self._last_used = 0.0         # monotonic; for idle-close

    def _next_tx(self):
        self._txid = (self._txid + 1) & 0xFFFF
        return self._txid

    async def _connect(self):
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
        w, self._writer, self._reader = self._writer, None, None
        if w is not None:
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass

    async def read(self, unit, addr, count):
        """Read `count` holding registers from `unit`. Returns a list of ints; raises
        ModbusException (inverter said no) or ConnectionError (transport / link down)."""
        async with self._lock:
            if self._writer is None:
                await self._connect()
            req = struct.pack(">HHHBBHH", self._next_tx(), 0, 6, unit, 3, addr, count)
            try:
                self._writer.write(req)
                await self._writer.drain()
                values = await self._read_fc3(count * 2)
                self._last_used = time.monotonic()
                return values
            except ModbusException:
                self._last_used = time.monotonic()
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
                await asyncio.wait_for(r.readexactly(length - 2), timeout=WRITE_TIMEOUT)
                self._last_used = time.monotonic()
                return True
            except ModbusException:
                self._last_used = time.monotonic()
                raise
            except Exception as e:
                await self._drop()
                raise ConnectionError(f"write failed: {type(e).__name__}: {e}")

    async def _read_fc3(self, expect):
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

    async def idle_closer(self):
        """Background task: drop the socket once it has been idle for IDLE_CLOSE."""
        interval = max(0.5, min(IDLE_CLOSE, 2.0))
        while True:
            await asyncio.sleep(interval)
            # Take the lock so we never drop the socket mid-transaction. Closing when idle
            # frees the dongle's single connection slot for its FusionSolar cloud push.
            async with self._lock:
                if self._writer is not None and \
                        time.monotonic() - self._last_used >= IDLE_CLOSE:
                    await self._drop()
                    logger.debug("Inverter link idle-closed")

    async def close(self):
        async with self._lock:
            await self._drop()


LINK = None  # set in main()

# === Cache & learned demand model ===
CACHE = {}                     # (unit, addr) -> (value, monotonic read_ts)
CACHE_LOCK = asyncio.Lock()
INFLIGHT = {}                  # (unit, start, count) -> Future: fetches in flight
DEMAND = {}                    # (unit, addr) -> {"last_req": ts, "period": float|None}
_demand_event = None           # asyncio.Event: wakes the scheduler on new demand
_model_dirty = False           # learned model changed since last save

_last_write_ts = 0.0           # monotonic time of the previous FC6 write
_write_count = 0


def _mark_dirty():
    global _model_dirty
    _model_dirty = True


def _eff_period(d):
    """Clamped poll period for a demand entry (MAX_PERIOD if not yet learned)."""
    p = d["period"] if (d and d["period"] is not None) else MAX_PERIOD
    return max(MIN_PERIOD, min(MAX_PERIOD, p))


def _record_demand(unit, addr, count, now):
    """Update the learned model from a client FC3 for [addr, addr+count) on `unit`."""
    for a in range(addr, addr + count):
        key = (unit, a)
        d = DEMAND.get(key)
        if d is None:
            # First time we've seen this register: add it and wake the scheduler so it
            # starts warming it. Period stays unknown until we observe a second request.
            DEMAND[key] = {"last_req": now, "period": None}
            _mark_dirty()
            if _demand_event is not None:
                _demand_event.set()
            continue
        # Learn the client's cadence: seed the period from the first observed gap, then
        # smooth later gaps with an EMA. Two clients reading the same register make the
        # gaps shorter, so the period naturally tracks the fastest consumer.
        gap = now - d["last_req"]
        d["last_req"] = now
        if 0 < gap <= EVICT_AFTER:  # ignore zero (simultaneous clients) / absurd gaps
            d["period"] = gap if d["period"] is None else \
                EMA_ALPHA * gap + (1 - EMA_ALPHA) * d["period"]
            _mark_dirty()


def _contiguous_runs(addrs, max_run):
    """Group a sorted list of addresses into (start, count) runs, splitting on gaps and
    at `max_run` registers."""
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
    identical fetches (scheduler + client fallbacks) into one inverter transaction."""
    # Coalesce on the exact (unit, start, count): concurrent identical fetches — the
    # scheduler and a client fallback, or two clients — share one inverter transaction.
    key = (unit, start, count)
    existing = INFLIGHT.get(key)
    if existing is not None:
        return await existing

    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    INFLIGHT[key] = fut
    try:
        values = await LINK.read(unit, start, count)
    except BaseException as e:
        fut.set_exception(e)
        INFLIGHT.pop(key, None)
        fut.exception()  # mark retrieved so asyncio doesn't warn when there are no waiters
        raise
    fut.set_result(values)
    INFLIGHT.pop(key, None)
    return values


async def _store(unit, start, values):
    ts = time.monotonic()
    async with CACHE_LOCK:
        for i, v in enumerate(values):
            CACHE[(unit, start + i)] = (v, ts)


# === Read serving (with on-demand fallback) ===
async def serve_read(unit, addr, count):
    """Serve `count` registers for `unit` from cache, learning the request and reading
    through to the inverter for anything missing or older than its learned period.
    Returns the list of values or raises CannotServe."""
    now = time.monotonic()
    _record_demand(unit, addr, count, now)

    # A register is served from cache while it is younger than its learned period; the
    # scheduler normally keeps it fresher than that, so this is usually a pure cache hit.
    # Whatever is missing or stale is fetched below — the cold / mispredicted path.
    async with CACHE_LOCK:
        needed = []
        for a in range(addr, addr + count):
            key = (unit, a)
            ce = CACHE.get(key)
            if ce is None or (now - ce[1]) > _eff_period(DEMAND.get(key)):
                needed.append(a)

    fetch_failed = False
    for start, run_count in _contiguous_runs(needed, MAX_READ):
        try:
            values = await _fetch_run(unit, start, run_count)
        except ModbusException as e:
            raise CannotServe(e.code)
        except ConnectionError:
            fetch_failed = True  # link down; fall back to stale where we can
            continue
        await _store(unit, start, values)

    # Assemble the reply from cache. A register still missing here was never cached and
    # its fetch just failed (inverter unreachable) — we cannot fabricate it, so fail the
    # whole request; a stale-but-present value is served instead (see fetch_failed below).
    async with CACHE_LOCK:
        out = []
        for a in range(addr, addr + count):
            ce = CACHE.get((unit, a))
            if ce is None:
                raise CannotServe(EXC_GATEWAY_NO_RESPONSE)
            out.append(ce[0])

    if fetch_failed:
        logger.warning(f"Serving stale: unit {unit} [{addr}..{addr + count}) — inverter unreachable")
    return out


# === Read-ahead scheduler ===
async def scheduler_loop():
    """Keep the cache warm: refresh each learned register a bit faster than the client
    reads it, in contiguous bursts, and evict registers that clients stopped requesting."""
    while True:
        now = time.monotonic()

        # Evict registers nobody has asked for recently.
        for key, d in list(DEMAND.items()):
            if now - d["last_req"] > EVICT_AFTER:
                DEMAND.pop(key, None)
                async with CACHE_LOCK:
                    CACHE.pop(key, None)
                _mark_dirty()

        # Decide which registers are due for a read-ahead refresh.
        due_by_unit = {}
        next_wake = now + SCHED_MAX_SLEEP
        for key, d in list(DEMAND.items()):
            unit, a = key
            async with CACHE_LOCK:
                ce = CACHE.get(key)
            read_ts = ce[1] if ce else 0.0
            # Read-ahead: refresh a fraction of a period *before* the value would go stale
            # for its client, so the next client read finds it warm. A never-read register
            # has read_ts 0, making due_at 0, so it is fetched on the next tick.
            due_at = read_ts + _eff_period(d) * (1 - READAHEAD_LEAD)
            if due_at <= now:
                due_by_unit.setdefault(unit, []).append(a)
            else:
                next_wake = min(next_wake, due_at)  # wake exactly when the soonest is due

        # Read the due registers, one contiguous burst at a time.
        for unit, addrs in due_by_unit.items():
            addrs.sort()
            for start, run_count in _contiguous_runs(addrs, MAX_READ):
                try:
                    values = await _fetch_run(unit, start, run_count)
                except ModbusException:
                    continue          # skip an illegal range; eviction will clear it
                except ConnectionError:
                    break             # link down; retry next tick
                await _store(unit, start, values)

        # Sleep until the soonest register is due, but wake early if new demand arrives
        # (a brand-new register sets _demand_event). The floor stops busy-spinning; the
        # ceiling guarantees we re-check for evictions at least every SCHED_MAX_SLEEP.
        sleep = max(0.2, min(SCHED_MAX_SLEEP, next_wake - time.monotonic()))
        try:
            await asyncio.wait_for(_demand_event.wait(), timeout=sleep)
        except asyncio.TimeoutError:
            pass
        _demand_event.clear()


# === Learned-model persistence ===
def load_state(now):
    """Seed DEMAND from the state file so a restart doesn't cold-start. Returns count."""
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
    except (FileNotFoundError, ValueError, OSError):
        return 0
    n = 0
    for item in data.get("registers", []):
        try:
            unit, a, period = item
        except (ValueError, TypeError):
            continue
        DEMAND[(int(unit), int(a))] = {
            "last_req": now,
            "period": None if period is None else float(period),
        }
        n += 1
    return n


def save_state():
    """Atomically write the learned model to STATE_FILE."""
    global _model_dirty
    regs = [[u, a, None if d["period"] is None else round(d["period"], 2)]
            for (u, a), d in DEMAND.items()]
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump({"version": 1, "registers": regs}, f)
        os.replace(tmp, STATE_FILE)
        _model_dirty = False
    except OSError as e:
        logger.warning(f"State save failed: {e}")


async def state_saver_loop():
    while True:
        await asyncio.sleep(STATE_SAVE_INTERVAL)
        if _model_dirty:
            save_state()
            logger.debug(f"Saved learned model: {len(DEMAND)} registers -> {STATE_FILE}")


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
                break
            pdu = await asyncio.wait_for(client_reader.readexactly(length - 1), timeout=5)
            fc = pdu[0]

            if fc == 3 and len(pdu) >= 5:
                reg_addr, reg_count = struct.unpack(">HH", pdu[1:5])
                if not (1 <= reg_count <= MAX_READ):
                    client_writer.write(_frame(tx_id, unit_id,
                                               _exception_pdu(fc, EXC_ILLEGAL_DATA_VALUE)))
                    await client_writer.drain()
                    continue
                try:
                    values = await serve_read(unit_id, reg_addr, reg_count)
                    resp_pdu = (struct.pack(">BB", fc, reg_count * 2)
                                + struct.pack(">" + "H" * reg_count, *values))
                    client_writer.write(_frame(tx_id, unit_id, resp_pdu))
                except CannotServe as e:
                    client_writer.write(_frame(tx_id, unit_id, _exception_pdu(fc, e.code)))
                await client_writer.drain()

            elif fc == 6 and len(pdu) >= 5:
                reg_addr, reg_value = struct.unpack(">HH", pdu[1:5])
                _log_write(addr, unit_id, reg_addr, reg_value)
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
                client_writer.write(_frame(tx_id, unit_id,
                                           _exception_pdu(fc, EXC_ILLEGAL_FUNCTION)))
                await client_writer.drain()

    except asyncio.IncompleteReadError:
        pass
    except asyncio.TimeoutError:
        pass
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


def _log_write(peer, unit_id, reg_addr, reg_value):
    """Log an FC6 write with the interval since the previous one, so write frequency
    is visible in the logs (to confirm how often the controller actually writes)."""
    global _last_write_ts, _write_count
    now = time.monotonic()
    _write_count += 1
    if _last_write_ts:
        gap = f"{now - _last_write_ts:.1f}s since last write"
    else:
        gap = "first write"
    _last_write_ts = now
    logger.info(f"Write #{_write_count} from {peer}: unit {unit_id} "
                f"reg {reg_addr} = {reg_value} ({gap})")


# === Main ===
async def main():
    global LINK, _demand_event
    _demand_event = asyncio.Event()
    LINK = InverterLink(SDONGLE_HOST, SDONGLE_PORT)

    seeded = load_state(time.monotonic())

    server = await asyncio.start_server(handle_client, SERVER_HOST, SERVER_PORT)
    logger.info(f"Adaptive Modbus Cache Server listening on {SERVER_HOST}:{SERVER_PORT}")
    logger.info(f"Relaying to inverter {SDONGLE_HOST}:{SDONGLE_PORT}; "
                f"learned model: {seeded} registers loaded from {STATE_FILE}")

    tasks = [
        asyncio.create_task(LINK.idle_closer()),
        asyncio.create_task(scheduler_loop()),
        asyncio.create_task(state_saver_loop()),
    ]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    logger.info("Shutting down...")
    server.close()
    await server.wait_closed()
    for t in tasks:
        t.cancel()
    if _model_dirty:
        save_state()
    await LINK.close()


if __name__ == "__main__":
    asyncio.run(main())
