#!/usr/bin/env python3
"""Serialising Modbus-TCP proxy hardening a Huawei SUN2000 SDongle for shared use.

The SDongle accepts exactly ONE Modbus TCP client and one in-flight transaction at a time,
is slow, and drops idle/over-driven connections. Reduxi (172.24.1.15, writes) and Home
Assistant (172.24.1.97, reads) both need it. When the link wobbles, Reduxi loses battery
telemetry, trips its safety fallback and reverts the inverters to self-consumption — so the
whole job of this proxy is to present a rock-solid link to both clients.

Design:
  * ONE persistent upstream connection, its lifecycle fully DECOUPLED from downstream client
    sockets (clients may cycle TCP sessions every few seconds; the upstream never reconnects
    because of that). Reconnect uses exponential backoff; every drop/reconnect is logged.
  * A single FIFO queue + one worker owns the socket, so there is exactly one upstream request
    in flight, ever — no concurrency, no per-client races. Downstream reads/writes/relays all
    funnel through it.
  * Per-request min-gap; a per-transaction total-time cap so one bad register can't wedge the
    queue; retry-with-backoff on ServerBusy(0x06)/timeout; reconnect on transport error.
  * An idle keep-alive read stops the dongle silently closing the socket between bursts.
  * Deny-by-default per-source write ACL; FC 0x17 rejected without a queue slot; high-risk
    writes (47590==0) blocked unless explicitly allowlisted; unchanged writes suppressed
    (except 47083); a chronically flaky register (47112) gets shadow-acked instead of
    resent once its current value has been tried once; short-TTL read cache; nothing is
    ever altered/clamped — deny or pass.
  * Full audit: every request/response (fc, register, value, src, Modbus result), upstream
    connection-state transitions, per-transaction queue wait/depth, a periodic health line,
    and a register-audit table dumped on SIGHUP and shutdown.

Function codes: 0x03 read, 0x06 write single, 0x10 write multiple, 0x2B device id (relayed),
0x41 Huawei private/login (relayed verbatim — the client does the crypto). 0x17 is rejected.

Config is env vars (below) plus the REGISTERS / ACL / HIGH_RISK tables near the top.

    SUN2000_HOST/PORT   dongle address / port (172.24.7.128 / 502)   (HOST required)
    LISTEN_HOST/PORT    where to serve                               (0.0.0.0 / 5502)
    READ_TTL            read-cache freshness, s                      (2.0)
    MIN_GAP             min seconds between upstream requests        (0.1)
    REQ_TIMEOUT         per-attempt response wait, s                 (3.0)
    WARMUP_TIMEOUT      first read after (re)connect waits up to, s  (14)
    TXN_MAX             max s one transaction may hold the queue     (10)
                        (bounds head-of-line blocking; raised from 6s
                        after live data showed the dongle answering
                        mostly ServerBusy - not silence - so 6s often
                        wasn't enough runway to outlast a busy burst,
                        pushing ~33% of reads to a stale cache serve)
    QUEUE_MAX_WAIT      max s a request may sit queued before being  (8)
                        failed without any upstream I/O (protects
                        against a deep backlog even with TXN_MAX capped)
    CONNECT_TIMEOUT     upstream connect timeout, s                  (8)
    BACKOFF_MIN/MAX     exponential backoff bounds, s                (0.1 / 2)
                        (MAX lowered from 8: it's shared by the
                        in-transaction retry backoff, and live data
                        showed transactions idling a full 8s in one
                        backoff sleep instead of retrying sooner -
                        eating most of TXN_MAX without attempting
                        anything)
    KEEPALIVE           idle seconds before a no-op read (0=off)     (20)
    HOLD / REFRESH      write-suppression window / force-through, s  (60 / 600)
    SUPPRESS_EXCLUDE    csv registers never suppressed               (47083)
    SHADOW_ACK_REGS     csv registers with shadow-ack write handling (47112)
                        (47112: dongle sometimes silently drops it,
                        sometimes rejects it with EXC 0x01, and Reduxi
                        retries the SAME value every ~10s regardless -
                        each silent retry occupies the whole queue for
                        TXN_MAX, starving every other client. Once a
                        value has been attempted upstream once (any
                        outcome), a repeat of that exact value within
                        HOLD/REFRESH is ACKed locally without ever
                        touching the dongle. A genuinely new value
                        always goes upstream.)
    HEALTH_INTERVAL     seconds between health log lines             (60)
    DEGRADED_THRESHOLD  consecutive failures -> "degraded"           (3)
    READ_LOG_MUTE       csv src IPs whose reads are served+audited   (172.24.1.97)
                        but not logged per-read
    AUDIT_FILE          register-audit dump path                     (register_audit.tsv)
    LOG_FILE            console output is also appended here         (adaptive_cache.log)
                        ("" disables the file copy)
    LOG_LEVEL           INFO / DEBUG / WARNING                       (INFO)
"""

import asyncio
import logging
import os
import signal
import struct
import sys
import time

# === Configuration ===
DONGLE_HOST = os.environ.get("SUN2000_HOST")
DONGLE_PORT = int(os.environ.get("SUN2000_PORT", 502))
SERVER_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("LISTEN_PORT", 5502))

READ_TTL = float(os.environ.get("READ_TTL", 2.0))
MIN_GAP = float(os.environ.get("MIN_GAP", 0.1))
REQ_TIMEOUT = float(os.environ.get("REQ_TIMEOUT", 3.0))
WARMUP_TIMEOUT = float(os.environ.get("WARMUP_TIMEOUT", 14))
TXN_MAX = float(os.environ.get("TXN_MAX", 10))
QUEUE_MAX_WAIT = float(os.environ.get("QUEUE_MAX_WAIT", 8))
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", 8))
BACKOFF_MIN = float(os.environ.get("BACKOFF_MIN", 0.1))
BACKOFF_MAX = float(os.environ.get("BACKOFF_MAX", 2))
KEEPALIVE = float(os.environ.get("KEEPALIVE", 20))
KEEPALIVE_UNIT = int(os.environ.get("KEEPALIVE_UNIT", 1))
KEEPALIVE_REG = int(os.environ.get("KEEPALIVE_REG", 30000))
HOLD = float(os.environ.get("HOLD", 60))
REFRESH = float(os.environ.get("REFRESH", 600))
SUPPRESS_EXCLUDE = {int(x) for x in os.environ.get("SUPPRESS_EXCLUDE", "47083").split(",") if x.strip()}
SHADOW_ACK_REGS = {int(x) for x in os.environ.get("SHADOW_ACK_REGS", "47112").split(",") if x.strip()}
HEALTH_INTERVAL = float(os.environ.get("HEALTH_INTERVAL", 60))
DEGRADED_THRESHOLD = int(os.environ.get("DEGRADED_THRESHOLD", 3))
READ_LOG_MUTE = {s for s in os.environ.get("READ_LOG_MUTE", "172.24.1.97").split(",") if s.strip()}
AUDIT_FILE = os.environ.get("AUDIT_FILE", "register_audit.tsv")
CLIENT_IDLE_TIMEOUT = float(os.environ.get("CLIENT_IDLE_TIMEOUT", 300))
LOG_FILE = os.environ.get("LOG_FILE", "adaptive_cache.log")


class _Tee:
    """Mirror a text stream to a second file object (console + logfile)."""

    def __init__(self, stream, fh):
        self._stream = stream
        self._fh = fh

    def write(self, data):
        n = self._stream.write(data)
        try:
            self._fh.write(data)
        except Exception:
            pass
        return n

    def flush(self):
        self._stream.flush()
        try:
            self._fh.flush()
        except Exception:
            pass

    def __getattr__(self, name):
        return getattr(self._stream, name)


# Tee stdout/stderr to LOG_FILE before logging is configured, so log records (which the
# StreamHandler writes to stderr), plain prints, and uncaught tracebacks all land in the file
# as well as on the console. Set LOG_FILE="" (or /dev/null) to disable.
if LOG_FILE:
    try:
        _logfh = open(LOG_FILE, "a", buffering=1)   # line-buffered append
        sys.stdout = _Tee(sys.stdout, _logfh)
        sys.stderr = _Tee(sys.stderr, _logfh)
    except OSError as _e:
        print(f"warning: cannot open LOG_FILE {LOG_FILE!r}: {_e}", file=sys.stderr)

if not DONGLE_HOST:
    raise SystemExit("SUN2000_HOST is not set. e.g. SUN2000_HOST=172.24.7.128 python3 "
                     "adaptive_modbus_cache_server.py")

FC_READ = 0x03
FC_WRITE1 = 0x06
FC_WRITE_N = 0x10
FC_WRITE_N_23 = 0x17          # NOT supported by the SUN2000 — reject
FC_DEVICE_ID = 0x2B
FC_PRIVATE = 0x41
RELAY_FCS = {FC_DEVICE_ID, FC_PRIVATE}

MAX_READ = 125
MAX_WRITE = 123

EXC_ILLEGAL_FUNCTION = 0x01
EXC_ILLEGAL_DATA_VALUE = 0x03
EXC_DEVICE_FAILURE = 0x04
EXC_SERVER_BUSY = 0x06
EXC_GATEWAY_NO_RESPONSE = 0x0B
EXC_NAMES = {
    0x01: "IllegalFunction", 0x02: "IllegalDataAddress", 0x03: "IllegalDataValue",
    0x04: "ServerDeviceFailure", 0x05: "Acknowledge", 0x06: "ServerBusy",
    0x08: "MemoryParityError", 0x0A: "GatewayPathUnavailable", 0x0B: "GatewayTargetNoResponse",
}


def exc_name(code):
    return EXC_NAMES.get(code, f"Exc{code:#04x}")


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sun2000_proxy")

# === Register map (decoding / logging / value-based ACL) ===
REGISTERS = {
    40126: ("Fixed active power derated",        "U32",  "W",   1),
    47075: ("Maximum charging power",            "U32",  "W",   1),
    47077: ("Maximum discharging power",         "U32",  "W",   1),
    47083: ("Forcible charge/discharge period",  "U16",  "min", 1),   # COUNTDOWN — never suppress
    47086: ("Storage working mode",              "ENUM", "",    1),
    47087: ("Charge from grid function",         "ENUM", "",    1),
    47100: ("Forcible charge/discharge",         "ENUM", "",    1),
    47247: ("Forcible charge power",             "I32",  "kW",  1000),
    47249: ("Forcible discharge power",          "I32",  "kW",  1000),
    47415: ("Active power control mode",         "U16",  "",    1),
    47416: ("Maximum Feed Grid Power",           "I32",  "kW",  1000),
    47589: ("Remote charge/discharge mode",      "ENUM", "",    1),
    47590: ("Plant max charge-from-grid power",  "U32",  "kW",  1000),
}
ENUMS = {
    47100: {0: "stop", 1: "charge", 2: "discharge"},
    47087: {0: "disable", 1: "enable"},
    47589: {0: "local", 5: "three-party"},
}


def reg_int(addr, raws):
    meta = REGISTERS.get(addr)
    typ = meta[1] if meta else ("U32" if len(raws) >= 2 else "U16")
    if typ in ("U32", "I32") and len(raws) >= 2:
        v = (raws[0] << 16) | raws[1]
        if typ == "I32" and v >= 0x80000000:
            v -= 0x100000000
    else:
        v = raws[0] if raws else 0
        if typ == "I16" and v >= 0x8000:
            v -= 0x10000
    return v


def decode_reg(addr, raws):
    meta = REGISTERS.get(addr)
    v = reg_int(addr, raws)
    if not meta:
        return (f"reg{addr}", str(v))
    name, typ, unit, gain = meta
    if typ == "ENUM":
        em = ENUMS.get(addr, {})
        s = f"{v}" + (f" ({em[v]})" if v in em else "")
    else:
        dv = v / gain if gain != 1 else v
        s = f"{dv:g}" + (f" {unit}" if unit else "")
    return (name, s)


def _raw_str(raws):
    return "[" + ",".join(f"0x{r:04X}" for r in raws) + "]" if raws else "[-]"


# === Access control (deny-by-default writes; read logging mute) ===
WRITE_DENY = [
    # SUPPORTED / not enabled: {"unit": None, "regs": (47590, 47590), "value": lambda v: v == 0},
]
WRITE_ALLOW = {
    "172.24.1.15": [{"unit": None, "regs": None}],   # Reduxi EMS — may write anything (except high-risk)
    "172.24.1.97": [],                               # Home Assistant — no writes
}

# High-risk writes are blocked for EVERYONE unless the source register is explicitly opted in
# via HIGH_RISK_ALLOW, and are always logged prominently.
HIGH_RISK = [
    (lambda unit, reg, val: reg == 47590 and val == 0,
     "47590 (Plant max charge-from-grid power)=0 disables charge-from-grid"),
]
HIGH_RISK_ALLOW = {
    # "172.24.1.15": {47590},     # uncomment to let Reduxi zero 47590
}


def _overlap(rule_regs, lo, hi):
    return rule_regs is None or not (hi < rule_regs[0] or lo > rule_regs[1])


def _contains(rule_regs, lo, hi):
    return rule_regs is None or (rule_regs[0] <= lo and hi <= rule_regs[1])


def check_write_allowed(src, unit, start, count, value):
    lo, hi = start, start + count - 1
    for d in WRITE_DENY:
        if d.get("unit") in (None, unit) and _overlap(d.get("regs"), lo, hi):
            pred = d.get("value")
            if pred is None or pred(value):
                return (False, f"deny-rule regs={d.get('regs')}")
    for a in WRITE_ALLOW.get(src, []):
        if a.get("unit") in (None, unit) and _contains(a.get("regs"), lo, hi):
            return (True, "allowed")
    return (False, "no-allow-rule")


def high_risk_match(unit, reg, value):
    for pred, desc in HIGH_RISK:
        if pred(unit, reg, value):
            return desc
    return None


# === Errors ===
class ModbusException(Exception):
    def __init__(self, code):
        super().__init__(f"modbus exception {code:#04x} {exc_name(code)}")
        self.code = code


class CannotServe(Exception):
    def __init__(self, code):
        super().__init__(f"cannot serve ({code:#04x})")
        self.code = code


class ReadTimeout(Exception):
    pass


class TxnTimeout(Exception):
    """A transaction exceeded TXN_MAX and was failed to free the queue."""


# === Upstream connection (touched ONLY by the single worker) ===
class Upstream:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._reader = None
        self._writer = None
        self._rxbuf = b""
        self._txid = 0
        self.backoff = BACKOFF_MIN
        self._next_connect_at = 0.0
        self.warm = False
        self.last_txn = 0.0
        self.consecutive_failures = 0
        self.last_success = 0.0
        self.degraded = False
        self.last_latency_ms = 0.0
        self.fail_timeout = 0      # no reply within the per-attempt window
        self.fail_busy = 0         # dongle replied with EXC 0x06 ServerBusy
        self.fail_transport = 0    # connect/send/recv raised (socket-level)

    @property
    def connected(self):
        return self._writer is not None

    def _next_tx(self):
        self._txid = (self._txid + 1) & 0xFFFF
        return self._txid

    async def connect_once(self):
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=CONNECT_TIMEOUT)
            self._rxbuf = b""
            self.warm = True
            self.backoff = BACKOFF_MIN
            logger.info(f"upstream CONNECTED to {self.host}:{self.port} (warming up)")
            return True
        except Exception as e:
            self._reader = self._writer = None
            logger.warning(f"upstream connect to {self.host}:{self.port} FAILED: "
                           f"{type(e).__name__}: {e}")
            return False

    async def drop(self, reason):
        w = self._writer
        self._writer = self._reader = None
        self._rxbuf = b""
        if w is not None:
            logger.warning(f"upstream DROPPED: {reason}")
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass

    def note_fail(self):
        self.consecutive_failures += 1
        self.backoff = min(self.backoff * 2, BACKOFF_MAX)
        self._check_degraded()

    def note_ok(self):
        self.consecutive_failures = 0
        self.backoff = BACKOFF_MIN
        self.warm = False
        self.last_success = time.monotonic()
        self._check_degraded()

    def _check_degraded(self):
        deg = self.consecutive_failures >= DEGRADED_THRESHOLD
        if deg != self.degraded:
            self.degraded = deg
            if deg:
                logger.warning(f"upstream DEGRADED ({self.consecutive_failures} consecutive failures)")
            else:
                logger.info("upstream RECOVERED")

    async def read_frame(self, want_tx, timeout):
        deadline = time.monotonic() + timeout
        while True:
            while len(self._rxbuf) >= 6:
                tx, _proto, length = struct.unpack(">HHH", self._rxbuf[:6])
                if not (2 <= length <= 260):
                    raise ConnectionError(f"bad MBAP length {length}")
                total = 6 + length
                if len(self._rxbuf) < total:
                    break
                frame, self._rxbuf = self._rxbuf[:total], self._rxbuf[total:]
                if tx == want_tx:
                    return frame[6:]        # unit + PDU
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReadTimeout()
            try:
                chunk = await asyncio.wait_for(self._reader.read(4096), timeout=remaining)
            except asyncio.TimeoutError:
                raise ReadTimeout()
            if not chunk:
                raise ConnectionError("connection closed by peer")
            self._rxbuf += chunk

    async def send(self, pdu, unit):
        tx = self._next_tx()
        self._writer.write(struct.pack(">HHHB", tx, 0, len(pdu) + 1, unit) + bytes(pdu))
        await self._writer.drain()
        return tx


UP = None                       # Upstream, set in main()
QUEUE = None                    # asyncio.Queue of (pdu, unit, future, enqueue_ts)


async def submit(pdu, unit):
    """Enqueue an upstream transaction and await its response body (unit + PDU). Raises
    ModbusException-free here (callers interpret) but may raise TxnTimeout."""
    fut = asyncio.get_running_loop().create_future()
    await QUEUE.put((pdu, unit, fut, time.monotonic()))
    return await fut


async def _do_transaction(pdu, unit, deadline):
    """Executed by the worker only. Serialised send/receive with reconnect + retry within the
    transaction deadline. Returns (body, attempts). Retries ServerBusy/timeout/reconnect;
    other Modbus exceptions and success are returned verbatim."""
    attempts = 0
    txn_start = time.monotonic()
    while True:
        if time.monotonic() >= deadline:
            e = TxnTimeout()
            e.attempts = attempts
            raise e
        if not UP.connected:
            wait = UP._next_connect_at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(min(wait, max(0.0, deadline - time.monotonic()), 0.5))
                continue
            if not await UP.connect_once():
                UP.note_fail()
                UP.fail_transport += 1
                UP._next_connect_at = time.monotonic() + UP.backoff
                continue
            UP._next_connect_at = 0.0

        gap = MIN_GAP - (time.monotonic() - UP.last_txn)
        if gap > 0:
            await asyncio.sleep(gap)

        attempts += 1
        try:
            tx = await UP.send(pdu, unit)
        except Exception as e:
            await UP.drop(f"send failed: {type(e).__name__}")
            UP.note_fail()
            UP.fail_transport += 1
            continue
        UP.last_txn = time.monotonic()

        timeout = min(WARMUP_TIMEOUT if UP.warm else REQ_TIMEOUT,
                      max(0.05, deadline - time.monotonic()))
        try:
            body = await UP.read_frame(tx, timeout)
        except ReadTimeout:
            UP.note_fail()
            UP.fail_timeout += 1
            logger.debug(f"upstream no reply within {timeout:.1f}s (attempt {attempts}, "
                         f"fc=0x{pdu[0]:02X} unit={unit})")
            await asyncio.sleep(min(UP.backoff, max(0.0, deadline - time.monotonic())))
            continue
        except Exception as e:
            await UP.drop(f"recv failed: {type(e).__name__}")
            UP.note_fail()
            UP.fail_transport += 1
            continue
        UP.last_txn = time.monotonic()

        if len(body) >= 3 and body[1] >= 0x80 and body[2] == EXC_SERVER_BUSY:
            UP.note_fail()
            UP.fail_busy += 1
            logger.debug(f"upstream ServerBusy, retry (attempt {attempts}, backoff {UP.backoff:.2f}s)")
            await asyncio.sleep(min(UP.backoff, max(0.0, deadline - time.monotonic())))
            continue

        UP.note_ok()
        UP.last_latency_ms = (time.monotonic() - txn_start) * 1000
        if attempts > 1:
            logger.debug(f"upstream recovered after {attempts} attempts, "
                         f"{UP.last_latency_ms:.0f}ms total (fc=0x{pdu[0]:02X} unit={unit})")
        return body, attempts


async def upstream_worker():
    while True:
        pdu, unit, fut, enq = await QUEUE.get()
        wait_s = time.monotonic() - enq
        wait_ms = wait_s * 1000
        depth = QUEUE.qsize()
        if wait_s > QUEUE_MAX_WAIT:
            # Already stale by the time we got to it (queue was backed up behind a slow/
            # stuck transaction) - fail fast without spending any upstream I/O on it, so we
            # don't compound the backlog for whatever is still queued behind it.
            if not fut.cancelled():
                fut.set_exception(TxnTimeout())
            logger.warning(f"queue-wait exceeded ({wait_s:.1f}s > {QUEUE_MAX_WAIT:g}s) "
                           f"fc=0x{pdu[0]:02X} unit={unit} - dropped without upstream I/O")
            QUEUE.task_done()
            continue
        deadline = time.monotonic() + TXN_MAX
        attempts = 0
        try:
            body, attempts = await _do_transaction(pdu, unit, deadline)
            if not fut.cancelled():
                fut.set_result(body)
            res = f"exc{body[2]:#04x}" if (len(body) >= 3 and body[1] >= 0x80) else "ok"
        except BaseException as e:
            if not fut.cancelled():
                fut.set_exception(e)
            attempts = getattr(e, "attempts", attempts)
            res = type(e).__name__
        logger.debug(f"txn fc=0x{pdu[0]:02X} unit={unit} queue_wait={wait_ms:.0f}ms "
                     f"depth={depth} attempts={attempts} -> {res}")
        QUEUE.task_done()


async def keepalive_loop():
    if KEEPALIVE <= 0:
        return
    while True:
        await asyncio.sleep(KEEPALIVE)
        if UP.connected and (time.monotonic() - UP.last_txn) >= KEEPALIVE:
            try:
                await submit(struct.pack(">BHH", FC_READ, KEEPALIVE_REG, 1), KEEPALIVE_UNIT)
                logger.debug("upstream keep-alive read ok")
            except Exception as e:
                logger.debug(f"upstream keep-alive failed: {type(e).__name__}")


async def health_loop():
    while True:
        await asyncio.sleep(HEALTH_INTERVAL)
        age = (time.monotonic() - UP.last_success) if UP.last_success else -1
        logger.info(f"health: connected={UP.connected} degraded={UP.degraded} "
                    f"consec_fail={UP.consecutive_failures} last_ok={age:.0f}s "
                    f"last_latency={UP.last_latency_ms:.0f}ms "
                    f"queue_depth={QUEUE.qsize()} suppressed_writes={_suppressed_count} "
                    f"shadow_acks={_shadow_ack_count} "
                    f"stale_reads={_stale_count} "
                    f"fail_timeout={UP.fail_timeout} fail_busy={UP.fail_busy} "
                    f"fail_transport={UP.fail_transport}")


# Upstream verbs (all go through the single worker)
async def up_read(unit, addr, count):
    body = await submit(struct.pack(">BHH", FC_READ, addr, count), unit)
    if body[1] >= 0x80:
        raise ModbusException(body[2])
    bc = body[2]
    return list(struct.unpack(">" + "H" * (bc // 2), body[3:3 + bc]))


async def up_write(unit, addr, value):
    body = await submit(struct.pack(">BHH", FC_WRITE1, addr, value), unit)
    if body[1] >= 0x80:
        raise ModbusException(body[2])
    return True


async def up_write_multiple(unit, addr, values):
    n = len(values)
    pdu = struct.pack(">BHHB", FC_WRITE_N, addr, n, n * 2) + struct.pack(">" + "H" * n, *values)
    body = await submit(pdu, unit)
    if body[1] >= 0x80:
        raise ModbusException(body[2])
    return True


async def up_relay(unit, pdu):
    body = await submit(bytes(pdu), unit)
    return body[1:]


# === Read-through cache (short TTL; coalesces near-simultaneous identical reads) ===
CACHE = {}
CACHE_LOCK = asyncio.Lock()
INFLIGHT = {}


def _fresh(entry, now):
    return entry is not None and (now - entry[1]) <= READ_TTL


def _runs(addrs, max_run):
    runs = []
    for a in addrs:
        if runs and a == runs[-1][0] + runs[-1][1] and runs[-1][1] < max_run:
            runs[-1] = (runs[-1][0], runs[-1][1] + 1)
        else:
            runs.append((a, 1))
    return runs


async def _fetch(unit, start, count):
    key = (unit, start, count)
    existing = INFLIGHT.get(key)
    if existing is not None:
        return await existing
    fut = asyncio.get_running_loop().create_future()
    INFLIGHT[key] = fut
    try:
        values = await up_read(unit, start, count)
    except BaseException as e:
        fut.set_exception(e)
        INFLIGHT.pop(key, None)
        fut.exception()
        raise
    fut.set_result(values)
    INFLIGHT.pop(key, None)
    return values


async def serve_read(unit, start, count):
    now = time.monotonic()
    async with CACHE_LOCK:
        needed = [start + i for i in range(count)
                  if not _fresh(CACHE.get((unit, start + i)), now)]
    result = "cache"
    fail_reason = ""
    for s, c in _runs(needed, MAX_READ):
        try:
            values = await _fetch(unit, s, c)
            ts = time.monotonic()
            async with CACHE_LOCK:
                for i, v in enumerate(values):
                    CACHE[(unit, s + i)] = (v, ts)
            result = "upstream"
        except ModbusException as e:
            raise CannotServe(e.code)
        except Exception as e:
            result = "stale"
            fail_reason = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    async with CACHE_LOCK:
        out = []
        for i in range(count):
            ce = CACHE.get((unit, start + i))
            if ce is None:
                raise CannotServe(EXC_GATEWAY_NO_RESPONSE)
            out.append(ce[0])
    return out, result, fail_reason


# === Write suppression ===
LAST_WRITE = {}
_suppressed_count = 0
_stale_count = 0


def should_suppress(unit, start, count, vals, now):
    if any(start <= r < start + count for r in SUPPRESS_EXCLUDE):
        return False
    rec = LAST_WRITE.get((unit, start))
    if rec is None or rec["count"] != count or rec["vals"] != vals:
        return False
    elapsed = now - rec["ts"]
    if elapsed >= REFRESH:
        return False
    return elapsed < HOLD


# === Shadow-ack for chronically flaky registers (e.g. 47112) ===
# Unlike should_suppress (which requires a prior genuine upstream ACK as its baseline -
# useless for a register that never actually succeeds), this tracks the last value
# ATTEMPTED upstream regardless of outcome. A repeat of that same value is ACKed locally
# without ever touching the dongle, so a client's own rapid retry loop against a register
# the dongle periodically won't answer can't keep seizing the shared queue.
SHADOW_WRITE = {}
_shadow_ack_count = 0


def should_shadow_ack(unit, start, count, vals, now):
    if start not in SHADOW_ACK_REGS:
        return False
    rec = SHADOW_WRITE.get((unit, start))
    if rec is None or rec["count"] != count or rec["vals"] != vals:
        return False
    elapsed = now - rec["ts"]
    if elapsed >= REFRESH:
        return False
    return elapsed < HOLD


# === Register audit ===
AUDIT = {}


def record_audit(src, unit, fc, reg, value, result):
    now = time.time()
    key = (src, unit, fc, reg)
    e = AUDIT.get(key)
    if e is None:
        e = AUDIT[key] = {"first": now, "last": now, "count": 0, "vals": set(), "result": result}
    e["last"] = now
    e["count"] += 1
    e["result"] = result
    if value is not None and len(e["vals"]) < 200:
        e["vals"].add(value)


def dump_audit(reason):
    def ts(t):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
    lines = [f"# register audit ({reason}) generated {ts(time.time())}",
             "src\tunit\tfc\treg\tname\tcount\tfirst_seen\tlast_seen\tlast_result\tdistinct_values"]
    for (src, unit, fc, reg), e in sorted(AUDIT.items(), key=lambda kv: tuple("" if x is None else x for x in kv[0])):
        name = REGISTERS.get(reg, ("", ))[0]
        lines.append(f"{src}\t{unit}\t0x{fc:02X}\t{reg}\t{name}\t{e['count']}\t"
                     f"{ts(e['first'])}\t{ts(e['last'])}\t{e['result']}\t{sorted(e['vals'])}")
    try:
        tmp = AUDIT_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.replace(tmp, AUDIT_FILE)
        logger.info(f"register audit dumped ({reason}): {len(AUDIT)} entries -> {AUDIT_FILE}")
    except OSError as e:
        logger.warning(f"audit dump failed: {e}")


# === Modbus TCP server ===
def _reply(writer, tx_id, unit_id, pdu):
    writer.write(struct.pack(">HHHB", tx_id, 0, len(pdu) + 1, unit_id) + pdu)


def _exc_pdu(fc, code):
    return struct.pack(">BB", (fc | 0x80) & 0xFF, code)


async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    src = addr[0] if addr else "?"
    logger.info(f"client connected: {addr}")
    try:
        while True:
            try:
                header = await asyncio.wait_for(reader.readexactly(7), timeout=CLIENT_IDLE_TIMEOUT)
            except asyncio.IncompleteReadError:
                break
            tx_id, proto, length, unit_id = struct.unpack(">HHHB", header)
            if not (2 <= length <= 260):
                break
            pdu = await asyncio.wait_for(reader.readexactly(length - 1), timeout=5)
            fc = pdu[0]

            if fc == FC_READ and len(pdu) >= 5:
                await _do_read(writer, src, tx_id, unit_id, pdu)
            elif fc in (FC_WRITE1, FC_WRITE_N):
                await _do_write(writer, src, tx_id, unit_id, fc, pdu)
            elif fc in RELAY_FCS:
                await _do_relay(writer, src, tx_id, unit_id, fc, pdu)
            elif fc == FC_WRITE_N_23:
                logger.warning(f"REJECT src={src} unit={unit_id} fc=0x17 "
                               "(write-multiple 0x17 not supported by the SUN2000)")
                record_audit(src, unit_id, fc, None, None, "reject-0x17")
                _reply(writer, tx_id, unit_id, _exc_pdu(fc, EXC_ILLEGAL_FUNCTION))
            else:
                logger.warning(f"REJECT src={src} unit={unit_id} fc=0x{fc:02X} (unsupported)")
                record_audit(src, unit_id, fc, None, None, "reject-unsupported")
                _reply(writer, tx_id, unit_id, _exc_pdu(fc, EXC_ILLEGAL_FUNCTION))
            await writer.drain()
    except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionResetError):
        pass
    except Exception as e:
        logger.warning(f"client {src} error: {type(e).__name__}: {e}")
    finally:
        logger.info(f"client disconnected: {addr}")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def _do_read(writer, src, tx_id, unit, pdu):
    start, count = struct.unpack(">HH", pdu[1:5])
    if not (1 <= count <= MAX_READ):
        _reply(writer, tx_id, unit, _exc_pdu(FC_READ, EXC_ILLEGAL_DATA_VALUE))
        return
    values = None
    fail_reason = ""
    try:
        values, result, fail_reason = await serve_read(unit, start, count)
        resp = struct.pack(">BB", FC_READ, count * 2) + struct.pack(">" + "H" * count, *values)
        _reply(writer, tx_id, unit, resp)
    except CannotServe as e:
        result = f"EXC {e.code:#04x} {exc_name(e.code)}"
        _reply(writer, tx_id, unit, _exc_pdu(FC_READ, e.code))
    rng = f"{start}" if count == 1 else f"{start}..{start + count - 1}"
    if result == "stale":
        # The upstream read failed and we served a last-known cached value instead. This is a
        # health signal, not routine poll chatter - always log it, even for READ_LOG_MUTE
        # sources, so a chronically-failing register doesn't silently serve old data for hours.
        global _stale_count
        _stale_count += 1
        logger.warning(f"STALE #{_stale_count} src={src} unit={unit} fc=0x03 reg={rng} "
                       f"reason={fail_reason or 'unknown'} queue_depth={QUEUE.qsize()} "
                       f"(upstream read failed, served last-known cached value)")
    elif src not in READ_LOG_MUTE:      # noisy pollers (HA) are served + audited, not logged
        name, dec = decode_reg(start, values) if values else ("", "")
        logger.info(f"READ  src={src} unit={unit} fc=0x03 reg={rng} raw={_raw_str(values)}"
                    f"{(' ' + name + '=' + dec) if name else ''} result={result}")
    for i in range(count):
        record_audit(src, unit, FC_READ, start + i, values[i] if values else None, result)


async def _do_write(writer, src, tx_id, unit, fc, pdu):
    if fc == FC_WRITE1:
        if len(pdu) < 5:
            _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_ILLEGAL_DATA_VALUE))
            return
        start, = struct.unpack(">H", pdu[1:3])
        raws = [struct.unpack(">H", pdu[3:5])[0]]
        count = 1
    else:
        start, count = struct.unpack(">HH", pdu[1:5])
        byte_count = pdu[5]
        if not (1 <= count <= MAX_WRITE) or byte_count != count * 2 or len(pdu) < 6 + byte_count:
            _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_ILLEGAL_DATA_VALUE))
            return
        raws = list(struct.unpack(">" + "H" * count, pdu[6:6 + byte_count]))

    rng = f"{start}" if count == 1 else f"{start}..{start + count - 1}"
    name, dec = decode_reg(start, raws)
    value = reg_int(start, raws)
    echo = struct.pack(">BHH", fc, start, raws[0] if fc == FC_WRITE1 else count)

    # ACL (deny by default)
    allowed, reason = check_write_allowed(src, unit, start, count, value)
    if not allowed:
        logger.warning(f"DENY  src={src} unit={unit} fc=0x{fc:02X} reg={rng} raw={_raw_str(raws)} "
                       f"{name}={dec} reason={reason}")
        record_audit(src, unit, fc, start, value, "denied")
        _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_ILLEGAL_FUNCTION))
        return

    # High-risk gate (blocked unless explicitly allowlisted; only the block is logged)
    hr = high_risk_match(unit, start, value)
    if hr and start not in HIGH_RISK_ALLOW.get(src, set()):
        logger.warning(f"HIGH-RISK WRITE BLOCKED src={src} unit={unit} reg={rng} "
                       f"{name}={dec} [{hr}] (add {start} to HIGH_RISK_ALLOW[{src!r}] to permit)")
        record_audit(src, unit, fc, start, value, "high-risk-blocked")
        _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_ILLEGAL_FUNCTION))
        return

    now = time.monotonic()
    if should_suppress(unit, start, count, tuple(raws), now):
        global _suppressed_count
        _suppressed_count += 1
        logger.debug(f"SUPPRESS #{_suppressed_count} src={src} unit={unit} reg={rng} "
                     f"{name}={dec} (unchanged, within {HOLD:g}s)")
        record_audit(src, unit, fc, start, value, "suppressed")
        _reply(writer, tx_id, unit, echo)
        return

    if should_shadow_ack(unit, start, count, tuple(raws), now):
        global _shadow_ack_count
        _shadow_ack_count += 1
        logger.info(f"SHADOW-ACK #{_shadow_ack_count} src={src} unit={unit} reg={rng} "
                    f"{name}={dec} (same value already attempted upstream recently, "
                    f"not resent to dongle)")
        record_audit(src, unit, fc, start, value, "shadow-ack")
        _reply(writer, tx_id, unit, echo)
        return

    try:
        if fc == FC_WRITE1:
            await up_write(unit, start, raws[0])
        else:
            await up_write_multiple(unit, start, raws)
        async with CACHE_LOCK:
            for i in range(count):
                CACHE.pop((unit, start + i), None)
        LAST_WRITE[(unit, start)] = {"count": count, "vals": tuple(raws), "ts": now}
        _reply(writer, tx_id, unit, echo)
        result = "ACK"
    except ModbusException as e:
        result = f"EXC {e.code:#04x} {exc_name(e.code)}"
        _reply(writer, tx_id, unit, _exc_pdu(fc, e.code))
    except Exception as e:                      # TxnTimeout / cancelled / transport give-up
        result = f"FAIL {type(e).__name__}"
        _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_DEVICE_FAILURE))
    finally:
        # Remember what we attempted upstream regardless of outcome, so a client's rapid
        # retry of the SAME value against a chronically flaky register (see SHADOW_ACK_REGS)
        # doesn't keep resending - one attempt is enough to have "tried".
        if start in SHADOW_ACK_REGS:
            SHADOW_WRITE[(unit, start)] = {"count": count, "vals": tuple(raws), "ts": now}
    logger.info(f"WRITE src={src} unit={unit} fc=0x{fc:02X} reg={rng} raw={_raw_str(raws)} "
                f"{name}={dec} result={result}")
    record_audit(src, unit, fc, start, value, result)


async def _do_relay(writer, src, tx_id, unit, fc, pdu):
    kind = "device-id" if fc == FC_DEVICE_ID else "huawei-private"
    try:
        resp = await up_relay(unit, pdu)
        _reply(writer, tx_id, unit, resp)
        result = f"EXC {resp[1]:#04x}" if (len(resp) >= 2 and resp[0] >= 0x80) else "relayed"
    except Exception as e:
        result = f"FAIL {type(e).__name__}"
        _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_DEVICE_FAILURE))
    logger.info(f"RELAY src={src} unit={unit} fc=0x{fc:02X} ({kind}) result={result}")
    record_audit(src, unit, fc, None, None, result)


# === Main ===
async def main():
    global UP, QUEUE
    UP = Upstream(DONGLE_HOST, DONGLE_PORT)
    QUEUE = asyncio.Queue()

    server = await asyncio.start_server(handle_client, SERVER_HOST, SERVER_PORT)
    logger.info(f"SUN2000 proxy listening on {SERVER_HOST}:{SERVER_PORT}; upstream "
                f"{DONGLE_HOST}:{DONGLE_PORT} (min-gap {MIN_GAP*1000:.0f}ms, read-ttl {READ_TTL:g}s, "
                f"txn-cap {TXN_MAX:g}s, queue-wait-cap {QUEUE_MAX_WAIT:g}s, "
                f"keepalive {KEEPALIVE:g}s, suppress {HOLD:g}/{REFRESH:g}s, "
                f"audit -> {AUDIT_FILE}"
                f"{', log -> ' + LOG_FILE if LOG_FILE else ''})")

    tasks = [asyncio.create_task(upstream_worker()),
             asyncio.create_task(keepalive_loop()),
             asyncio.create_task(health_loop())]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGHUP, lambda: dump_audit("SIGHUP"))
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    logger.info("shutting down...")
    dump_audit("shutdown")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    server.close()
    await server.wait_closed()
    if UP.connected:
        await UP.drop("shutdown")


if __name__ == "__main__":
    asyncio.run(main())
