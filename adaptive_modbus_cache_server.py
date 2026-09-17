#!/usr/bin/env python3
"""Decoupling Modbus-TCP proxy hardening a Huawei SUN2000 SDongle for shared use.

The SDongle accepts exactly ONE Modbus TCP client and one in-flight transaction at a time,
is slow, and is frequently "too busy to answer" (it replies EXC 0x06 ServerBusy far more
often than it goes silent, and essentially never drops TCP). Reduxi (172.24.1.15, EMS
control writes) and Home Assistant (172.24.1.97, read-only telemetry) both need it.

WHAT CHANGED IN THIS REVISION (and why)
---------------------------------------
Measured over 39h of the previous "serialising" revision: 18,757 stale serves, 4,319
queue-wait drops, Reduxi in Communication-error ~55% of the time, and 58% of all client
reads were register 32000 which returned the identical value 23,100 times out of 23,209.
Tuning TXN_MAX/BACKOFF_MAX across four sessions moved none of the headline numbers, because
none of them changed how much is ASKED of the dongle.

So the design is inverted:

  * DECOUPLED, not just serialised. A background POLLER owns the dongle and refreshes a
    learned register plan at a fixed rate the dongle can actually sustain. Client reads are
    answered from cache IMMEDIATELY and unconditionally - never queued behind the dongle,
    never failed because the dongle is busy. Dongle health no longer maps onto client-visible
    latency, which is what made HA go "unavailable" and Reduxi raise Communication error.
  * PER-REGISTER cache lifetimes instead of one flat TTL. A rating register polled hourly and
    a battery power register polled every 5s no longer cost the same.
  * WRITES keep the queue and take priority over polling - they are the only traffic that
    genuinely must reach the device - and critical registers are retried before failing.

Everything the previous revision got right is kept: one persistent upstream connection whose
lifecycle is fully decoupled from downstream client sockets; exactly one upstream transaction
in flight; per-request min-gap; per-transaction time cap; retry/backoff on ServerBusy;
deny-by-default write ACL; FC 0x17 rejected; unchanged-write suppression (except 47083);
shadow-ack for chronically flaky registers; full register audit.

Three behaviour changes to be aware of before deploying:
  1. HIGH_RISK_REPLY defaults to "ack" (see below). The previous revision answered a blocked
     high-risk write with EXC 0x01 IllegalFunction - 44,041 times in 39h - which a control
     system quite reasonably reads as a device fault. Blocking the write is still right; the
     hard exception was not. Set HIGH_RISK_REPLY=exception to restore the old behaviour.
  2. Client reads are served from cache even when the cached value is old, rather than
     failing. Set CACHE_MAX_AGE to a number of seconds if you would rather a truly dead
     dongle surface as an error than as silently ageing data.
  3. BG_CONFIRM_REGS (default 47416, "Maximum Feed Grid Power") gets a stronger version of
     shadow-ack. Overnight data showed the dongle going silent on 89% of writes to this
     register while Reduxi retried the same value every 10-20s for hours, each retry
     monopolising the shared queue for the rest of TXN_MAX - the exact 47112 pattern, but
     unlike 47112 this register DOES reach the device ~10% of the time, so faking success
     outright (plain shadow-ack) risked a lasting mismatch between what the client was told
     and what the inverter is actually enforcing. Instead: the first attempt is always tried
     for real; if it fails, the client is ACKed immediately (so its own retry storm stops
     hitting the queue) but a low-rate background task (BG_CONFIRM_INTERVAL apart, up to
     BG_CONFIRM_MAX_ATTEMPTS) keeps trying the SAME value until it actually lands, and logs
     loudly - at WARNING - whether it eventually landed or gave up. A genuinely new value
     from the client always preempts a stale background attempt and is tried for real again.

Function codes: 0x03 read, 0x06 write single, 0x10 write multiple, 0x2B device id (relayed),
0x41 Huawei private/login (relayed verbatim - the client does the crypto). 0x17 is rejected.

Config is env vars (below) plus the REGISTERS / TTL / ACL tables near the top.

  CONNECTION
    SUN2000_HOST/PORT   dongle address / port (172.24.7.128 / 502)   (HOST required)
    LISTEN_HOST/PORT    where to serve                               (0.0.0.0 / 5502)
    CONNECT_TIMEOUT     upstream connect timeout, s                  (8)
    REQ_TIMEOUT         per-attempt response wait, s                 (3.0)
    WARMUP_TIMEOUT      first read after (re)connect waits up to, s  (14)
    MIN_GAP             min seconds between upstream requests        (0.25)
    BACKOFF_MIN/MAX     exponential backoff bounds, s                (0.1 / 2)
    KEEPALIVE           idle seconds before a no-op read (0=off)     (20)
                        (redundant when the poller is on; kept for POLL_ENABLE=0)

  SCHEDULING
    TXN_MAX             max s one transaction may hold the queue     (10)
    QUEUE_MAX_WAIT      max s a request may sit queued before being  (8)
                        failed without any upstream I/O
    POLL_ENABLE         run the background poller (1/0)              (1)
    POLL_INTERVAL       seconds between poller transactions          (0.5)
                        (the dongle's sustainable rate - raise this
                        until fail_busy collapses; 0.5 = 2 txn/s)
    POLL_MAX_RUN        max registers coalesced into one poll read   (64)
    POLL_BACKLOG_PAUSE  pause polling while the queue is deeper than (8)
    DEMAND_TTL          stop polling a register nobody has asked     (900)
                        for in this many seconds
    SERVE_FROM_CACHE    answer client reads from cache without ever  (1)
                        blocking on the dongle (1/0)
    CLIENT_READ_MAX_WAIT  hard cap on how long a client read may     (3.0)
                        block when it asks for a register we have
                        never read (the only blocking case left).
                        On expiry the client gets an exception and
                        the register is left to the poller, so the
                        next read is served from cache.
    CACHE_MAX_AGE       refuse to serve a value older than this, s   (0 = never refuse)
    READ_TTL            default cache lifetime, s (per-register      (5.0)
                        values in REG_TTL / TTL_RANGES override it)
    REG_TTL_OVERRIDES   csv reg:seconds, e.g. "32000:30,30071:3600"  ("")

  WRITES
    HOLD / REFRESH      write-suppression window / force-through, s  (60 / 600)
    SUPPRESS_EXCLUDE    csv registers never suppressed               (47083)
    SHADOW_ACK_REGS     csv registers with shadow-ack write handling (47112)
    CONFIRM_REGS        csv registers whose writes are retried       (40126,47590,47589)
    WRITE_RETRIES       extra upstream attempts for CONFIRM_REGS     (2)
    BG_CONFIRM_REGS     csv registers with background-confirm write (47416)
                        handling (see below)
    BG_CONFIRM_INTERVAL seconds between background confirm retries   (5.0)
    BG_CONFIRM_MAX_ATTEMPTS  give up (and log loudly) after this many (30)
                        background attempts for one value
    HIGH_RISK_REPLY     ack | exception - what a blocked high-risk   (ack)
                        write is answered with
    DENY_REPLY          ack | exception - same, for ACL denials      (exception)
    ACL_ALLOW_SRC       csv extra source IPs allowed to write        ("")

  LOGGING  (all of it switchable - see LOG_QUIET for the big hammer)
    LOG_LEVEL           INFO / DEBUG / WARNING                       (INFO)
    LOG_CONSOLE         write to console at all (1/0)                (1)
    LOG_FILE            also append here ("" = no file)              (adaptive_cache.log)
    LOG_FILE_MAX_MB     rotate the file at this size (0 = no rotate) (32)
    LOG_FILE_BACKUPS    how many rotated files to keep               (3)
    LOG_QUIET           1 = turn off all per-event chatter (reads,   (0)
                        writes, stale, blocked, connects, degraded)
                        and keep only health/summary + warnings
    LOG_READS           per-read lines (1/0)                         (1)
    LOG_WRITES          per-write lines (1/0)                        (1)
    LOG_RELAY           per-relay lines (1/0)                        (1)
    LOG_STALE           stale-serve lines (1/0)                      (1)
    LOG_BLOCKED         blocked/denied write lines (1/0)             (1)
    LOG_CONN            client connect/disconnect lines (1/0)        (1)
    LOG_DEGRADED        upstream DEGRADED/RECOVERED lines (1/0)      (1)
    LOG_POLL            per-poll-transaction lines (1/0)             (0)
    LOG_HEALTH          periodic health line (1/0)                   (1)
    LOG_SUMMARY         periodic rolled-up counts of whatever the    (1)
                        per-event switches above are hiding
    LOG_EVERY_N         log only every Nth of the high-volume        (1)
                        categories (reads/stale/blocked); 0 = never
    READ_LOG_MUTE       csv src IPs whose reads are served+audited   (172.24.1.97)
                        but not logged per-read
    HEALTH_INTERVAL     seconds between health/summary lines         (60)
    DEGRADED_THRESHOLD  consecutive failures -> "degraded"           (3)
    AUDIT_FILE          register-audit dump path                     (register_audit.tsv)

Whatever the switches say, anything that indicates a real problem (connect failures, upstream
drops, audit-dump failures, client errors) is always logged.
"""

import asyncio
import itertools
import logging
import logging.handlers
import os
import signal
import struct
import sys
import time


def _env_flag(name, default):
    v = os.environ.get(name)
    if v is None:
        return bool(default)
    return v.strip().lower() not in ("", "0", "false", "no", "off")


def _env_csv_int(name, default):
    return {int(x) for x in os.environ.get(name, default).split(",") if x.strip()}


# === Configuration ===
DONGLE_HOST = os.environ.get("SUN2000_HOST")
DONGLE_PORT = int(os.environ.get("SUN2000_PORT", 502))
SERVER_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("LISTEN_PORT", 5502))

READ_TTL = float(os.environ.get("READ_TTL", 5.0))
MIN_GAP = float(os.environ.get("MIN_GAP", 0.25))
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

POLL_ENABLE = _env_flag("POLL_ENABLE", True)
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", 0.5))
POLL_MAX_RUN = int(os.environ.get("POLL_MAX_RUN", 64))
POLL_BACKLOG_PAUSE = int(os.environ.get("POLL_BACKLOG_PAUSE", 8))
DEMAND_TTL = float(os.environ.get("DEMAND_TTL", 900))
SERVE_FROM_CACHE = _env_flag("SERVE_FROM_CACHE", True)
CACHE_MAX_AGE = float(os.environ.get("CACHE_MAX_AGE", 0))
CLIENT_READ_MAX_WAIT = float(os.environ.get("CLIENT_READ_MAX_WAIT", 3.0))

HOLD = float(os.environ.get("HOLD", 60))
REFRESH = float(os.environ.get("REFRESH", 600))
SUPPRESS_EXCLUDE = _env_csv_int("SUPPRESS_EXCLUDE", "47083")
SHADOW_ACK_REGS = _env_csv_int("SHADOW_ACK_REGS", "47112")
CONFIRM_REGS = _env_csv_int("CONFIRM_REGS", "40126,47590,47589")
WRITE_RETRIES = int(os.environ.get("WRITE_RETRIES", 2))
BG_CONFIRM_REGS = _env_csv_int("BG_CONFIRM_REGS", "47416")
BG_CONFIRM_INTERVAL = float(os.environ.get("BG_CONFIRM_INTERVAL", 5.0))
BG_CONFIRM_MAX_ATTEMPTS = int(os.environ.get("BG_CONFIRM_MAX_ATTEMPTS", 30))
HIGH_RISK_REPLY = os.environ.get("HIGH_RISK_REPLY", "ack").strip().lower()
DENY_REPLY = os.environ.get("DENY_REPLY", "exception").strip().lower()

HEALTH_INTERVAL = float(os.environ.get("HEALTH_INTERVAL", 60))
DEGRADED_THRESHOLD = int(os.environ.get("DEGRADED_THRESHOLD", 3))
READ_LOG_MUTE = {s for s in os.environ.get("READ_LOG_MUTE", "172.24.1.97").split(",") if s.strip()}
AUDIT_FILE = os.environ.get("AUDIT_FILE", "register_audit.tsv")
CLIENT_IDLE_TIMEOUT = float(os.environ.get("CLIENT_IDLE_TIMEOUT", 300))

# --- logging switches ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_CONSOLE = _env_flag("LOG_CONSOLE", True)
LOG_FILE = os.environ.get("LOG_FILE", "adaptive_cache.log")
LOG_FILE_MAX_MB = float(os.environ.get("LOG_FILE_MAX_MB", 32))
LOG_FILE_BACKUPS = int(os.environ.get("LOG_FILE_BACKUPS", 3))
LOG_QUIET = _env_flag("LOG_QUIET", False)
LOG_EVERY_N = int(os.environ.get("LOG_EVERY_N", 1))
LOG_HEALTH = _env_flag("LOG_HEALTH", True)
LOG_SUMMARY = _env_flag("LOG_SUMMARY", True)
# quiet mode turns the per-event categories off unless one is explicitly set
_q = not LOG_QUIET
LOG_READS = _env_flag("LOG_READS", _q)
LOG_WRITES = _env_flag("LOG_WRITES", _q)
LOG_RELAY = _env_flag("LOG_RELAY", _q)
LOG_STALE = _env_flag("LOG_STALE", _q)
LOG_BLOCKED = _env_flag("LOG_BLOCKED", _q)
LOG_CONN = _env_flag("LOG_CONN", _q)
LOG_DEGRADED = _env_flag("LOG_DEGRADED", _q)
LOG_POLL = _env_flag("LOG_POLL", False)


# === Logging setup ===
logger = logging.getLogger("sun2000_proxy")


def _setup_logging():
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    if LOG_CONSOLE:
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(fmt)
        logger.addHandler(h)
    if LOG_FILE:
        try:
            if LOG_FILE_MAX_MB > 0:
                fh = logging.handlers.RotatingFileHandler(
                    LOG_FILE, maxBytes=int(LOG_FILE_MAX_MB * 1024 * 1024),
                    backupCount=LOG_FILE_BACKUPS)
            else:
                fh = logging.FileHandler(LOG_FILE)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except OSError as e:
            print(f"warning: cannot open LOG_FILE {LOG_FILE!r}: {e}", file=sys.stderr)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())

    def _excepthook(exc_type, exc, tb):
        logger.error("uncaught exception", exc_info=(exc_type, exc, tb))
    sys.excepthook = _excepthook


_setup_logging()

if not DONGLE_HOST:
    raise SystemExit("SUN2000_HOST is not set. e.g. SUN2000_HOST=172.24.7.128 python3 "
                     "adaptive_modbus_cache_server.py")

if HIGH_RISK_REPLY not in ("ack", "exception"):
    raise SystemExit(f"HIGH_RISK_REPLY must be 'ack' or 'exception', got {HIGH_RISK_REPLY!r}")
if DENY_REPLY not in ("ack", "exception"):
    raise SystemExit(f"DENY_REPLY must be 'ack' or 'exception', got {DENY_REPLY!r}")


class Counters:
    """Everything the per-event log switches might be hiding, rolled up for the summary line."""

    def __init__(self):
        self.reads = 0
        self.reads_cache = 0
        self.reads_upstream = 0
        self.reads_aged = 0
        self.stale = 0
        self.writes_ok = 0
        self.writes_fail = 0
        self.suppressed = 0
        self.shadow_acks = 0
        self.blocked_high_risk = 0
        self.denied = 0
        self.polls = 0
        self.polls_failed = 0
        self.queue_drops = 0
        self.degraded_cycles = 0
        self.client_connects = 0
        self.write_retries = 0

    def snapshot_and_reset(self):
        d = {k: v for k, v in self.__dict__.items()}
        for k in self.__dict__:
            setattr(self, k, 0)
        return d


C = Counters()
_n_read_log = itertools.count()
_n_stale_log = itertools.count()
_n_blocked_log = itertools.count()


def _every_n(counter):
    """True if this occurrence should be logged, honouring LOG_EVERY_N."""
    if LOG_EVERY_N <= 0:
        return False
    return next(counter) % LOG_EVERY_N == 0


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

# Priorities for the upstream queue (lower runs first).
PRIO_WRITE = 0
PRIO_CLIENT_READ = 1
PRIO_POLL = 2


def exc_name(code):
    return EXC_NAMES.get(code, f"Exc{code:#04x}")


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

# === Cache lifetimes ===
# Tuned from 39h of observed traffic. The dominant cost was register 32000 (58% of all client
# reads, identical value in 23,100 of 23,209 reads) being refetched on a flat 2s TTL, and
# rating/identification registers that never change at all being treated the same way.
# Ranges are (first, last, ttl_seconds) and are checked in order; REG_TTL wins over ranges.
REG_TTL = {
    30071: 3600.0,    # rated/config value - observed 100% constant over 39h
    32000: 30.0,      # device state bitfield - 99.5% constant over 39h
    32002: 30.0,      # alarm/state word
}
TTL_RANGES = [
    (30000, 30099, 3600.0),    # identification / model / rating - static
    (32000, 32015, 30.0),      # state & alarm words
    (32016, 32079, 15.0),      # PV string voltages/currents
    (32080, 32114, 5.0),       # active power, frequency, temperature, efficiency
    (37100, 37200, 5.0),       # meter block
    (37700, 37820, 5.0),       # battery block (power, SOC)
    (38400, 38500, 30.0),      # battery pack detail (cell voltages/temps)
    (47000, 47999, 60.0),      # configuration / setpoint registers
]
for _spec in os.environ.get("REG_TTL_OVERRIDES", "").split(","):
    if _spec.strip():
        _r, _t = _spec.split(":")
        REG_TTL[int(_r)] = float(_t)


def ttl_for(reg):
    t = REG_TTL.get(reg)
    if t is not None:
        return t
    for lo, hi, val in TTL_RANGES:
        if lo <= reg <= hi:
            return val
    return READ_TTL


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
# ACL_ALLOW_SRC grants full write access to extra source IPs without editing this file —
# useful for a bench test or a temporary second controller. Still subject to the high-risk gate.
for _ip in os.environ.get("ACL_ALLOW_SRC", "").split(","):
    if _ip.strip():
        WRITE_ALLOW[_ip.strip()] = [{"unit": None, "regs": None}]

# High-risk writes are blocked for EVERYONE unless the source register is explicitly opted in
# via HIGH_RISK_ALLOW. How a blocked one is ANSWERED is HIGH_RISK_REPLY (default "ack"):
# answering EXC 0x01 made Reduxi log 44,041 device faults in 39h for a write we were
# deliberately refusing, which is worse than quietly not performing it.
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
        self.drops = 0

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
            self.drops += 1
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
                C.degraded_cycles += 1
                if LOG_DEGRADED:
                    logger.warning(f"upstream DEGRADED ({self.consecutive_failures} "
                                   "consecutive failures)")
            elif LOG_DEGRADED:
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
QUEUE = None                    # asyncio.PriorityQueue of (prio, seq, pdu, unit, future, ts)
_SEQ = itertools.count()


async def submit(pdu, unit, prio=PRIO_CLIENT_READ):
    """Enqueue an upstream transaction and await its response body (unit + PDU).
    Writes jump ahead of client reads, which jump ahead of background polling."""
    fut = asyncio.get_running_loop().create_future()
    await QUEUE.put((prio, next(_SEQ), pdu, unit, fut, time.monotonic()))
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
        prio, _seq, pdu, unit, fut, enq = await QUEUE.get()
        try:
            wait_s = time.monotonic() - enq
            depth = QUEUE.qsize()
            # A backlogged CLIENT request is dropped rather than compounding the queue; a
            # backlogged POLL is simply skipped (the poller will pick it up again when due).
            if wait_s > QUEUE_MAX_WAIT:
                C.queue_drops += 1
                if not fut.cancelled() and not fut.done():
                    fut.set_exception(TxnTimeout())
                if prio != PRIO_POLL:
                    logger.warning(f"queue-wait exceeded ({wait_s:.1f}s > {QUEUE_MAX_WAIT:g}s) "
                                   f"fc=0x{pdu[0]:02X} unit={unit} - dropped without upstream I/O")
                continue
            deadline = time.monotonic() + TXN_MAX
            attempts = 0
            try:
                body, attempts = await _do_transaction(pdu, unit, deadline)
                if not fut.cancelled() and not fut.done():
                    fut.set_result(body)
                res = f"exc{body[2]:#04x}" if (len(body) >= 3 and body[1] >= 0x80) else "ok"
            except asyncio.CancelledError:
                # Shutdown. Previously this was swallowed by the broad BaseException handler
                # below and the worker looped forever, so cancel()+gather() in main() never
                # returned and the process had to be SIGKILLed - and a still-open client could
                # even make it reconnect upstream after "shutting down...". Propagate instead.
                if not fut.cancelled() and not fut.done():
                    fut.cancel()
                raise
            except BaseException as e:
                if not fut.cancelled() and not fut.done():
                    fut.set_exception(e)
                attempts = getattr(e, "attempts", attempts)
                res = type(e).__name__
            logger.debug(f"txn prio={prio} fc=0x{pdu[0]:02X} unit={unit} "
                         f"queue_wait={wait_s * 1000:.0f}ms depth={depth} "
                         f"attempts={attempts} -> {res}")
        finally:
            QUEUE.task_done()


async def keepalive_loop():
    # Redundant while the poller is running - the poller is constantly talking to the dongle.
    if KEEPALIVE <= 0 or POLL_ENABLE:
        return
    while True:
        await asyncio.sleep(KEEPALIVE)
        if UP.connected and (time.monotonic() - UP.last_txn) >= KEEPALIVE:
            try:
                await submit(struct.pack(">BHH", FC_READ, KEEPALIVE_REG, 1), KEEPALIVE_UNIT,
                             PRIO_POLL)
                logger.debug("upstream keep-alive read ok")
            except Exception as e:
                logger.debug(f"upstream keep-alive failed: {type(e).__name__}")


def _fmt_summary(d):
    parts = [f"reads={d['reads']}(cache={d['reads_cache']} up={d['reads_upstream']} "
             f"aged={d['reads_aged']} stale={d['stale']})",
             f"writes={d['writes_ok']}ok/{d['writes_fail']}fail",
             f"retried={d['write_retries']}",
             f"suppressed={d['suppressed']}", f"shadow_ack={d['shadow_acks']}",
             f"high_risk_blocked={d['blocked_high_risk']}", f"denied={d['denied']}",
             f"polls={d['polls']}({d['polls_failed']}fail)",
             f"queue_drops={d['queue_drops']}", f"degraded_cycles={d['degraded_cycles']}",
             f"client_conn={d['client_connects']}"]
    return " ".join(parts)


async def health_loop():
    while True:
        await asyncio.sleep(HEALTH_INTERVAL)
        if LOG_HEALTH:
            age = (time.monotonic() - UP.last_success) if UP.last_success else -1
            logger.info(f"health: connected={UP.connected} degraded={UP.degraded} "
                        f"consec_fail={UP.consecutive_failures} last_ok={age:.0f}s "
                        f"last_latency={UP.last_latency_ms:.0f}ms "
                        f"queue_depth={QUEUE.qsize()} cached_regs={len(CACHE)} "
                        f"polled_regs={len(DEMAND)} drops={UP.drops} "
                        f"fail_timeout={UP.fail_timeout} fail_busy={UP.fail_busy} "
                        f"fail_transport={UP.fail_transport}")
        snap = C.snapshot_and_reset()
        if LOG_SUMMARY:
            logger.info(f"summary({HEALTH_INTERVAL:g}s): {_fmt_summary(snap)}")


# Upstream verbs (all go through the single worker)
async def up_read(unit, addr, count, prio=PRIO_CLIENT_READ):
    body = await submit(struct.pack(">BHH", FC_READ, addr, count), unit, prio)
    if body[1] >= 0x80:
        raise ModbusException(body[2])
    bc = body[2]
    return list(struct.unpack(">" + "H" * (bc // 2), body[3:3 + bc]))


async def up_write(unit, addr, value):
    body = await submit(struct.pack(">BHH", FC_WRITE1, addr, value), unit, PRIO_WRITE)
    if body[1] >= 0x80:
        raise ModbusException(body[2])
    return True


async def up_write_multiple(unit, addr, values):
    n = len(values)
    pdu = struct.pack(">BHHB", FC_WRITE_N, addr, n, n * 2) + struct.pack(">" + "H" * n, *values)
    body = await submit(pdu, unit, PRIO_WRITE)
    if body[1] >= 0x80:
        raise ModbusException(body[2])
    return True


async def up_relay(unit, pdu):
    body = await submit(bytes(pdu), unit, PRIO_CLIENT_READ)
    return body[1:]


# === Cache + demand tracking ===
CACHE = {}                 # (unit, reg) -> (value, monotonic_ts)
CACHE_LOCK = asyncio.Lock()
INFLIGHT = {}
DEMAND = {}                # (unit, reg) -> last time a client asked for it
BG_FETCHES = set()         # strong refs to shielded background _fetch() tasks - see _bg_done


def _bg_done(task):
    # asyncio.shield() only holds a WEAK reference to a bare coroutine/task passed to it
    # (documented in shield()'s own docstring): once the caller's own wait_for budget expires
    # and it stops awaiting, nothing else keeps the background fetch alive, so it can be
    # garbage-collected mid-flight - and if it then raises, there is nothing left to retrieve
    # the exception from, producing "Task exception was never retrieved". Keeping a strong
    # reference here until the task is actually done (this callback fires) prevents that; the
    # explicit .exception() call is what marks it retrieved regardless of asyncio version.
    BG_FETCHES.discard(task)
    if not task.cancelled():
        exc = task.exception()
        if exc is not None:
            logger.debug(f"background fetch failed after client stopped waiting: "
                         f"{type(exc).__name__}")


def _fresh(entry, now, ttl):
    return entry is not None and (now - entry[1]) <= ttl


def _runs(addrs, max_run):
    runs = []
    for a in addrs:
        if runs and a == runs[-1][0] + runs[-1][1] and runs[-1][1] < max_run:
            runs[-1] = (runs[-1][0], runs[-1][1] + 1)
        else:
            runs.append((a, 1))
    return runs


async def _fetch(unit, start, count, prio=PRIO_CLIENT_READ):
    key = (unit, start, count)
    existing = INFLIGHT.get(key)
    if existing is not None:
        return await asyncio.shield(existing)
    fut = asyncio.get_running_loop().create_future()
    INFLIGHT[key] = fut
    try:
        values = await up_read(unit, start, count, prio)
    except BaseException as e:
        if not fut.done():
            fut.set_exception(e)
        INFLIGHT.pop(key, None)
        fut.exception()
        raise
    if not fut.done():
        fut.set_result(values)
    INFLIGHT.pop(key, None)
    await _store(unit, start, values)
    return values


async def _store(unit, start, values):
    ts = time.monotonic()
    async with CACHE_LOCK:
        for i, v in enumerate(values):
            CACHE[(unit, start + i)] = (v, ts)


async def serve_read(unit, start, count):
    """Answer a client read.

    With SERVE_FROM_CACHE (the default) a client NEVER waits on the dongle for a register we
    already hold a value for, however old - the poller is responsible for freshness. Only a
    register we have never read at all is fetched synchronously, and only once.
    """
    now = time.monotonic()
    addrs = [start + i for i in range(count)]
    for a in addrs:
        DEMAND[(unit, a)] = now

    async with CACHE_LOCK:
        unknown = [a for a in addrs if CACHE.get((unit, a)) is None]
        stale = [a for a in addrs
                 if CACHE.get((unit, a)) is not None
                 and not _fresh(CACHE[(unit, a)], now, ttl_for(a))]

    result = "cache"
    fail_reason = ""
    # Registers we have never seen must be fetched - there is nothing to serve otherwise.
    # This is the ONLY case where a client can block, and it is bounded well below TXN_MAX:
    # a sick dongle must not turn a first-touch read into a 10s client stall (that is how HA
    # ends up "unavailable"). On expiry the register stays in DEMAND, so the poller fetches it
    # and the next client read is served from cache.
    need = list(unknown)
    if not SERVE_FROM_CACHE:
        need = sorted(set(unknown) | set(stale))
    budget = CLIENT_READ_MAX_WAIT
    for s, c in _runs(need, MAX_READ):
        t0 = time.monotonic()
        try:
            if budget > 0:
                # shield: if this client gives up, the fetch still completes in the
                # background and primes the cache for the next read instead of being
                # thrown away (which is what starved a cold cache on a busy dongle).
                # create_task + BG_FETCHES: shield() only weakly references a bare coroutine,
                # so the background task must be kept alive explicitly or it can be
                # garbage-collected mid-flight - see _bg_done.
                bg = asyncio.create_task(_fetch(unit, s, c))
                BG_FETCHES.add(bg)
                bg.add_done_callback(_bg_done)
                await asyncio.wait_for(asyncio.shield(bg), timeout=budget)
            else:
                await _fetch(unit, s, c)
            result = "upstream"
        except ModbusException as e:
            raise CannotServe(e.code)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            result = "stale"
            fail_reason = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        if CLIENT_READ_MAX_WAIT > 0:
            budget = max(0.05, budget - (time.monotonic() - t0))

    async with CACHE_LOCK:
        out = []
        oldest = 0.0
        for a in addrs:
            ce = CACHE.get((unit, a))
            if ce is None:
                raise CannotServe(EXC_GATEWAY_NO_RESPONSE)
            out.append(ce[0])
            oldest = max(oldest, now - ce[1])

    if CACHE_MAX_AGE > 0 and oldest > CACHE_MAX_AGE:
        raise CannotServe(EXC_GATEWAY_NO_RESPONSE)
    if result == "cache" and stale:
        result = "aged"          # served from cache, older than its TTL, poller behind
    return out, result, fail_reason, oldest


# === Background poller: the only thing that routinely talks to the dongle ===
async def poll_loop():
    if not POLL_ENABLE:
        return
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        # Polls are the lowest queue priority, so writes and client reads already overtake
        # them - there is no need to stand down just because the queue is non-empty, and
        # doing so starved the poller exactly when it was needed most (a busy dongle plus a
        # retrying client kept the queue permanently non-empty, so the cache never primed).
        # Only back off from a genuinely deep backlog.
        if QUEUE.qsize() > POLL_BACKLOG_PAUSE:
            continue
        now = time.monotonic()
        due = []
        for (unit, reg), asked in list(DEMAND.items()):
            if now - asked > DEMAND_TTL:
                DEMAND.pop((unit, reg), None)
                continue
            ttl = ttl_for(reg)
            ce = CACHE.get((unit, reg))
            age = (now - ce[1]) if ce else 1e9
            if age > ttl:
                due.append((age / ttl, unit, reg))
        if not due:
            continue
        # Most overdue first, then coalesce that unit's due registers into one block read.
        due.sort(reverse=True)
        _, unit, _reg = due[0]
        regs = sorted(r for (_o, u, r) in due if u == unit)
        runs = _runs(regs, POLL_MAX_RUN)
        s, c = runs[0]
        try:
            await _fetch(unit, s, c, PRIO_POLL)
            C.polls += 1
            if LOG_POLL:
                rng = f"{s}" if c == 1 else f"{s}..{s + c - 1}"
                logger.info(f"POLL  unit={unit} reg={rng} ok")
        except Exception as e:
            C.polls_failed += 1
            if LOG_POLL:
                logger.info(f"POLL  unit={unit} reg={s}..{s + c - 1} failed "
                            f"{type(e).__name__}")


# === Write suppression ===
LAST_WRITE = {}


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
SHADOW_WRITE = {}


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


# === Background-confirm for registers that DO reach the device, just unreliably (47416) ===
# Plain shadow-ack (above) is only safe when a blocked/failed write has no real consequence -
# 47112 is mostly rejected outright by the device, so faking success costs little. 47416
# ("Maximum Feed Grid Power") is different: ~10% of attempts genuinely land, so silently
# ACKing every retry forever would let the client believe a value is set that the inverter
# never actually received. Instead the client is only ACKed early once a background task is
# actively still trying to land that EXACT value - and that task keeps trying (slowly, so it
# can't reproduce the queue-starvation problem) until it succeeds or gives up, logging loudly
# either way so a lasting mismatch is never silent.
BG_CONFIRM = {}           # (unit, start) -> {"vals": tuple, "count": int, "landed": bool, "gave_up": bool}
BG_CONFIRM_TASKS = set()  # strong refs to background confirm-retry tasks - see _bg_confirm_task_done


def _bg_confirm_task_done(task):
    # Same GC-safety concern as BG_FETCHES/_bg_done: a bare asyncio.create_task result with no
    # other reference can be collected mid-flight, and an unretrieved exception on a collected
    # task logs "Task exception was never retrieved". Keep a strong ref until done, then
    # explicitly retrieve.
    BG_CONFIRM_TASKS.discard(task)
    if not task.cancelled():
        exc = task.exception()
        if exc is not None:
            logger.debug(f"background confirm task ended with: {type(exc).__name__}")


async def _bg_confirm_loop(src, unit, fc, start, raws, count, name, dec, rng):
    key = (unit, start)
    vals = tuple(raws)
    for attempt in range(1, BG_CONFIRM_MAX_ATTEMPTS + 1):
        await asyncio.sleep(BG_CONFIRM_INTERVAL)
        rec = BG_CONFIRM.get(key)
        if rec is None or rec["vals"] != vals:
            return    # superseded by a newer client write (or cleared) - abandon quietly
        try:
            await _attempt_write(unit, fc, start, raws)
            async with CACHE_LOCK:
                for i in range(count):
                    CACHE.pop((unit, start + i), None)
            LAST_WRITE[(unit, start)] = {"count": count, "vals": vals, "ts": time.monotonic()}
            rec["landed"] = True
            logger.warning(f"BG-CONFIRM landed src={src} unit={unit} reg={rng} {name}={dec} "
                           f"after {attempt} background attempt(s) - device now matches what "
                           f"the client was told")
            record_audit(src, unit, fc, start, reg_int(start, raws), "bg-confirm-landed")
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            continue
    rec = BG_CONFIRM.get(key)
    if rec is not None and rec["vals"] == vals:
        rec["gave_up"] = True
        logger.warning(f"BG-CONFIRM GAVE UP src={src} unit={unit} reg={rng} {name}={dec} after "
                       f"{BG_CONFIRM_MAX_ATTEMPTS} background attempts over "
                       f"{BG_CONFIRM_MAX_ATTEMPTS * BG_CONFIRM_INTERVAL:g}s - the device almost "
                       f"certainly does NOT have this value even though the client was told it did")
        record_audit(src, unit, fc, start, reg_int(start, raws), "bg-confirm-gave-up")


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
             "src\tunit\tfc\treg\tname\tttl_s\tcount\tfirst_seen\tlast_seen\tlast_result\tdistinct_values"]
    for (src, unit, fc, reg), e in sorted(
            AUDIT.items(), key=lambda kv: tuple("" if x is None else x for x in kv[0])):
        name = REGISTERS.get(reg, ("", ))[0]
        ttl = f"{ttl_for(reg):g}" if reg is not None else ""
        lines.append(f"{src}\t{unit}\t0x{fc:02X}\t{reg}\t{name}\t{ttl}\t{e['count']}\t"
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
CLIENTS = set()


def _reply(writer, tx_id, unit_id, pdu):
    writer.write(struct.pack(">HHHB", tx_id, 0, len(pdu) + 1, unit_id) + pdu)


def _exc_pdu(fc, code):
    return struct.pack(">BB", (fc | 0x80) & 0xFF, code)


async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    src = addr[0] if addr else "?"
    CLIENTS.add(writer)
    C.client_connects += 1
    if LOG_CONN:
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
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"client {src} error: {type(e).__name__}: {e}")
    finally:
        CLIENTS.discard(writer)
        if LOG_CONN:
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
    age = 0.0
    try:
        values, result, fail_reason, age = await serve_read(unit, start, count)
        resp = struct.pack(">BB", FC_READ, count * 2) + struct.pack(">" + "H" * count, *values)
        _reply(writer, tx_id, unit, resp)
    except CannotServe as e:
        result = f"EXC {e.code:#04x} {exc_name(e.code)}"
        _reply(writer, tx_id, unit, _exc_pdu(FC_READ, e.code))

    C.reads += 1
    if result == "cache":
        C.reads_cache += 1
    elif result == "upstream":
        C.reads_upstream += 1
    elif result == "aged":
        C.reads_aged += 1

    rng = f"{start}" if count == 1 else f"{start}..{start + count - 1}"
    if result == "stale":
        # The upstream read failed and we served a last-known cached value instead. A health
        # signal, not routine poll chatter - logged even for READ_LOG_MUTE sources.
        C.stale += 1
        if LOG_STALE and _every_n(_n_stale_log):
            logger.warning(f"STALE src={src} unit={unit} fc=0x03 reg={rng} "
                           f"reason={fail_reason or 'unknown'} age={age:.0f}s "
                           f"queue_depth={QUEUE.qsize()} "
                           f"(upstream read failed, served last-known cached value)")
    elif LOG_READS and src not in READ_LOG_MUTE and _every_n(_n_read_log):
        name, dec = decode_reg(start, values) if values else ("", "")
        logger.info(f"READ  src={src} unit={unit} fc=0x03 reg={rng} raw={_raw_str(values)}"
                    f"{(' ' + name + '=' + dec) if name else ''} result={result} "
                    f"age={age:.1f}s")
    for i in range(count):
        record_audit(src, unit, FC_READ, start + i, values[i] if values else None, result)


async def _attempt_write(unit, fc, start, raws):
    if fc == FC_WRITE1:
        await up_write(unit, start, raws[0])
    else:
        await up_write_multiple(unit, start, raws)


async def _do_bg_confirm_write(writer, tx_id, src, unit, fc, start, count, raws, value,
                                name, dec, rng, echo):
    """Write path for BG_CONFIRM_REGS (default 47416). Always tries for real first; a client
    retry of the SAME value while a background attempt is still in flight (or has already
    landed) is answered immediately with no upstream I/O. A new value always preempts and is
    tried for real again. See the BG_CONFIRM section above for why this differs from
    should_shadow_ack."""
    key = (unit, start)
    vals = tuple(raws)
    rec = BG_CONFIRM.get(key)

    if rec is not None and rec["vals"] == vals and not rec["gave_up"]:
        # Either still being chased in the background, or already landed - either way the
        # client doesn't need to resend it, and resending would just restart the chase.
        C.shadow_acks += 1
        if LOG_WRITES:
            status = "already landed" if rec["landed"] else "still retrying in the background"
            logger.info(f"BG-CONFIRM shadow-ack src={src} unit={unit} reg={rng} {name}={dec} "
                        f"({status})")
        record_audit(src, unit, fc, start, value,
                     "bg-confirm-already-landed" if rec["landed"] else "bg-confirm-shadow-ack")
        _reply(writer, tx_id, unit, echo)
        return

    # A new value (or the previous attempt for this exact value gave up) - always try it for
    # real, synchronously, exactly like a normal write.
    BG_CONFIRM[key] = {"vals": vals, "count": count, "landed": False, "gave_up": False}
    try:
        await _attempt_write(unit, fc, start, raws)
        async with CACHE_LOCK:
            for i in range(count):
                CACHE.pop((unit, start + i), None)
        LAST_WRITE[(unit, start)] = {"count": count, "vals": vals, "ts": time.monotonic()}
        BG_CONFIRM[key]["landed"] = True
        _reply(writer, tx_id, unit, echo)
        C.writes_ok += 1
        if LOG_WRITES:
            logger.info(f"WRITE src={src} unit={unit} fc=0x{fc:02X} reg={rng} raw={_raw_str(raws)} "
                        f"{name}={dec} result=ACK")
        record_audit(src, unit, fc, start, value, "ACK")
        return
    except ModbusException as e:
        # A real rejection from the device - retrying will not change its mind.
        result = f"EXC {e.code:#04x} {exc_name(e.code)}"
        _reply(writer, tx_id, unit, _exc_pdu(fc, e.code))
        C.writes_fail += 1
        if LOG_WRITES:
            logger.info(f"WRITE src={src} unit={unit} fc=0x{fc:02X} reg={rng} raw={_raw_str(raws)} "
                        f"{name}={dec} result={result}")
        record_audit(src, unit, fc, start, value, result)
        BG_CONFIRM.pop(key, None)
        return
    except asyncio.CancelledError:
        raise
    except Exception:
        pass   # TxnTimeout / transport give-up: fall through to ACK-now + background-confirm

    task = asyncio.create_task(_bg_confirm_loop(src, unit, fc, start, raws, count, name, dec, rng))
    BG_CONFIRM_TASKS.add(task)
    task.add_done_callback(_bg_confirm_task_done)
    if LOG_WRITES:
        logger.warning(f"BG-CONFIRM src={src} unit={unit} reg={rng} {name}={dec} did not land "
                       f"synchronously - ACKing client now, retrying in the background every "
                       f"{BG_CONFIRM_INTERVAL:g}s (up to {BG_CONFIRM_MAX_ATTEMPTS}x)")
    record_audit(src, unit, fc, start, value, "bg-confirm-pending")
    _reply(writer, tx_id, unit, echo)


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
        C.denied += 1
        if LOG_BLOCKED and _every_n(_n_blocked_log):
            logger.warning(f"DENY  src={src} unit={unit} fc=0x{fc:02X} reg={rng} "
                           f"raw={_raw_str(raws)} {name}={dec} reason={reason} "
                           f"(reply={DENY_REPLY})")
        record_audit(src, unit, fc, start, value, "denied")
        if DENY_REPLY == "ack":
            _reply(writer, tx_id, unit, echo)
        else:
            _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_ILLEGAL_FUNCTION))
        return

    # High-risk gate (blocked unless explicitly allowlisted)
    hr = high_risk_match(unit, start, value)
    if hr and start not in HIGH_RISK_ALLOW.get(src, set()):
        C.blocked_high_risk += 1
        if LOG_BLOCKED and _every_n(_n_blocked_log):
            logger.warning(f"HIGH-RISK WRITE BLOCKED src={src} unit={unit} reg={rng} "
                           f"{name}={dec} [{hr}] (reply={HIGH_RISK_REPLY}; add {start} to "
                           f"HIGH_RISK_ALLOW[{src!r}] to permit)")
        record_audit(src, unit, fc, start, value, "high-risk-blocked")
        if HIGH_RISK_REPLY == "ack":
            # Answer as if written, without touching the dongle. Refusing the write is the
            # point; answering EXC 0x01 to a control system just makes it log device faults
            # (44,041 of them in 39h) and retry harder.
            _reply(writer, tx_id, unit, echo)
        else:
            _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_ILLEGAL_FUNCTION))
        return

    now = time.monotonic()
    if should_suppress(unit, start, count, tuple(raws), now):
        C.suppressed += 1
        logger.debug(f"SUPPRESS src={src} unit={unit} reg={rng} {name}={dec} "
                     f"(unchanged, within {HOLD:g}s)")
        record_audit(src, unit, fc, start, value, "suppressed")
        _reply(writer, tx_id, unit, echo)
        return

    if should_shadow_ack(unit, start, count, tuple(raws), now):
        C.shadow_acks += 1
        if LOG_WRITES:
            logger.info(f"SHADOW-ACK src={src} unit={unit} reg={rng} {name}={dec} "
                        f"(same value already attempted upstream recently, not resent)")
        record_audit(src, unit, fc, start, value, "shadow-ack")
        _reply(writer, tx_id, unit, echo)
        return

    if start in BG_CONFIRM_REGS:
        await _do_bg_confirm_write(writer, tx_id, src, unit, fc, start, count, raws, value,
                                    name, dec, rng, echo)
        return

    # Critical registers get extra upstream attempts before we report failure. Writes are rare
    # (1,160 in 39h against 40k+ reads) so retrying them is cheap, and a 77%-failing write to a
    # live power-limit register leaves the inverter enforcing whatever happened to get through.
    tries = 1 + (WRITE_RETRIES if start in CONFIRM_REGS else 0)
    result = None
    for attempt in range(tries):
        try:
            await _attempt_write(unit, fc, start, raws)
            async with CACHE_LOCK:
                for i in range(count):
                    CACHE.pop((unit, start + i), None)
            LAST_WRITE[(unit, start)] = {"count": count, "vals": tuple(raws), "ts": now}
            _reply(writer, tx_id, unit, echo)
            result = "ACK" if attempt == 0 else f"ACK (retry {attempt})"
            C.writes_ok += 1
            break
        except ModbusException as e:
            # A real rejection from the device - retrying will not change its mind.
            result = f"EXC {e.code:#04x} {exc_name(e.code)}"
            _reply(writer, tx_id, unit, _exc_pdu(fc, e.code))
            C.writes_fail += 1
            break
        except asyncio.CancelledError:
            raise
        except Exception as e:                  # TxnTimeout / transport give-up
            if attempt + 1 < tries:
                C.write_retries += 1
                logger.debug(f"write retry {attempt + 1}/{tries - 1} reg={rng} "
                             f"after {type(e).__name__}")
                continue
            result = f"FAIL {type(e).__name__}"
            _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_DEVICE_FAILURE))
            C.writes_fail += 1

    # Remember what we attempted upstream regardless of outcome, so a client's rapid retry of
    # the SAME value against a chronically flaky register doesn't keep resending.
    if start in SHADOW_ACK_REGS:
        SHADOW_WRITE[(unit, start)] = {"count": count, "vals": tuple(raws), "ts": now}

    if LOG_WRITES:
        logger.info(f"WRITE src={src} unit={unit} fc=0x{fc:02X} reg={rng} raw={_raw_str(raws)} "
                    f"{name}={dec} result={result}")
    record_audit(src, unit, fc, start, value, result)


async def _do_relay(writer, src, tx_id, unit, fc, pdu):
    kind = "device-id" if fc == FC_DEVICE_ID else "huawei-private"
    try:
        resp = await up_relay(unit, pdu)
        _reply(writer, tx_id, unit, resp)
        result = f"EXC {resp[1]:#04x}" if (len(resp) >= 2 and resp[0] >= 0x80) else "relayed"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        result = f"FAIL {type(e).__name__}"
        _reply(writer, tx_id, unit, _exc_pdu(fc, EXC_DEVICE_FAILURE))
    if LOG_RELAY:
        logger.info(f"RELAY src={src} unit={unit} fc=0x{fc:02X} ({kind}) result={result}")
    record_audit(src, unit, fc, None, None, result)


# === Main ===
def _banner():
    logmode = []
    for nm, on in (("reads", LOG_READS), ("writes", LOG_WRITES), ("stale", LOG_STALE),
                   ("blocked", LOG_BLOCKED), ("conn", LOG_CONN), ("degraded", LOG_DEGRADED),
                   ("poll", LOG_POLL), ("health", LOG_HEALTH), ("summary", LOG_SUMMARY)):
        if on:
            logmode.append(nm)
    return (f"SUN2000 proxy listening on {SERVER_HOST}:{SERVER_PORT}; upstream "
            f"{DONGLE_HOST}:{DONGLE_PORT}\n"
            f"  scheduling: min-gap {MIN_GAP * 1000:.0f}ms, txn-cap {TXN_MAX:g}s, "
            f"queue-wait-cap {QUEUE_MAX_WAIT:g}s, "
            f"poller {'on' if POLL_ENABLE else 'OFF'}"
            f"{f' @{POLL_INTERVAL:g}s/txn, max-run {POLL_MAX_RUN}' if POLL_ENABLE else ''}\n"
            f"  cache: default ttl {READ_TTL:g}s, per-register overrides active, "
            f"serve-from-cache {'on' if SERVE_FROM_CACHE else 'OFF'}, "
            f"client-read-cap {CLIENT_READ_MAX_WAIT:g}s, "
            f"max-age {CACHE_MAX_AGE:g}s{' (unlimited)' if CACHE_MAX_AGE <= 0 else ''}\n"
            f"  writes: suppress {HOLD:g}/{REFRESH:g}s, shadow-ack {sorted(SHADOW_ACK_REGS)}, "
            f"confirm {sorted(CONFIRM_REGS)} x{WRITE_RETRIES} retries, "
            f"bg-confirm {sorted(BG_CONFIRM_REGS)} every {BG_CONFIRM_INTERVAL:g}s "
            f"x{BG_CONFIRM_MAX_ATTEMPTS}, "
            f"high-risk reply={HIGH_RISK_REPLY}, deny reply={DENY_REPLY}\n"
            f"  logging: console {'on' if LOG_CONSOLE else 'OFF'}, "
            f"file {LOG_FILE or 'OFF'}"
            f"{f' (rotate {LOG_FILE_MAX_MB:g}MB x{LOG_FILE_BACKUPS})' if LOG_FILE and LOG_FILE_MAX_MB > 0 else ''}, "
            f"every-n {LOG_EVERY_N}, enabled: {','.join(logmode) or 'none'}\n"
            f"  audit -> {AUDIT_FILE}")


async def main():
    global UP, QUEUE
    UP = Upstream(DONGLE_HOST, DONGLE_PORT)
    QUEUE = asyncio.PriorityQueue()

    server = await asyncio.start_server(handle_client, SERVER_HOST, SERVER_PORT)
    logger.info(_banner())
    if HIGH_RISK_REPLY == "ack":
        logger.warning("HIGH_RISK_REPLY=ack: blocked high-risk writes are answered as if "
                       "written. The client's view of those registers will not match the "
                       "device. Set HIGH_RISK_REPLY=exception to refuse them visibly instead.")

    tasks = [asyncio.create_task(upstream_worker()),
             asyncio.create_task(keepalive_loop()),
             asyncio.create_task(poll_loop()),
             asyncio.create_task(health_loop())]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGHUP, lambda: dump_audit("SIGHUP"))
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
    except NotImplementedError:      # non-POSIX
        pass

    await stop.wait()

    logger.info("shutting down...")
    dump_audit("shutdown")
    for t in tasks:
        t.cancel()
    # The worker used to swallow CancelledError and loop forever, so this gather never
    # returned and the process had to be SIGKILLed. Bounded anyway, belt and braces.
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)
    except asyncio.TimeoutError:
        logger.warning("background tasks did not stop within 10s")
    server.close()
    # Close client sockets ourselves: on Python 3.12.1+ wait_closed() blocks until every
    # client handler has finished, and a client that just sits idle would hold shutdown for
    # CLIENT_IDLE_TIMEOUT.
    for w in list(CLIENTS):
        try:
            w.close()
        except Exception:
            pass
    try:
        await asyncio.wait_for(server.wait_closed(), timeout=5)
    except asyncio.TimeoutError:
        logger.warning("server socket did not close within 5s")
    if UP.connected:
        await UP.drop("shutdown")
    logger.info("stopped")


if __name__ == "__main__":
    asyncio.run(main())
