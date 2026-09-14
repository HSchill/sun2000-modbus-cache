# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Modbus TCP cache + write-through proxy for Huawei SUN2000 inverters. The inverter's
Modbus interface (via the SDongle) tolerates only a few concurrent connections; this
proxy holds one connection to it and fans cached reads out to any number of clients.

There are three independent, self-contained server implementations of the same proxy idea
(pick one to run — they don't interact):

- **`modbus_cache_server.py`** — *polling*. Reads a configured `REGISTER_BATCHES` set on
  a timer into a cache and serves clients from it. Reads are always instant; you maintain
  a register map. Opens a fresh inverter connection per poll (and a separate one per write).
- **`ondemand_modbus_cache_server.py`** — *on-demand / read-through*. No register map: it
  relays exactly what clients request, caching each register for `CACHE_TTL` seconds. All
  inverter I/O goes through one persistent, mutex-guarded connection (reads *and* writes),
  with concurrent-identical-read coalescing. First read of a cold/expired register pays
  the inverter round-trip.
- **`adaptive_modbus_cache_server.py`** — *serialising control proxy*. The hardened variant
  for the shared production dongle (Reduxi EMS writes + Home Assistant reads on one SDongle).
  Holds ONE persistent connection whose lifecycle is fully decoupled from downstream clients,
  funnels every read/write/relay through a single FIFO queue + worker (exactly one upstream
  transaction in flight, ever), reconnects with exponential backoff, keeps the socket alive
  when idle, and retries ServerBusy/timeouts with a per-transaction time cap. Adds a
  deny-by-default per-source write ACL, high-risk-write gating, unchanged-write suppression,
  a short-TTL read cache, FC-0x17 rejection, and full request/response + connection-state +
  register audit logging. See the persistent-connection finding
  [[sdongle-requires-persistent-modbus-connection]]. (It grew out of an earlier learned
  read-ahead design; that scheduler / DEMAND model / JSON persistence were removed in the
  hardening rework.)

All three are Python standard library only (asyncio, struct), require Python 3.8+, and
have **no third-party dependencies, no build step, and no lint config**. An integration
test suite lives under `tests/` (see Run / develop). The sections below describe the
polling server unless noted.

## Run / develop

```bash
SUN2000_HOST=10.0.0.50 python3 modbus_cache_server.py   # run against a real inverter
docker compose up -d                                    # edit SUN2000_HOST in docker-compose.yml first
```

Run the integration suite (standard library only; starts everything as real subprocesses
over real TCP):

```bash
python3 tests/run_integration_test.py [polling|ondemand|adaptive|all]
```

It launches a stateful dummy dongle (`tests/dummy_dongle.py`) and each server, then drives
two concurrent readers + a write-with-readback and checks: consistent cached reads, the
write reaching the dongle, write-readback (on-demand/adaptive), dongle shielding, the
single-connection discipline, the adaptive idle-close/reconnect, and — with the dongle
capped at one connection (`DONGLE_MAX_CONNS=1`) — that the proxy serves many clients while
a direct client is refused. `tests/dummy_dongle.py` also runs standalone
(`DONGLE_PORT=15599 python3 tests/dummy_dongle.py`) to point a server at without real
hardware; set `LOG_LEVEL=DEBUG` on a server for per-request detail.

Config is entirely environment variables (see the header docstring and README table):
`SUN2000_HOST` (required), `SUN2000_PORT`, `SUN2000_UNIT_IDS` (comma-separated slave
ids, e.g. `1,2,3,4,5`; `SUN2000_UNIT_ID` is a single-id fallback), `LISTEN_HOST`,
`LISTEN_PORT`, `POLL_INTERVAL`, `LOG_LEVEL`. Deployment artifacts: `Dockerfile`
(python:3.12-alpine, runs as `nobody`), `docker-compose.yml`, and the systemd unit
`sun2000-modbus-cache.service`.

## Architecture

Two asyncio tasks started from `main()`, sharing one in-memory cache:

- **`reader_loop` → `read_sdongle` → `read_batch`** is the *only* code that talks to the
  inverter for reads. Each poll opens a fresh connection and reads every entry in
  `REGISTER_BATCHES` for every unit id in `DEVICE_IDS` sequentially (50 ms apart, all over
  the one socket — the slave is selected by the unit-id byte in the MBAP header), writes
  results into the cache, and closes. This single-connection-at-a-time discipline is the
  whole point of the proxy — preserve it. On failure it retries faster (≤10 s) than
  `POLL_INTERVAL` and logs when the cache goes stale (>120 s).
- **`handle_client`** is the client-facing Modbus TCP server. It serves **FC3** (read
  holding registers) purely from `register_cache` — never touching the inverter — and
  passes **FC6** (write single register) through to the inverter via `forward_write`.
  Any other function code returns an "illegal function" exception.

Shared state: `register_cache` (dict `(unit_id, address) -> uint16`) guarded by the
`cache_lock` asyncio.Lock, plus `last_update` (timestamp) for staleness. `init_cache()`
seeds every `(unit_id, register)` pair to 0 at startup.

Things that are easy to miss and matter when editing:

- **The cache is keyed by `(unit_id, address)`, and FC3 honors the client's unit id.**
  A read is served from `register_cache.get((unit_id, addr), 0)` — the same register on
  different slaves is distinct data. Get this wrong (e.g. drop the unit id) and every
  client silently gets slave 1's values.
- **FC3 never fails and never blocks on the inverter.** Unpolled `(unit_id, address)`
  pairs return `0`, not an exception — a client reading the wrong register or an
  unconfigured slave gets silent zeros. The cache is served even when stale; staleness is
  only logged, never surfaced to clients.
- **Writes open their own separate connection** (`forward_write`), so during a write
  there are briefly two connections to the inverter. Writes forward the client's own
  `unit_id`, so control paths address the right slave.
- **Poll duration scales with `len(DEVICE_IDS) × len(REGISTER_BATCHES)`** (50 ms/batch).
  Many slaves can push a poll cycle past `POLL_INTERVAL`; raise the interval or trim
  batches if so. A transport error mid-poll abandons the *whole* cycle (all remaining
  slaves) and reconnects; a per-slave Modbus exception just fails that batch.
- **Modbus/MBAP framing is hand-rolled with `struct`.** Header is `>HHHB`
  (transaction id, protocol=0, length, unit id); requests/responses are packed inline in
  each handler. There is no Modbus library abstracting this — changes to wire format
  touch the `struct.pack`/`unpack` calls directly, and byte counts / PDU lengths must be
  kept consistent by hand.
- **`REGISTER_BATCHES`** (list of `(start_address, count)` at the top of the file) is the
  hardware config. Defaults target a SUN2000 + LUNA2000 battery + grid meter. Reading a
  register the inverter doesn't expose just logs a failed batch — it never stops the proxy.

### On-demand variant (`ondemand_modbus_cache_server.py`)

Same wire-format helpers, inverted control flow — pull, not push. No `REGISTER_BATCHES`
and no configured unit list (the unit id is taken from each client request and relayed).
Key pieces:

- **`InverterLink`** owns the *single persistent* connection and is the only code that
  touches the inverter. Every `read`/`write` takes its `_lock` (one transaction in flight
  at a time); a transport error calls `_drop()` and the next call reconnects after
  `RECONNECT_BACKOFF`. Reads *and* writes share this one connection — do not add a second.
- **`serve_read`** is the read-through core: find registers that are missing or older than
  `CACHE_TTL`, group them into contiguous runs (≤`MAX_READ`=125), fetch each via
  `_fetch_run`, refill the cache (`(unit,addr) -> (value, monotonic_ts)`), then assemble
  the response. `CACHE_LOCK` is held only for the short cache read/write blocks, never
  across inverter I/O.
- **`_fetch_run`** coalesces concurrent identical fetches through the `INFLIGHT` future
  map, so a herd of clients requesting the same range causes one inverter transaction.
- **Failure policy** (as chosen): inverter unreachable → serve last-known value for
  previously-seen registers (logged), but a *never-seen* register raises
  `CannotServe(0x0B)` (gateway-target-failed). FC6 (single) and FC16 (multiple) write
  success **invalidates** the written register(s) (`CACHE.pop`) rather than assuming the
  value reads back; any other function code returns illegal-function `0x01` (logged).
- **Huawei private FC `0x41` is relayed verbatim** (`InverterLink.raw`) — the installer-login
  challenge/response that unlocks the `47xxx` control registers. The proxy does no crypto;
  the client (e.g. Reduxi / huawei-solar-lib's `PrivateHuaweiModbusRequest`) computes the
  SHA-256/HMAC digest. It authenticates the proxy's *shared* held connection, so a reconnect
  drops the auth (the client must re-login on write rejection). Adaptive pauses read-ahead
  (`LOGIN_GRACE`) around it so intervening reads don't disrupt the handshake.

Behavior is covered by the `tests/` integration suite (write-readback, dongle shielding,
persistent-connection / *no* idle-close). For finer-grained checks (TTL expiry,
coalescing, stale-serve, gateway exception) an in-process fake-inverter harness against the
module functions is quick to write.

### Adaptive variant (`adaptive_modbus_cache_server.py`)

The hardened, production variant: a **serialising control proxy** in front of the shared
SDongle. Its job is to present a rock-solid link to both clients so Reduxi never sees a
"Communication error" (which makes it trip its safety fallback, lose battery telemetry, and
revert the inverters to self-consumption). Cross-validating proxy logs against Reduxi's own
CSV export tied those fallbacks to Modbus link wobble, not a Reduxi logic bug — so the
whole design goal is **zero upstream disruption**. Key pieces (top-of-file docstring has the
full env-var list):

- **`Upstream`** owns the *single persistent* TCP connection and is touched by **nothing but
  the worker** — so no lock is needed on the socket. `connect_once`/`drop` log every state
  transition; `note_ok`/`note_fail` drive exponential backoff (`BACKOFF_MIN..MAX`) and a
  `DEGRADED_THRESHOLD` consecutive-failure health flag. `warm` gives the first read after a
  (re)connect a longer `WARMUP_TIMEOUT` (the dongle needs ~8–15 s to warm a fresh session —
  see [[sdongle-requires-persistent-modbus-connection]]).
- **`QUEUE` + `upstream_worker`** are the heart: every read/write/relay is a `submit()` that
  enqueues one op and awaits a future. The single worker pulls FIFO and runs `_do_transaction`
  — so there is **exactly one upstream transaction in flight, ever**, regardless of how many
  downstream clients connect. `_do_transaction` enforces `MIN_GAP` between requests, retries
  ServerBusy(0x06)/timeout with backoff, reconnects on transport error, and is bounded by
  `TXN_MAX` (a per-transaction time cap so one bad register can't wedge the queue — it fails
  that op and moves on). Because the queue is strictly FIFO/single-worker, a chronically-slow
  or nonresponsive register **head-of-line-blocks every other client** for up to `TXN_MAX`;
  keep `TXN_MAX` small (default 6 s, was 20 s — production evidence: a register the dongle
  wouldn't answer caused a live HA read to hang 25+ s behind it). `QUEUE_MAX_WAIT` is a second
  line of defence: an item that's already waited longer than that when dequeued is failed
  immediately with **no upstream I/O**, so a backlog can't keep growing the worst case. The
  worker logs per-txn queue wait/depth/attempts at DEBUG.
- **Downstream lifecycle is fully decoupled from upstream.** Clients (Reduxi/HA) cycle TCP
  sessions every few seconds; `handle_client` connect/disconnect never touches `Upstream`.
  This is the P0 invariant — a churn test proves 300 short-lived clients cause exactly one
  upstream connection. **Preserve it.**
- **`keepalive_loop`** issues a no-op read (`KEEPALIVE_REG` on `KEEPALIVE_UNIT`) after
  `KEEPALIVE` idle seconds so the dongle doesn't silently close the socket between bursts.
  **`health_loop`** logs a periodic health line (connected / degraded / consec-fail /
  last-ok age / queue depth / suppressed-write count).
- **`serve_read`** is a short-TTL (`READ_TTL`, default 2 s) read-through with
  concurrent-identical-read coalescing (`INFLIGHT`) — it exists to fold *near-simultaneous*
  reads of the same register from both clients into one round-trip, not to shield periodic
  polling. Writes invalidate the cached registers. **A failed upstream read falls back to the
  last-known cached value** (`result="stale"`) rather than failing the client outright — this
  always logs a `STALE` WARNING (bypassing `READ_LOG_MUTE`, since staleness is a health signal,
  not routine poll chatter) and bumps `_stale_count`. This was previously silent: a chronically
  failing register served hours-old data to a muted source (HA) with zero visibility in the
  log — only a manual `SIGHUP` audit dump revealed it.
- **Write path** (`_do_write`), in order: deny-by-default per-source ACL (`WRITE_DENY` first,
  then `WRITE_ALLOW`; default `172.24.1.15`=Reduxi may write anything, `172.24.1.97`=HA
  none) → **high-risk gate** (`HIGH_RISK`, e.g. `47590==0` zeroing charge-from-grid, blocked
  for everyone unless the register is in `HIGH_RISK_ALLOW[src]`; a block is always logged
  prominently, an allowlisted pass is silent) →
  unchanged-write **suppression** (`should_suppress`: same `(unit,reg,value)` within `HOLD`,
  forced through every `REFRESH`; `SUPPRESS_EXCLUDE` — 47083 countdown — never suppressed) →
  forward upstream. **Multi-register writes are never split/merged**: a client FC16 of N regs
  is relayed as one FC16 of N regs (32-bit registers = 2 regs written atomically).
- **FC gatekeeping**: 0x03/0x06/0x10 handled; 0x2B/0x41 relayed **verbatim** (the client does
  any Huawei private 0x41 login crypto — the proxy performs no login and rewrites no value);
  **0x17 (23) is rejected with 0x01 without consuming a queue slot**; any other FC → 0x01.
- **Nothing is ever silently altered/clamped** — a disallowed write is denied and logged, an
  allowed one passes through unchanged.
- **Audit**: `record_audit` tracks every distinct `(src, unit, fc, reg)` — first/last seen,
  count, distinct values, last result; `dump_audit` writes a TSV on **SIGHUP** and shutdown
  (atomic `os.replace`). Muted sources (`READ_LOG_MUTE`, default HA) are still served and
  audited, only their per-read INFO line is suppressed to keep the log control-focused.

The `tests/` integration suite predates this rework and is **out of date** for the adaptive
variant (it exercised the removed scheduler/idle-close and writes from loopback, which the
ACL now denies by default). An in-process fake-dongle smoke test is the quick way to check
this variant — it covers serialisation (max in-flight == 1), downstream-churn decoupling,
ServerBusy retry, reconnect, ACL deny, high-risk block, suppression, 47083 exclusion, FC16
atomic relay, FC-0x17 rejection, 0x41 relay, and the audit dump.
