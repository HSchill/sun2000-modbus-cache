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
- **`adaptive_modbus_cache_server.py`** — *adaptive read-ahead*. Learns the register set
  *and* each register's request period from client traffic, then a scheduler keeps the
  cache warm by polling that set slightly ahead of demand, in bursts through one
  idle-closing connection (so the dongle is free for its FusionSolar push between reads).
  Persists the learned model to a JSON state file. Best fit for the cloud-connected
  SDongle — see [[sdongle-cloud-vs-local-modbus-contention]].

All three are Python standard library only (asyncio, struct), require Python 3.8+, and
have **no third-party dependencies, no build step, no committed test suite, and no lint
config**. The sections below describe the polling server unless noted.

## Run / develop

```bash
SUN2000_HOST=10.0.0.50 python3 modbus_cache_server.py   # run against a real inverter
docker compose up -d                                    # edit SUN2000_HOST in docker-compose.yml first
```

There is no automated test suite. To exercise a change without an inverter, point
`SUN2000_HOST` at any reachable host (polls will fail and log, the FC3/FC6 server still
runs) and drive it with a Modbus client on `LISTEN_PORT` (default 5502). Set
`LOG_LEVEL=DEBUG` for per-batch detail.

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
  `CannotServe(0x0B)` (gateway-target-failed). FC6 write success **invalidates** the
  cached register (`CACHE.pop`) rather than assuming the written value reads back.

Behavior is verified with an in-process fake-inverter harness (cache hit/miss/TTL,
coalescing, write-invalidation, stale-serve, gateway exception) — not committed; recreate
it when changing this file.

### Adaptive variant (`adaptive_modbus_cache_server.py`)

Builds on the on-demand `InverterLink` (same single-connection discipline) and adds
learning + read-ahead. Key pieces:

- **`DEMAND`** (`(unit,addr) -> {last_req, period}`) is the learned model. `_record_demand`
  updates it on every client FC3 — even cache hits, since we always see the request — using
  an EMA of the inter-arrival gap as the period. `_eff_period` clamps to
  `[MIN_PERIOD, MAX_PERIOD]`.
- **`scheduler_loop`** refreshes each learned register when `read_ts + period*(1-READAHEAD_LEAD)`
  is due, grouping due registers into contiguous bursts, and evicts registers unrequested
  for `EVICT_AFTER`. It waits on `_demand_event` so new demand wakes it promptly.
- **`serve_read`** is the on-demand read-through but with per-register freshness = learned
  period (not a global TTL); the scheduler keeps most reads as pure cache hits, and a cold
  or mispredicted read falls back to fetching now.
- **`InverterLink.idle_closer`** drops the socket after `IDLE_CLOSE` — this is what makes
  the bursty polling FusionSolar-friendly. Don't remove it.
- **Persistence**: `load_state` seeds `DEMAND` at startup; `state_saver_loop` calls
  `save_state` every `STATE_SAVE_INTERVAL` when `_model_dirty` (atomic write via
  `os.replace`). Only the register set + periods are saved, never values. State file is
  gitignored.
- **`_log_write`** logs each FC6 with the interval since the previous write, to reveal how
  often the controller actually writes (the Reduxi "writes are rare" assumption).
