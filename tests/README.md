# Tests

Integration tests for the three proxy variants. Python 3.8+, standard library only — no
test framework, no dependencies. Each test starts a fake inverter and the real server(s)
as separate processes and talks Modbus TCP to them over loopback, so the actual
`__main__` entry points are exercised end to end.

## Files

- **`dummy_dongle.py`** — a stateful fake Huawei SDongle: a minimal Modbus TCP server that
  answers **FC3** (read holding registers) and **FC6** (write single register). Values
  default to the register's own address (so reads are deterministic) and writes are stored,
  so a write is visible to a later read (write + readback). It logs every request and every
  connection, and prints a summary line on shutdown.
- **`run_integration_test.py`** — the driver. For each variant it launches a fresh dummy
  dongle and the server, runs the scenario, and prints a per-variant report.

## Running

```bash
python3 tests/run_integration_test.py all        # all three variants (default)
python3 tests/run_integration_test.py adaptive   # a single variant
python3 tests/run_integration_test.py ondemand
python3 tests/run_integration_test.py polling
```

Exit code is `0` if everything passed, `1` otherwise. Runtime is ~30 s for `all`. The
tests bind ports in the `15700`–`15760` range on `127.0.0.1`; nothing external is touched.

## What it checks

For every variant:

- **Two concurrent readers** poll a telemetry range and must get consistent, error-free
  values served from the cache.
- **Write with readback** — a client writes a control register, then reads it back. On the
  **on-demand** and **adaptive** variants the write invalidates the cache, so the readback
  returns the new value. The **polling** variant does not poll control registers, so it
  cannot read one back (served from cache as `0`) — the test documents this rather than
  failing it.
- **Dongle shielding** — how many reads and connections the dongle actually saw. The
  caching variants scale dongle load to demand and hold a single connection.

Extra phases:

- **Automated disconnect (proxy ↔ dongle)** — after activity stops, a quiet period longer
  than `IDLE_CLOSE`, then a cold read. Verifies **adaptive** idle-closes its link and
  reconnects on demand, while **on-demand** holds one persistent link open by design
  (**polling** disconnects natively after every poll).
- **Connection-limit value** — with the dummy dongle capped at one connection
  (`DONGLE_MAX_CONNS=1`, like the real SDongle), two clients read fine *through* the proxy
  while a client hitting the dongle *directly* is refused.

## Running the dummy dongle on its own

Handy for poking at a server without real hardware — start the dongle, then point a server
at it:

```bash
DONGLE_PORT=15599 python3 tests/dummy_dongle.py &
SUN2000_HOST=127.0.0.1 SUN2000_PORT=15599 LISTEN_PORT=5502 \
    python3 adaptive_modbus_cache_server.py
```

Dongle environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `DONGLE_HOST` | `0.0.0.0` | bind address |
| `DONGLE_PORT` | `502` | port |
| `DONGLE_DELAY` | `0` | artificial per-request latency (s), to simulate a slow dongle |
| `DONGLE_MAX_CONNS` | `0` | reject beyond N concurrent connections (`0` = unlimited; `1` mimics the real dongle) |
