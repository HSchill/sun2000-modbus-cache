# sun2000-modbus-cache

**A Modbus TCP cache and write-through proxy for Huawei SUN2000 inverters.**
One connection to the inverter, as many clients as you want. No dependencies.

## The problem

The SUN2000's Modbus TCP interface — whether you reach it through the SDongle or
a direct connection — accepts only a **very small number of concurrent
connections**. That limit is easy to hit without realising it:

- Home Assistant polling the inverter for PV and battery sensors
- a surplus-heating controller (my·PV AC·THOR, a wallbox, a heat pump) reading
  grid feed-in to decide when to run
- a dashboard, a script, or a second Home Assistant instance

Three clients is often already too many. The symptoms are recognisable: reads
that time out at random, sensors that go `unavailable` for a minute and come
back, a heating controller that stops seeing surplus, and inverter connections
that get refused entirely until something disconnects. Nothing is broken —
you've simply run out of Modbus sockets.

The usual advice is "poll less often", which trades away exactly the
responsiveness you installed the integration for.

## What this does

This proxy sits between your clients and the inverter:

```
  Home Assistant  ┐
  AC·THOR         ├──►  sun2000-modbus-cache  ──►  SUN2000 / SDongle
  dashboard       ┘         (one connection)
  …any number
```

- Holds **exactly one** connection to the inverter and polls a configured set of
  registers on an interval (default every 10 s).
- Serves those cached values to **any number** of Modbus TCP clients (FC3, read
  holding registers) — instantly, with no load on the inverter.
- **Forwards writes** (FC6, write single register) straight through to the
  inverter, so control paths keep working.
- Keeps serving the last known values when the inverter is briefly unreachable,
  instead of failing every client at once. Stale caches are logged.

Because reads are served from memory, a client can poll as fast as it likes
without the inverter ever noticing.

## Requirements

Python 3.8+. **No third-party packages** — standard library only.

## Quick start

```bash
git clone https://github.com/cloudapp-dev/sun2000-modbus-cache.git
cd sun2000-modbus-cache
SUN2000_HOST=10.0.0.50 python3 modbus_cache_server.py
```

Then point your clients at this machine on port `5502` instead of the inverter
on `502`.

### Docker

```bash
docker compose up -d   # edit SUN2000_HOST in docker-compose.yml first
```

### systemd

```bash
sudo cp modbus_cache_server.py /opt/sun2000-modbus-cache/
sudo cp sun2000-modbus-cache.service /etc/systemd/system/
sudo systemctl enable --now sun2000-modbus-cache
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `SUN2000_HOST` | *(required)* | Inverter / SDongle address |
| `SUN2000_PORT` | `502` | Inverter Modbus port |
| `SUN2000_UNIT_IDS` | `1` | Modbus unit/slave ids to poll, comma-separated (e.g. `1,2,3,4,5`) |
| `SUN2000_UNIT_ID` | `1` | Single unit id — fallback used when `SUN2000_UNIT_IDS` is unset |
| `LISTEN_HOST` | `0.0.0.0` | Address to serve on |
| `LISTEN_PORT` | `5502` | Port to serve on |
| `POLL_INTERVAL` | `10` | Seconds between inverter polls |
| `LOG_LEVEL` | `INFO` | `DEBUG` for per-batch detail |

### Registers

The polled registers are the `REGISTER_BATCHES` list at the top of
`modbus_cache_server.py`. The defaults cover a SUN2000 with a LUNA2000 battery
and a grid meter: PV strings, input and active power, phase voltages, grid
frequency, inverter temperature, daily and cumulative yield, grid import/export,
battery SOC, battery power, and battery charge/discharge totals.

Trim the list to what your hardware actually exposes — requesting registers your
inverter doesn't have simply logs a failed batch, it won't stop the proxy.

### Multiple slave IDs

For a cascaded setup — several inverters behind one SDongle, addressed by different
Modbus unit ids — list them all in `SUN2000_UNIT_IDS` (e.g. `SUN2000_UNIT_IDS=1,2,3,4,5`).
The proxy polls the same `REGISTER_BATCHES` for each id over its single inverter
connection, caches them separately, and serves each client the slave it addresses via
the request's unit id. Point one Modbus client per unit id at the proxy, exactly as you
would at the inverter.

Polling is sequential (~50 ms per batch), so poll time grows with the number of slaves ×
batches. If a cycle starts approaching `POLL_INTERVAL`, raise the interval or trim the
register list.

## On-demand variant

`ondemand_modbus_cache_server.py` is an alternative server with the same job but the
opposite strategy: instead of polling a fixed register set, it **relays exactly what
clients ask for** and caches each register briefly.

- **No register map.** There is no `REGISTER_BATCHES` to maintain — the cache discovers
  what your clients actually read. The Modbus unit id is taken from each request and
  relayed as-is, so cascaded multi-inverter setups need no configuration either.
- **One connection, still.** All inverter traffic — reads *and* writes — goes through a
  single persistent connection, serialised so only one request is in flight at a time.
- **Fresh within `CACHE_TTL`.** A read is served from cache when the requested registers
  are younger than `CACHE_TTL` (default 10 s); otherwise the missing ones are fetched from
  the inverter, cached, and returned. Concurrent identical reads collapse into a single
  inverter request.
- **Same write-through.** FC6 writes are relayed straight to the inverter, and the written
  register is invalidated so the next read reflects the new value.
- **Resilience.** If the inverter is briefly unreachable, previously-seen registers keep
  being served (stale, logged); a register never read before returns a Modbus "gateway
  target failed to respond" exception.

### Which should I run?

| | Polling | On-demand | Adaptive |
|---|---|---|---|
| File | `modbus_cache_server.py` | `ondemand_modbus_cache_server.py` | `adaptive_modbus_cache_server.py` |
| Register map | you configure `REGISTER_BATCHES` | none — discovered | none — learned |
| Read latency | always instant | instant cached; cold read waits a round-trip | almost always instant (warmed ahead of demand) |
| Inverter load | constant, every `POLL_INTERVAL` | only what clients request (≤1 read/reg/`CACHE_TTL`) | matches learned demand, in idle-closing bursts |
| Freshness | up to `POLL_INTERVAL` old | up to `CACHE_TTL` old | ~ the client's own poll period |
| Cloud-friendly | yes — releases the connection between polls | no — holds one connection open | yes — bursts, then idle-closes |
| Best for | a known, stable sensor set | sparse or changing register use, no map | a cloud-connected SDongle; hands-off |

### Running it

Same as the polling server, except you run the other file and use `CACHE_TTL` instead of
`POLL_INTERVAL`:

```bash
SUN2000_HOST=10.0.0.50 CACHE_TTL=10 python3 ondemand_modbus_cache_server.py
```

The Docker image bundles both servers; override the command to select on-demand:

```yaml
services:
  sun2000-modbus-cache:
    build: .
    command: ["python3", "-u", "ondemand_modbus_cache_server.py"]
    environment:
      SUN2000_HOST: "10.0.0.50"
      CACHE_TTL: "10"
    ports:
      - "5502:5502"
```

For systemd, use the bundled `sun2000-modbus-cache-ondemand.service` unit in place of
`sun2000-modbus-cache.service`.

Configuration variables: `SUN2000_HOST`, `SUN2000_PORT`, `LISTEN_HOST`, `LISTEN_PORT`,
`CACHE_TTL` (default 10), `RECONNECT_BACKOFF` (default 5), `LOG_LEVEL`. There is no
`REGISTER_BATCHES`, `SUN2000_UNIT_IDS`, or `POLL_INTERVAL`.

## Adaptive variant

`adaptive_modbus_cache_server.py` combines the best of the other two and is the best fit
when the dongle is *also* reporting to FusionSolar. It **learns** which registers your
clients read and how often, then keeps just those warm — polling slightly ahead of demand,
in short bursts, over a connection it drops between bursts.

- **No register map, no unit-id config** — both are discovered from the FC3 requests your
  clients actually make.
- **Warm on arrival.** For each register it tracks a moving average of the client's request
  period and refreshes a little sooner than that, so reads are almost always instant cache
  hits. A cold or mispredicted read falls back to fetching on demand, so cached data is
  never wrong.
- **Cloud-friendly by design.** All inverter traffic uses one connection that is **closed
  after `IDLE_CLOSE` seconds idle**, and reads happen in bursts — so between bursts the
  SDongle is free for its ~180 s FusionSolar push. (A persistent local connection is the
  main thing that disrupts the dongle's cloud reporting; this variant deliberately avoids
  holding one open.)
- **Learns the write cadence too.** Each FC6 write is logged with the interval since the
  previous one, so you can see how often your controller actually writes.
- **Survives restarts.** The learned model (register set + periods, not values) is written
  to a JSON state file periodically when it changes, and reloaded at startup — no
  cold-start.

### Running it

```bash
SUN2000_HOST=10.0.0.50 python3 adaptive_modbus_cache_server.py
```

For Docker (bundled in the same image), give it a **writable, mounted path** for the state
file — the container runs as `nobody`, so the default in-image path isn't writable:

```yaml
services:
  sun2000-modbus-cache:
    build: .
    command: ["python3", "-u", "adaptive_modbus_cache_server.py"]
    environment:
      SUN2000_HOST: "10.0.0.50"
      STATE_FILE: "/data/adaptive_cache_state.json"
    volumes:
      - ./data:/data
    ports:
      - "5502:5502"
```

For systemd, use the bundled `sun2000-modbus-cache-adaptive.service` unit; it sets a
`StateDirectory`, so the learned model persists under `/var/lib/sun2000-modbus-cache/`.

Configuration variables: `SUN2000_HOST`, `SUN2000_PORT`, `LISTEN_HOST`, `LISTEN_PORT`,
`MIN_PERIOD` (2), `MAX_PERIOD` (60), `READAHEAD_LEAD` (0.2), `EVICT_AFTER` (300),
`IDLE_CLOSE` (5), `RECONNECT_BACKOFF` (5), `STATE_FILE`, `STATE_SAVE_INTERVAL` (30),
`LOG_LEVEL`. There is no `REGISTER_BATCHES` or `POLL_INTERVAL`.

## Home Assistant

Point the built-in Modbus integration at the proxy instead of the inverter:

```yaml
modbus:
  - name: huawei_inverter
    type: tcp
    host: 10.0.0.60      # the machine running this proxy
    port: 5502
    sensors:
      - name: PV Power
        address: 32080
        input_type: holding
        data_type: int32
        unit_of_measurement: W
        device_class: power
      - name: Battery SOC
        address: 37760
        input_type: holding
        data_type: uint16
        scale: 0.1
        unit_of_measurement: "%"
        device_class: battery
```

The same applies to the Huawei Solar HACS integration and to any other Modbus
client — they all just talk to a different host and port.

## Notes and limitations

- **Read values are as fresh as `POLL_INTERVAL`.** That is the trade: every
  client gets an instant answer, at the cost of data up to one poll interval old.
  For PV and battery telemetry that's fine; for sub-second control loops it isn't.
- **Writes are not cached** — they go straight to the inverter and are subject
  to whatever the inverter accepts. If your inverter refuses a write, this proxy
  won't change that.
- **No authentication.** It's a plain Modbus TCP server, exactly like the
  inverter's own. Keep it on a trusted network segment.
- Only FC3 (read holding registers) and FC6 (write single register) are
  implemented; anything else returns an "illegal function" exception.

## Background

The reasoning, the failure modes and the register map are written up in more
detail here:

- [Caching a Huawei SUN2000 over Modbus](https://www.cloudapp.dev/caching-huawei-sun2000-modbus-home-assistant)
- [The SUN2000 Modbus registers I actually use](https://www.cloudapp.dev/home-assistant-huawei-sun2000-modbus-registers)
- [Write-through on a Modbus proxy](https://www.cloudapp.dev/home-assistant-modbus-write-through-proxy)
- [Reconnects and stale caches](https://www.cloudapp.dev/home-assistant-modbus-proxy-reconnect-stale-cache)

Built and run in production on a ~9 kWp SUN2000 with a LUNA2000 battery, serving
Home Assistant and an AC·THOR surplus heater at the same time.

## License

MIT — see [LICENSE](LICENSE).
