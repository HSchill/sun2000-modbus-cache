# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-09-01

### Added

- **On-demand proxy variant** (`ondemand_modbus_cache_server.py`): a read-through cache that
  relays exactly what clients request instead of polling a fixed register set. No register
  map to maintain; per-register freshness via `CACHE_TTL`; all inverter traffic (reads and
  writes) goes through a single mutex-guarded connection with concurrent-read coalescing;
  serves the last known values while the inverter is briefly unreachable; writes invalidate
  the cached register.
- **Adaptive read-ahead proxy variant** (`adaptive_modbus_cache_server.py`): learns the
  requested register set and each register's request period from client traffic and keeps
  the cache warm slightly ahead of demand, in bursts over a connection that is closed
  between them (`IDLE_CLOSE`) — leaving the SDongle free for its FusionSolar cloud push. A
  cold or mispredicted read falls back to fetching on demand. The learned model is persisted
  to a JSON state file (`STATE_FILE`) and reloaded at startup, and each write is logged with
  the interval since the previous one.
- systemd units `sun2000-modbus-cache-ondemand.service` and
  `sun2000-modbus-cache-adaptive.service`. The Docker image now bundles all three servers,
  and the container command selects the variant.
- `CLAUDE.md` guidance file for working in the repository.

### Changed

- The polling server (`modbus_cache_server.py`) now supports multiple Modbus slave ids via
  `SUN2000_UNIT_IDS` (comma-separated). Its cache is keyed by `(unit id, register)` and FC3
  reads are served for the slave the client addresses. Single-slave setups are unchanged —
  `SUN2000_UNIT_ID` remains as a fallback (default `1`).

## [1.0.0] - 2026-08-17

First tagged release. The proxy has been running unchanged in production against a
SUN2000 with a LUNA2000 battery since 2026-07-21.

### Added

- Modbus TCP cache in front of a Huawei SUN2000 inverter: holds exactly one connection
  to the inverter and polls a configured register set on an interval (`POLL_INTERVAL`,
  default 10 s).
- Serves cached holding registers (FC3) to any number of concurrent Modbus TCP clients,
  with no additional load on the inverter. Clients can poll as fast as they like.
- Write-through for single-register writes (FC6), so control paths such as a
  surplus-heating controller keep working.
- Stale-cache behaviour: the last known values keep being served while the inverter is
  briefly unreachable, and the staleness is logged, instead of every client failing at
  once.
- Configuration entirely through environment variables: `SUN2000_HOST`, `SUN2000_PORT`,
  `SUN2000_UNIT_ID`, `LISTEN_HOST`, `LISTEN_PORT`, `POLL_INTERVAL`, `LOG_LEVEL`.
- Deployment units: `Dockerfile` (python:3.12-alpine, runs as `nobody`),
  `docker-compose.yml`, and a systemd service unit.
- No third-party Python dependencies — standard library only.

[1.1.0]: https://github.com/cloudapp-dev/sun2000-modbus-cache/releases/tag/v1.1.0
[1.0.0]: https://github.com/cloudapp-dev/sun2000-modbus-cache/releases/tag/v1.0.0
