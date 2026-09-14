#!/usr/bin/env python3
"""Integration test: dummy dongle + a cache-server variant, exercised by two concurrent
reading clients and a write-with-readback — each server run as a real subprocess over
real TCP.

    python3 tests/run_integration_test.py [polling|ondemand|adaptive|all]

For each variant it checks:
  - both readers get consistent, error-free reads served from the cache,
  - the write reaches the dongle (FC6) and, for the caching-on-write variants, an
    immediate readback returns the new value,
  - how many reads the dongle actually saw and how many connections it held
    (the proxy should shield it: few reads, one connection at a time).
"""

import asyncio
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DONGLE = os.path.join(HERE, "dummy_dongle.py")

SPECS = {
    "polling":  ("modbus_cache_server.py",          {"POLL_INTERVAL": "2"}),
    "ondemand": ("ondemand_modbus_cache_server.py", {"CACHE_TTL": "1"}),
    "adaptive": ("adaptive_modbus_cache_server.py", {"MIN_PERIOD": "0.3", "MAX_PERIOD": "5"}),
}

TELEMETRY = (32064, 2)   # in the polling server's REGISTER_BATCHES (input power)
CONTROL_REG = 47075      # a write/control register (max charge current)
WRITE_VALUE = 5


def mbap(u, pdu):
    return struct.pack(">HHHB", 1, 0, len(pdu) + 1, u) + pdu


async def fc3(r, w, u, a, c):
    w.write(mbap(u, struct.pack(">BHH", 3, a, c))); await w.drain()
    h = await r.readexactly(7); _, _, length, _u = struct.unpack(">HHHB", h)
    pdu = await r.readexactly(length - 1)
    if pdu[0] >= 0x80:
        return ("exc", pdu[1])
    bc = pdu[1]
    return ("ok", list(struct.unpack(">" + "H" * (bc // 2), pdu[2:2 + bc])))


async def fc6(r, w, u, a, v):
    w.write(mbap(u, struct.pack(">BHH", 6, a, v))); await w.drain()
    h = await r.readexactly(7); _, _, length, _u = struct.unpack(">HHHB", h)
    pdu = await r.readexactly(length - 1)
    if pdu[0] >= 0x80:
        return ("exc", pdu[1])
    return ("ok", struct.unpack(">HH", pdu[1:5]))


async def try_direct(dport):
    """Attempt to talk Modbus straight to the dongle. Returns True if it refuses us
    (accepts then drops, or won't respond) — i.e. the proxy is holding the one slot."""
    try:
        r, w = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", dport), timeout=3)
    except OSError:
        return True
    try:
        w.write(mbap(1, struct.pack(">BHH", 3, 32064, 2))); await w.drain()
        await asyncio.wait_for(r.readexactly(7), timeout=3)
        return False   # got a reply -> not refused
    except (asyncio.IncompleteReadError, ConnectionResetError, OSError, asyncio.TimeoutError):
        return True
    finally:
        w.close()
        try: await w.wait_closed()
        except Exception: pass


async def wait_port(port, timeout=10):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        try:
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.close(); await w.wait_closed()
            return True
        except OSError:
            await asyncio.sleep(0.1)
    return False


async def spawn(cmd, env):
    proc = await asyncio.create_subprocess_exec(
        *cmd, env={**os.environ, **env},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    lines = []

    async def drain():
        async for line in proc.stdout:
            lines.append(line.decode(errors="replace").rstrip())

    return proc, lines, asyncio.create_task(drain())


async def stop(proc):
    if proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proc.kill(); await proc.wait()


async def scenario(sport):
    """Two readers hammer the telemetry range; a writer does write+readback midway."""
    loop = asyncio.get_running_loop()
    stop_at = loop.time() + 4.0

    async def reader():
        ok = errors = 0
        seen = set()
        r, w = await asyncio.open_connection("127.0.0.1", sport)
        try:
            while loop.time() < stop_at:
                kind, val = await fc3(r, w, 1, *TELEMETRY)
                if kind == "ok":
                    ok += 1; seen.add(tuple(val))
                else:
                    errors += 1
                await asyncio.sleep(0.5)
        finally:
            w.close(); await w.wait_closed()
        return {"ok": ok, "errors": errors, "seen": seen}

    async def writer():
        await asyncio.sleep(1.5)
        r, w = await asyncio.open_connection("127.0.0.1", sport)
        try:
            before = await fc3(r, w, 1, CONTROL_REG, 1)
            wr = await fc6(r, w, 1, CONTROL_REG, WRITE_VALUE)
            after = await fc3(r, w, 1, CONTROL_REG, 1)
        finally:
            w.close(); await w.wait_closed()
        return {"before": before, "write": wr, "after": after}

    a, b, wres = await asyncio.gather(reader(), reader(), writer())
    return a, b, wres


def parse_dongle(lines):
    for ln in lines:
        if "shutting down:" in ln:
            out = {}
            for tok in ln.split("shutting down:", 1)[1].split():
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    out[k] = v
            return out
    return {}


def count_connects(lines):
    return sum(1 for ln in lines if "] connect " in ln)


def count_disconnects(lines):
    return sum(1 for ln in lines if "] disconnect " in ln)


async def run_one(name, dport, sport):
    script, extra = SPECS[name]
    state = f"/tmp/itest_{name}_state.json"
    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    dproc, dlines, dtask = await spawn(["python3", "-u", DONGLE], {"DONGLE_PORT": str(dport)})
    if not await wait_port(dport):
        await stop(dproc)
        return {"name": name, "error": "dongle didn't start", "log": dlines[-6:]}

    senv = {"SUN2000_HOST": "127.0.0.1", "SUN2000_PORT": str(dport),
            "LISTEN_HOST": "127.0.0.1", "LISTEN_PORT": str(sport),
            "STATE_FILE": state, "LOG_LEVEL": "INFO", **extra}
    sproc, slines, stask = await spawn(["python3", "-u", os.path.join(ROOT, script)], senv)
    if not await wait_port(sport):
        await stop(sproc); await stop(dproc)
        return {"name": name, "error": "server didn't start", "log": slines[-10:]}

    await asyncio.sleep(1.5)  # let first poll / warm-up happen
    try:
        a, b, wres = await scenario(sport)
    finally:
        await stop(sproc)     # stop the server first so it stops polling the dongle
        await stop(dproc)
        await dtask; await stask

    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    return {"name": name, "a": a, "b": b, "w": wres, "dongle": parse_dongle(dlines)}


async def run_idle_close(name, dport, sport, expect_reconnect):
    """Verify the proxy's connection behaviour to the dongle when it goes idle: read once
    (opens the link), stay quiet past IDLE_CLOSE, then read a cold register. If the link
    idle-closed, the cold read forces a *new* dongle connection; if the proxy holds it
    open, the cold read reuses the existing one."""
    script, _ = SPECS[name]
    state = f"/tmp/itest_idle_{name}_state.json"
    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    dproc, dlines, dtask = await spawn(["python3", "-u", DONGLE], {"DONGLE_PORT": str(dport)})
    if not await wait_port(dport):
        await stop(dproc)
        return {"name": name, "error": "dongle didn't start"}

    senv = {"SUN2000_HOST": "127.0.0.1", "SUN2000_PORT": str(dport),
            "LISTEN_HOST": "127.0.0.1", "LISTEN_PORT": str(sport),
            "STATE_FILE": state, "LOG_LEVEL": "INFO",
            "IDLE_CLOSE": "1", "CACHE_TTL": "1", "MIN_PERIOD": "2", "MAX_PERIOD": "60"}
    sproc, slines, stask = await spawn(["python3", "-u", os.path.join(ROOT, script)], senv)
    if not await wait_port(sport):
        await stop(sproc); await stop(dproc)
        return {"name": name, "error": "server didn't start", "log": slines[-10:]}

    # Measure deltas during the idle window — the raw log already has one connect +
    # disconnect from wait_port's health check, so absolute counts are not meaningful.
    r, w = await asyncio.open_connection("127.0.0.1", sport)
    try:
        await fc3(r, w, 1, 40000, 1)      # proxy opens the dongle link
        await asyncio.sleep(0.4)
        c0 = count_connects(dlines)
        d0 = count_disconnects(dlines)
        await asyncio.sleep(2.0)          # stay quiet past IDLE_CLOSE (1 s)
        closed_when_idle = count_disconnects(dlines) > d0
        await fc3(r, w, 1, 40010, 1)      # cold read -> new dongle connection iff link had closed
        await asyncio.sleep(0.4)
        reconnected = count_connects(dlines) > c0
    finally:
        w.close(); await w.wait_closed()
        await stop(sproc); await stop(dproc)
        await dtask; await stask

    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    return {"name": name, "reconnected": reconnected, "closed_when_idle": closed_when_idle,
            "conns_max": parse_dongle(dlines).get("conns_max"),
            "expect_reconnect": expect_reconnect}


def report_idle(res):
    print("-" * 68)
    print(f"IDLE-CLOSE ({res['name']}):")
    if res.get("error"):
        print("  ERROR:", res["error"])
        for ln in res.get("log", []):
            print("   |", ln)
        return False
    print(f"  link closed during idle window: {res['closed_when_idle']}")
    print(f"  cold read afterwards reconnected: {res['reconnected']} (conns_max={res['conns_max']})")
    if res["expect_reconnect"]:
        ok = res["closed_when_idle"] and res["reconnected"] and res["conns_max"] == "1"
        label = "adaptive: proxy auto-disconnects when idle, reconnects on demand"
    else:
        ok = (not res["closed_when_idle"]) and (not res["reconnected"])
        label = "on-demand: holds ONE persistent link open (no idle-close, by design)"
    print(f"    [{'PASS' if ok else 'FAIL'}] {label}")
    return ok


async def run_conn_limit(name, dport, sport):
    """The real SDongle accepts only a tiny number of Modbus TCP connections. With the
    dummy dongle capped at ONE, the proxy holds that single slot and still serves many
    clients, while a client trying to reach the dongle *directly* is refused."""
    script, _ = SPECS[name]
    state = f"/tmp/itest_limit_{name}_state.json"
    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    dproc, dlines, dtask = await spawn(
        ["python3", "-u", DONGLE], {"DONGLE_PORT": str(dport), "DONGLE_MAX_CONNS": "1"})
    if not await wait_port(dport):
        await stop(dproc)
        return {"name": name, "error": "dongle didn't start"}

    senv = {"SUN2000_HOST": "127.0.0.1", "SUN2000_PORT": str(dport),
            "LISTEN_HOST": "127.0.0.1", "LISTEN_PORT": str(sport),
            "STATE_FILE": state, "LOG_LEVEL": "INFO",
            "IDLE_CLOSE": "30", "MIN_PERIOD": "2", "MAX_PERIOD": "60"}
    sproc, slines, stask = await spawn(["python3", "-u", os.path.join(ROOT, script)], senv)
    if not await wait_port(sport):
        await stop(sproc); await stop(dproc)
        return {"name": name, "error": "server didn't start", "log": slines[-10:]}

    ar, aw = await asyncio.open_connection("127.0.0.1", sport)
    br, bw = await asyncio.open_connection("127.0.0.1", sport)
    try:
        a1 = await fc3(ar, aw, 1, 32064, 2)      # proxy opens & holds its one dongle slot
        b1 = await fc3(br, bw, 1, 32064, 2)      # served without a second dongle connection
        proxy_reads_ok = a1[0] == "ok" and b1[0] == "ok"

        direct_refused = await try_direct(dport)  # proxy holds the slot -> refused

        a2 = await fc3(ar, aw, 1, 32064, 2)
        b2 = await fc3(br, bw, 1, 32064, 2)
        proxy_still_ok = a2[0] == "ok" and b2[0] == "ok"
    finally:
        for c in (aw, bw):
            c.close()
            try: await c.wait_closed()
            except Exception: pass
        await stop(sproc); await stop(dproc)
        await dtask; await stask

    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    return {"name": name, "proxy_reads_ok": proxy_reads_ok,
            "direct_refused": direct_refused, "rejected": any("REJECT" in ln for ln in dlines),
            "proxy_still_ok": proxy_still_ok}


def report_limit(res):
    print("-" * 68)
    print(f"CONNECTION LIMIT ({res['name']}, dummy dongle capped at 1 connection):")
    if res.get("error"):
        print("  ERROR:", res["error"])
        for ln in res.get("log", []):
            print("   |", ln)
        return False
    print(f"  two clients via the proxy read OK: {res['proxy_reads_ok']}")
    print(f"  direct client to the dongle refused: {res['direct_refused']} "
          f"(dongle logged REJECT: {res['rejected']})")
    print(f"  proxy clients still OK afterwards: {res['proxy_still_ok']}")
    ok = res["proxy_reads_ok"] and res["direct_refused"] and res["proxy_still_ok"]
    print(f"    [{'PASS' if ok else 'FAIL'}] proxy shares its single dongle slot among "
          "many clients; direct access is refused")
    return ok


async def run_warmup(name, dport, sport):
    """The real SDongle stays silent for a few seconds after connect (warm-up). The proxy
    must HOLD one connection through that — a read timeout must not drop it — and start
    serving once the warm-up ends, rather than churning a fresh connection each timeout."""
    script, _ = SPECS[name]
    state = f"/tmp/itest_warmup_{name}_state.json"
    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    dproc, dlines, dtask = await spawn(
        ["python3", "-u", DONGLE], {"DONGLE_PORT": str(dport), "DONGLE_WARMUP": "3"})
    if not await wait_port(dport):
        await stop(dproc)
        return {"name": name, "error": "dongle didn't start"}

    # READ_TIMEOUT=2 is shorter than the 3 s warm-up, so the first reads time out — the
    # proxy must keep the connection anyway.
    senv = {"SUN2000_HOST": "127.0.0.1", "SUN2000_PORT": str(dport),
            "LISTEN_HOST": "127.0.0.1", "LISTEN_PORT": str(sport),
            "STATE_FILE": state, "LOG_LEVEL": "INFO",
            "READ_TIMEOUT": "2", "IDLE_CLOSE": "0", "MIN_PERIOD": "1", "MAX_PERIOD": "10"}
    sproc, slines, stask = await spawn(["python3", "-u", os.path.join(ROOT, script)], senv)
    if not await wait_port(sport):
        await stop(sproc); await stop(dproc)
        return {"name": name, "error": "server didn't start", "log": slines[-10:]}

    loop = asyncio.get_running_loop()
    r, w = await asyncio.open_connection("127.0.0.1", sport)
    any_ok = False; first_ok = None; t0 = loop.time()
    try:
        while loop.time() - t0 < 10:
            try:
                kind, _ = await fc3(r, w, 1, 32064, 2)
            except Exception:
                kind = "err"
            if kind == "ok" and not any_ok:
                any_ok = True; first_ok = loop.time() - t0
            await asyncio.sleep(1.5)
    finally:
        w.close(); await w.wait_closed()
        await stop(sproc); await stop(dproc)
        await dtask; await stask

    for f in (state, state + ".tmp"):
        try: os.remove(f)
        except FileNotFoundError: pass

    d = parse_dongle(dlines)
    return {"name": name, "any_ok": any_ok, "first_ok": first_ok,
            "conns_total": d.get("conns_total"), "conns_max": d.get("conns_max")}


def report_warmup(res):
    print("-" * 68)
    print(f"WARM-UP / HOLD ({res['name']}, dongle 3 s warm-up, proxy READ_TIMEOUT=2 s):")
    if res.get("error"):
        print("  ERROR:", res["error"])
        for ln in res.get("log", []):
            print("   |", ln)
        return False
    served = f"yes (first OK ~{res['first_ok']:.1f}s)" if res["any_ok"] else "no"
    print(f"  served after warm-up: {served}")
    print(f"  dongle connections: total={res['conns_total']} max={res['conns_max']} "
          f"(hold = no reconnect churn on read timeouts)")
    ok = res["any_ok"] and res["conns_total"] in ("1", "2")   # 2 = wait_port probe + 1 held
    print(f"    [{'PASS' if ok else 'FAIL'}] proxy holds ONE connection through the "
          "warm-up and serves once it ends")
    return ok


def report(res, overall):
    print("=" * 68)
    print(f"SERVER: {res['name']}")
    if res.get("error"):
        print("  ERROR:", res["error"])
        for ln in res.get("log", []):
            print("   |", ln)
        return False

    a, b, w, d = res["a"], res["b"], res["w"], res["dongle"]
    print(f"  reader A: {a['ok']} reads ok, {a['errors']} err, values {sorted(a['seen'])}")
    print(f"  reader B: {b['ok']} reads ok, {b['errors']} err, values {sorted(b['seen'])}")

    before, after = w["before"], w["after"]
    bval = before[1][0] if before[0] == "ok" else before
    aval = after[1][0] if after[0] == "ok" else after
    print(f"  write {CONTROL_REG}={WRITE_VALUE} -> readback before={bval} after={aval}")
    print(f"  dongle saw: FC3={d.get('FC3')} reads, FC6={d.get('FC6')} writes, "
          f"conns_total={d.get('conns_total')} conns_max={d.get('conns_max')}")

    reads_ok = a["errors"] == 0 and b["errors"] == 0 and a["ok"] > 0 and b["ok"] > 0
    consistent = a["seen"] == b["seen"] and a["seen"] <= {(32064, 32065)}
    readback_ok = after[0] == "ok" and after[1][0] == WRITE_VALUE

    checks = {
        "two readers: consistent, error-free": reads_ok and consistent,
        "write reached the dongle (FC6 >= 1)": d.get("FC6", "0") not in ("0", None),
    }
    if res["name"] in ("ondemand", "adaptive"):
        checks["write-readback returns the new value"] = readback_ok
        checks["single dongle connection at a time"] = d.get("conns_max") == "1"
    else:
        print("    note: the polling server does not poll control register "
              f"{CONTROL_REG}, so it cannot read it back (served from cache=0); "
              "it also opens a fresh connection per poll and per write.")

    ok = True
    for k, v in checks.items():
        print(f"    [{'PASS' if v else 'FAIL'}] {k}")
        ok = ok and v
    return ok


async def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    names = ["polling", "ondemand", "adaptive"] if which == "all" else [which]
    overall = True
    for i, name in enumerate(names):
        res = await run_one(name, 15700 + i, 15710 + i)
        overall = report(res, overall) and overall

    print("=" * 68)
    print("AUTOMATED DISCONNECT (proxy <-> dongle)")
    if "polling" in names:
        print("  polling: opens a fresh connection per poll and per write — disconnects natively")
    idle_specs = [(n, n == "adaptive") for n in names if n in ("ondemand", "adaptive")]
    for i, (name, expect_reconnect) in enumerate(idle_specs):
        res = await run_idle_close(name, 15730 + i, 15740 + i, expect_reconnect)
        overall = report_idle(res) and overall

    limit_name = "adaptive" if "adaptive" in names else next(
        (n for n in names if n in ("ondemand", "adaptive")), None)
    if limit_name:
        print("=" * 68)
        print("CONNECTION-LIMIT VALUE (why the proxy exists)")
        res = await run_conn_limit(limit_name, 15750, 15751)
        overall = report_limit(res) and overall

    if "adaptive" in names:
        print("=" * 68)
        print("SDONGLE WARM-UP (hold the connection through post-connect silence)")
        res = await run_warmup("adaptive", 15760, 15761)
        overall = report_warmup(res) and overall

    print("=" * 68)
    print("OVERALL:", "PASS" if overall else "FAIL")
    raise SystemExit(0 if overall else 1)


if __name__ == "__main__":
    asyncio.run(main())
