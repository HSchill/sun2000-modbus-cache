#!/usr/bin/env python3
"""Stateful dummy Huawei SDongle — a minimal Modbus TCP server for testing the proxies.

Holds a register store per unit id. FC3 (read holding registers) returns stored values,
defaulting to the register's own address so reads are deterministic; FC6 (write single
register) stores the value, so a write is visible to any later read (write + readback).
Every request is logged and connection counts are tracked, so you can see how the proxy
shields the dongle. On shutdown it prints a summary line the integration test parses.

Run it standalone to point a cache server at it:
    DONGLE_PORT=15599 python3 tests/dummy_dongle.py

Environment:
    DONGLE_HOST       bind address                         (default 0.0.0.0)
    DONGLE_PORT       port                                 (default 502)
    DONGLE_DELAY      artificial per-request latency, s    (default 0)
    DONGLE_MAX_CONNS  reject beyond N concurrent conns     (default 0 = unlimited)
"""

import asyncio
import os
import signal
import struct
import time

HOST = os.environ.get("DONGLE_HOST", "0.0.0.0")
PORT = int(os.environ.get("DONGLE_PORT", 502))
DELAY = float(os.environ.get("DONGLE_DELAY", 0))
MAX_CONNS = int(os.environ.get("DONGLE_MAX_CONNS", 0))
# Like the real SDongle: for the first WARMUP seconds of each connection, silently ignore
# reads (no response) to simulate the post-connect warm-up.
WARMUP = float(os.environ.get("DONGLE_WARMUP", 0))

store = {}          # (unit, addr) -> value
fc3_count = 0
fc6_count = 0
conns_total = 0
conns_open = 0
conns_max = 0


def value(unit, addr):
    return store.get((unit, addr), addr & 0xFFFF)


async def handle(reader, writer):
    global conns_total, conns_open, conns_max, fc3_count, fc6_count
    peer = writer.get_extra_info("peername")
    if MAX_CONNS and conns_open >= MAX_CONNS:
        print(f"[dongle] REJECT {peer}: over connection limit ({MAX_CONNS})", flush=True)
        writer.close()
        return
    conns_total += 1
    conns_open += 1
    conns_max = max(conns_max, conns_open)
    conn_start = time.monotonic()
    print(f"[dongle] connect {peer} (open={conns_open}, total={conns_total})", flush=True)
    try:
        while True:
            head = await reader.readexactly(7)
            tx, proto, length, unit = struct.unpack(">HHHB", head)
            pdu = await reader.readexactly(length - 1)
            fc = pdu[0]
            if DELAY:
                await asyncio.sleep(DELAY)

            if fc == 3 and WARMUP and time.monotonic() - conn_start < WARMUP:
                # simulate the SDongle warm-up: swallow the read, send nothing
                print(f"[dongle] FC3 during warm-up "
                      f"({time.monotonic() - conn_start:.1f}s) -> silent", flush=True)
                continue

            if fc == 3:
                fc3_count += 1
                addr, count = struct.unpack(">HH", pdu[1:5])
                vals = [value(unit, addr + i) for i in range(count)]
                body = struct.pack(">BB", 3, count * 2) + struct.pack(">" + "H" * count, *vals)
                print(f"[dongle] FC3 #{fc3_count} unit{unit} [{addr}..{addr + count}) -> {vals}", flush=True)
            elif fc == 6:
                fc6_count += 1
                addr, val = struct.unpack(">HH", pdu[1:5])
                store[(unit, addr)] = val
                body = struct.pack(">BHH", 6, addr, val)
                print(f"[dongle] FC6 #{fc6_count} unit{unit} reg {addr} = {val} (stored)", flush=True)
            else:
                body = struct.pack(">BB", fc | 0x80, 1)  # illegal function
                print(f"[dongle] FC{fc} unsupported -> illegal function", flush=True)

            writer.write(struct.pack(">HHHB", tx, 0, len(body) + 1, unit) + body)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
        pass
    finally:
        conns_open -= 1
        print(f"[dongle] disconnect {peer} (open={conns_open})", flush=True)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def main():
    server = await asyncio.start_server(handle, HOST, PORT)
    print(f"[dongle] listening on {HOST}:{PORT} "
          f"(delay={DELAY}s, max_conns={MAX_CONNS or 'unlimited'})", flush=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, stop.set)
    await stop.wait()

    print(f"[dongle] shutting down: FC3={fc3_count} FC6={fc6_count} "
          f"conns_total={conns_total} conns_max={conns_max}", flush=True)
    server.close()
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
