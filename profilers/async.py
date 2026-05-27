"""
Profiler for the async dlstorage benchmark.

Runs two passes:

1. cProfile pass  – runs a short benchmark under cProfile, prints top functions
                    by cumulative time, and optionally saves a .prof file for
                    snakeviz / kcachegrind.

2. Step-timing pass – instruments every layer in the hot path and prints a
                      breakdown of where time actually goes:

    Layer                  example budget
    ─────────────────────────────────────
    LocalStore.set/get     baseline (no network)
    RendezvousRing lookup  ring overhead
    pickle.dumps           encode cost
    pickle.loads           decode cost
    raw TCP roundtrip      kernel + loopback
    full node.set / .get   all-in cost

Usage:
    .venv/bin/python profiler.py [--ops 10000] [--concurrency 64] [--save profile.prof]
"""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import io
import pickle
import pstats
import socket
import time

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    _LOOP = "uvloop"
except ImportError:
    _LOOP = "asyncio"

from dlstorage import StaticDiscovery, AsyncStorageNode
from dlstorage.ring import RendezvousRing
from dlstorage.store import LocalStore
from dlstorage.types import Message, MessageType, NodeInfo

ADDRS = ["127.0.0.1:7301", "127.0.0.1:7302", "127.0.0.1:7303"]
VALUE = b"x" * 64


def fmt_ns(ns: float) -> str:
    if ns < 1_000:
        return f"{ns:.1f} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.2f} µs"
    return f"{ns / 1_000_000:.3f} ms"


def bar(frac: float, width: int = 30) -> str:
    filled = round(frac * width)
    return "█" * filled + "░" * (width - filled)


def section(title: str) -> None:
    print(f"\n{'━' * 60}")
    print(f"  {title}")
    print(f"{'━' * 60}")


# --------------------------------------------------------------------------- #
# 1. cProfile pass                                                             #
# --------------------------------------------------------------------------- #


async def _bench_coro(nodes: list[AsyncStorageNode], ops: int, concurrency: int) -> None:
    counter = [0]

    async def worker():
        while True:
            i = counter[0]
            if i >= ops:
                return
            counter[0] += 1
            n = nodes[i % len(nodes)]
            await n.set(f"k{i}", VALUE)
            await n.get(f"k{i}")

    await asyncio.gather(*[worker() for _ in range(concurrency)])


async def _run_cprofile(ops: int, concurrency: int, save: str | None) -> None:
    nodes = _make_nodes()
    for n in nodes:
        await n.start()
    await asyncio.sleep(0.05)

    pr = cProfile.Profile()
    pr.enable()
    await _bench_coro(nodes, ops, concurrency)
    pr.disable()

    for n in nodes:
        await n.stop()

    # Print top-20 by cumulative time
    stream = io.StringIO()
    ps = pstats.Stats(pr, stream=stream).sort_stats("cumulative")
    ps.print_stats(20)
    section(
        f"cProfile  top-20 by cumtime  ({ops:,} set+get pairs, concurrency={concurrency})"
    )
    # Strip absolute paths for readability
    output = stream.getvalue()
    for line in output.splitlines():
        stripped = line
        for addr in ADDRS:
            stripped = stripped.replace(addr, addr.split(":")[1])
        print(stripped)

    if save:
        pr.dump_stats(save)
        print(f"\n  Profile saved to: {save}")
        print(f"  Visualise with:  snakeviz {save}")


# --------------------------------------------------------------------------- #
# 2. Step-timing pass                                                          #
# --------------------------------------------------------------------------- #


def _timeit_sync(fn, reps: int) -> float:
    """Return median nanoseconds per call."""
    times = []
    for _ in range(reps):
        t0 = time.perf_counter_ns()
        fn()
        times.append(time.perf_counter_ns() - t0)
    times.sort()
    return times[len(times) // 2]


async def _timeit_async(coro_fn, reps: int) -> float:
    times = []
    for _ in range(reps):
        t0 = time.perf_counter_ns()
        await coro_fn()
        times.append(time.perf_counter_ns() - t0)
    times.sort()
    return times[len(times) // 2]


async def _step_timing(ops: int) -> None:
    section("Step-timing breakdown  (median per operation)")

    reps = min(ops, 5000)

    # ── 1. LocalStore ────────────────────────────────────────────────────── #
    store = LocalStore()
    store_set_ns = _timeit_sync(lambda: store.set("bench", VALUE), reps)
    store_get_ns = _timeit_sync(lambda: store.get("bench"), reps)

    # ── 2. RendezvousRing lookup ─────────────────────────────────────────── #
    ring = RendezvousRing()
    for a in ADDRS:
        ring.add(NodeInfo.from_address(a))
    ring_ns = _timeit_sync(lambda: ring.get_node("bench-key"), reps)

    # ── 3. Pickle encode / decode ─────────────────────────────────────────── #
    msg = Message(MessageType.SET, {"key": "bench", "value": VALUE})
    enc_ns = _timeit_sync(
        lambda: pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL), reps
    )
    enc_data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    dec_ns = _timeit_sync(lambda: pickle.loads(enc_data), reps)  # noqa: S301

    # ── 4. Raw loopback TCP roundtrip ─────────────────────────────────────── #
    # Minimal echo server to measure pure kernel TCP overhead
    echo_ready = asyncio.Event()
    echo_port = 7399

    async def echo_server(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    return
                writer.write(data)
                await writer.drain()
        finally:
            writer.close()

    srv = await asyncio.start_server(echo_server, "127.0.0.1", echo_port)
    echo_ready.set()

    # Connect once and reuse — measures warm-socket RTT, same as pool does
    _r, _w = await asyncio.open_connection("127.0.0.1", echo_port)
    for _ in range(10):  # warmup
        _w.write(enc_data)
        await _w.drain()
        await _r.read(len(enc_data))

    async def raw_rtt() -> None:
        _w.write(enc_data)
        await _w.drain()
        await _r.read(len(enc_data))

    raw_rtt_ns = await _timeit_async(raw_rtt, min(reps, 500))
    _w.close()
    srv.close()
    await srv.wait_closed()

    # ── 5. Full node.set / node.get ───────────────────────────────────────── #
    nodes = _make_nodes()
    for n in nodes:
        await n.start()
    await asyncio.sleep(0.05)

    key_idx = [0]

    async def full_set() -> None:
        k = f"k{key_idx[0]}"
        key_idx[0] += 1
        await nodes[0].set(k, VALUE)

    # Cycle through all set keys so full_get gets a representative mix
    # of local and remote routing (~1/3 local, ~2/3 remote).
    _get_keys: list[str] = []
    _get_i = [0]

    async def full_get() -> None:
        k = _get_keys[_get_i[0] % len(_get_keys)]
        _get_i[0] += 1
        await nodes[0].get(k)

    full_set_ns = await _timeit_async(full_set, min(reps, 500))
    _get_keys[:] = [f"k{i}" for i in range(key_idx[0])]
    full_get_ns = await _timeit_async(full_get, min(reps, 500))

    for n in nodes:
        await n.stop()

    # ── Print table ──────────────────────────────────────────────────────── #
    rows = [
        ("LocalStore.set", store_set_ns, "pure dict write + lock"),
        ("LocalStore.get", store_get_ns, "pure dict read + lock"),
        ("RendezvousRing lookup", ring_ns, "sha256 × n_nodes"),
        ("pickle.dumps (msg)", enc_ns, "encode request"),
        ("pickle.loads (msg)", dec_ns, "decode response"),
        ("raw TCP roundtrip", raw_rtt_ns, "warm socket, loopback kernel cost"),
        ("node.set  (full)", full_set_ns, "ring + pool + pickle + TCP"),
        ("node.get  (full)", full_get_ns, "ring + pool + pickle + TCP"),
    ]

    baseline = full_set_ns or 1
    print(f"\n  {'Operation':<26}  {'Median':>10}  {'% of node.set':>14}  Notes")
    print(f"  {'─'*26}  {'─'*10}  {'─'*14}  {'─'*30}")
    for name, ns, note in rows:
        frac = ns / baseline
        print(f"  {name:<26}  {fmt_ns(ns):>10}  {frac*100:>13.1f}%  {note}")

    print()
    protocol_overhead = raw_rtt_ns + enc_ns + dec_ns
    print(f"  Protocol overhead (encode+decode+RTT) : {fmt_ns(protocol_overhead)}")
    print(f"  node.set total                        : {fmt_ns(full_set_ns)}")
    unexplained = max(0, full_set_ns - protocol_overhead - ring_ns)
    print(f"  Coroutine/pool scheduling overhead    : {fmt_ns(unexplained)}")
    print(f"\n  Event loop : {_LOOP}")


# --------------------------------------------------------------------------- #
# Shared helpers                                                               #
# --------------------------------------------------------------------------- #


def _make_nodes() -> list[AsyncStorageNode]:
    return [
        AsyncStorageNode(
            StaticDiscovery([a for a in ADDRS if a != addr]),
            "127.0.0.1",
            int(addr.split(":")[1]),
        )
        for addr in ADDRS
    ]


# --------------------------------------------------------------------------- #
# Entry point                                                                  #
# --------------------------------------------------------------------------- #


async def main(
    ops: int, concurrency: int, save: str | None, skip_cprofile: bool
) -> None:
    if not skip_cprofile:
        await _run_cprofile(ops, concurrency, save)
    await _step_timing(ops)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dlstorage async benchmark profiler")
    p.add_argument(
        "--ops", type=int, default=10_000, help="ops for cProfile pass (default 10 000)"
    )
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument(
        "--save",
        type=str,
        default=None,
        help="save .prof file for snakeviz (e.g. --save out.prof)",
    )
    p.add_argument(
        "--no-cprofile",
        action="store_true",
        help="skip cProfile pass, only show step-timing breakdown",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.ops, args.concurrency, args.save, args.no_cprofile))
