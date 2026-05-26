"""
Benchmark: 1 000 000 reads + 1 000 000 writes across a 3-node local cluster.

Usage:
    .venv/bin/python benchmark.py [--ops 1000000] [--concurrency 1024] [--value-size 64]

Design:
  - N persistent worker coroutines share a single atomic counter (safe because
    asyncio is cooperative – no await between counter read and increment).
  - No asyncio.Lock anywhere: list.append / int increment are atomic in a
    single-threaded event loop.
  - Progress printed every 100k ops without the overhead of batch boundaries.
"""

from __future__ import annotations

import argparse
import asyncio
import time

from dlstorage import StaticDiscovery, StorageNode
from dlstorage.types import Message, MessageType

# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    k = (len(data) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(data) - 1)
    return data[lo] + (data[hi] - data[lo]) * (k - lo)


def fmt_ns(ns: float) -> str:
    if ns < 1_000:
        return f"{ns:.0f} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.1f} µs"
    return f"{ns / 1_000_000:.2f} ms"


def print_results(
    label: str, latencies: list[float], errors: int, elapsed: float
) -> None:
    n = len(latencies) + errors
    latencies.sort()
    ops_s = n / elapsed if elapsed > 0 else 0
    print(f"\n{'─' * 54}")
    print(f"  {label}")
    print(f"{'─' * 54}")
    print(f"  total ops   : {n:>12,}")
    print(f"  errors      : {errors:>12,}  ({errors / n * 100:.2f}%)")
    print(f"  elapsed     : {elapsed:>12.2f} s")
    print(f"  throughput  : {ops_s:>12,.0f} ops/s")
    if latencies:
        print(f"  p50 latency : {fmt_ns(percentile(latencies, 50)):>12}")
        print(f"  p95 latency : {fmt_ns(percentile(latencies, 95)):>12}")
        print(f"  p99 latency : {fmt_ns(percentile(latencies, 99)):>12}")
        print(f"  max latency : {fmt_ns(latencies[-1]):>12}")
    print(f"{'─' * 54}")


# --------------------------------------------------------------------------- #
# Worker-pool runner                                                           #
# --------------------------------------------------------------------------- #


async def run_phase(
    label: str,
    nodes: list[StorageNode],
    ops: int,
    concurrency: int,
    op_fn,  # async (index, node) -> bool
) -> tuple[list[float], int]:
    """
    Run `ops` operations using `concurrency` persistent worker coroutines.

    Workers share a plain int counter – safe because asyncio is cooperative:
    there is no `await` between reading and incrementing the counter, so no
    other coroutine can interleave.
    """
    counter = [0]  # next op index
    reported = [0]  # last printed milestone
    latencies: list[float] = []
    errors = [0]

    async def worker() -> None:
        while True:
            i = counter[0]
            if i >= ops:
                return
            counter[0] += 1  # no await between these two lines → safe

            node = nodes[i % len(nodes)]
            t0 = time.perf_counter_ns()
            ok = await op_fn(i, node)
            dt = time.perf_counter_ns() - t0

            if ok:
                latencies.append(dt)
            else:
                errors[0] += 1

            done = counter[0]
            if done - reported[0] >= 100_000:
                reported[0] = (done // 100_000) * 100_000
                print(f"  {label}  {done:>9,} / {ops:,}", end="\r", flush=True)

    await asyncio.gather(*[worker() for _ in range(concurrency)])
    print(f"  {label}  {ops:>9,} / {ops:,}")
    return latencies, errors[0]


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #


async def main(ops: int, concurrency: int, value_size: int) -> None:
    addrs = ["127.0.0.1:7101", "127.0.0.1:7102", "127.0.0.1:7103"]
    nodes = [
        StorageNode(
            "127.0.0.1",
            int(addr.split(":")[1]),
            StaticDiscovery([a for a in addrs if a != addr]),
            replication=1,
            max_conns=concurrency,
        )
        for addr in addrs
    ]

    print(f"\ndlstorage benchmark")
    print(f"  ops         : {ops:,}")
    print(f"  concurrency : {concurrency}")
    print(f"  value size  : {value_size} bytes")
    print(f"  nodes       : {len(nodes)}")
    print(f"  replication : 1\n")

    for node in nodes:
        await node.start()
    await asyncio.sleep(0.05)

    value = b"x" * value_size

    # ---- WARMUP ------------------------------------------------------------ #
    # Pre-open `concurrency` connections per (node, peer) pair so no TCP
    # handshakes happen during the timed phases.  We batch by max_conns to
    # avoid overwhelming the server accept queue.
    print("Warming up connections …")
    batch = min(concurrency, 128)
    ping = Message(MessageType.PING, {})
    for node in nodes:
        peers = [p for p in node._ring.nodes() if p != node.info]
        for peer in peers:
            for _ in range(0, concurrency, batch):
                tasks = [
                    node._pool.execute(peer.host, peer.port, ping) for _ in range(batch)
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
    print("Connections warmed.\n")

    async def do_write(i: int, node: StorageNode) -> bool:
        return await node.set(f"k{i}", value)

    print(f"Running {ops:,} writes …")
    t0 = time.perf_counter()
    write_lat, write_err = await run_phase("writes", nodes, ops, concurrency, do_write)
    write_elapsed = time.perf_counter() - t0

    # ---- READS ------------------------------------------------------------- #
    async def do_read(i: int, node: StorageNode) -> bool:
        return await node.get(f"k{i}") is not None

    print(f"\nRunning {ops:,} reads …")
    t0 = time.perf_counter()
    read_lat, read_err = await run_phase("reads ", nodes, ops, concurrency, do_read)
    read_elapsed = time.perf_counter() - t0

    # ---- RESULTS ----------------------------------------------------------- #
    print_results("WRITES", write_lat, write_err, write_elapsed)
    print_results("READS ", read_lat, read_err, read_elapsed)

    for node in nodes:
        await node.stop()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dlstorage benchmark")
    p.add_argument("--ops", type=int, default=1_000_000)
    p.add_argument("--concurrency", type=int, default=1024)
    p.add_argument("--value-size", type=int, default=64)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        import resource

        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 65536), hard))
    except Exception:
        pass
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("(using uvloop)")
    except ImportError:
        print("(uvloop not installed – pip install uvloop for ~2-4× speedup)")
    asyncio.run(main(args.ops, args.concurrency, args.value_size))
