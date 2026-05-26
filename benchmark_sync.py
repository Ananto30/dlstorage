"""
Sync benchmark: 1 000 000 reads + 1 000 000 writes across a 3-node local cluster
using SyncStorageNode (blocking sockets, thread-per-connection server).

Usage:
    .venv/bin/python benchmark_sync.py [--ops 1000000] [--concurrency 64] [--value-size 64]

Design:
  - N worker threads each own a contiguous slice of the key space
    (thread k handles keys k, k+N, k+2N, …) so work is distributed with
    zero locking during the hot loop.
  - Each thread appends latencies to its own local list – no shared state
    during measurement.
  - Results are merged and printed after all threads finish.
"""

from __future__ import annotations

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dlstorage import StaticDiscovery
from dlstorage.sync_node import SyncStorageNode

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
# Worker                                                                       #
# --------------------------------------------------------------------------- #

# Shared progress counter (GIL ensures atomic int reads/writes in CPython)
_progress = 0
_progress_lock = threading.Lock()
_last_reported = 0


def _maybe_report(done: int, ops: int, label: str) -> None:
    global _progress, _last_reported
    with _progress_lock:
        _progress += 1
        p = _progress
    milestone = (p // 100_000) * 100_000
    if milestone > _last_reported and milestone <= ops:
        _last_reported = milestone
        print(f"  {label}  {milestone:>9,} / {ops:,}", end="\r", flush=True)


def write_worker(
    thread_id: int,
    concurrency: int,
    ops: int,
    nodes: list[SyncStorageNode],
    value: bytes,
) -> tuple[list[float], int]:
    latencies: list[float] = []
    errors = 0
    for i in range(thread_id, ops, concurrency):
        node = nodes[i % len(nodes)]
        t0 = time.perf_counter_ns()
        ok = node.set(f"k{i}", value)
        dt = time.perf_counter_ns() - t0
        if ok:
            latencies.append(dt)
        else:
            errors += 1
        _maybe_report(i, ops, "writes")
    return latencies, errors


def read_worker(
    thread_id: int,
    concurrency: int,
    ops: int,
    nodes: list[SyncStorageNode],
) -> tuple[list[float], int]:
    latencies: list[float] = []
    errors = 0
    for i in range(thread_id, ops, concurrency):
        node = nodes[i % len(nodes)]
        t0 = time.perf_counter_ns()
        val = node.get(f"k{i}")
        dt = time.perf_counter_ns() - t0
        if val is not None:
            latencies.append(dt)
        else:
            errors += 1
        _maybe_report(i, ops, "reads ")
    return latencies, errors


def run_phase(label: str, futures_fn, concurrency: int) -> tuple[list[float], int]:
    global _progress, _last_reported
    _progress = 0
    _last_reported = 0

    all_latencies: list[float] = []
    total_errors = 0

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = futures_fn(pool)
        for fut in as_completed(futs):
            lats, errs = fut.result()
            all_latencies.extend(lats)
            total_errors += errs

    return all_latencies, total_errors


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #


def main(ops: int, concurrency: int, value_size: int) -> None:
    addrs = ["127.0.0.1:7201", "127.0.0.1:7202", "127.0.0.1:7203"]
    nodes = [
        SyncStorageNode(
            "127.0.0.1",
            int(addr.split(":")[1]),
            StaticDiscovery([a for a in addrs if a != addr]),
            replication=1,
            max_conns=concurrency,
        )
        for addr in addrs
    ]

    print(f"\ndlstorage SYNC benchmark")
    print(f"  ops         : {ops:,}")
    print(f"  concurrency : {concurrency}  (OS threads)")
    print(f"  value size  : {value_size} bytes")
    print(f"  nodes       : {len(nodes)}")
    print(f"  replication : 1\n")

    for node in nodes:
        node.start()
    time.sleep(0.1)  # let PEER_ANNOUNCE propagate

    value = b"x" * value_size

    # ---- WARMUP ------------------------------------------------------------ #
    # Pre-fill the connection pool so the timed phases don't include TCP setup.
    print("Warming up connections …")
    from dlstorage.types import Message, MessageType

    ping = Message(MessageType.PING, {})
    for node in nodes:
        for peer in node._ring.nodes():
            if peer == node.info:
                continue
            for _ in range(min(concurrency, node._pool._max)):
                node._pool.execute(peer.host, peer.port, ping)
    print("Connections warmed.\n")

    # ---- WRITES ------------------------------------------------------------ #
    print(f"Running {ops:,} writes …")
    t0 = time.perf_counter()

    def submit_writes(pool: ThreadPoolExecutor):
        return [
            pool.submit(write_worker, tid, concurrency, ops, nodes, value)
            for tid in range(concurrency)
        ]

    write_lat, write_err = run_phase("writes", submit_writes, concurrency)
    write_elapsed = time.perf_counter() - t0
    print(f"  writes  {ops:>9,} / {ops:,}")

    # ---- READS ------------------------------------------------------------- #
    print(f"\nRunning {ops:,} reads …")
    t0 = time.perf_counter()

    def submit_reads(pool: ThreadPoolExecutor):
        return [
            pool.submit(read_worker, tid, concurrency, ops, nodes)
            for tid in range(concurrency)
        ]

    read_lat, read_err = run_phase("reads ", submit_reads, concurrency)
    read_elapsed = time.perf_counter() - t0
    print(f"  reads   {ops:>9,} / {ops:,}")

    # ---- RESULTS ----------------------------------------------------------- #
    print_results("WRITES", write_lat, write_err, write_elapsed)
    print_results("READS ", read_lat, read_err, read_elapsed)

    for node in nodes:
        node.stop()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dlstorage sync benchmark")
    p.add_argument("--ops", type=int, default=1_000_000)
    p.add_argument(
        "--concurrency", type=int, default=64, help="number of OS threads (default 64)"
    )
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
    main(args.ops, args.concurrency, args.value_size)
