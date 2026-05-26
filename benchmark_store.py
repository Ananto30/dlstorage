"""
LocalStore micro-benchmark: raw in-memory set/get/delete throughput.

No network, no nodes – measures the ceiling speed of the local store itself.

Usage:
    .venv/bin/python benchmark_store.py [--ops 1000000] [--threads 1] [--value-size 64]
"""

from __future__ import annotations

import argparse
import threading
import time

from dlstorage.store import LocalStore

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


def print_results(label: str, latencies: list[float], elapsed: float) -> None:
    n = len(latencies)
    latencies.sort()
    ops_s = n / elapsed if elapsed > 0 else 0
    print(f"\n{'─' * 54}")
    print(f"  {label}")
    print(f"{'─' * 54}")
    print(f"  total ops   : {n:>12,}")
    print(f"  elapsed     : {elapsed:>12.4f} s")
    print(f"  throughput  : {ops_s:>12,.0f} ops/s")
    print(f"  p50 latency : {fmt_ns(percentile(latencies, 50)):>12}")
    print(f"  p95 latency : {fmt_ns(percentile(latencies, 95)):>12}")
    print(f"  p99 latency : {fmt_ns(percentile(latencies, 99)):>12}")
    print(f"  max latency : {fmt_ns(latencies[-1]):>12}")
    print(f"{'─' * 54}")


# --------------------------------------------------------------------------- #
# Worker                                                                       #
# --------------------------------------------------------------------------- #


def run_worker(
    thread_id: int,
    n_threads: int,
    ops: int,
    store: LocalStore,
    value: bytes,
    write_out: list[float],
    get_out: list[float],
    delete_out: list[float],
) -> None:
    """Each thread owns a contiguous stride of keys – no lock needed for distribution."""
    w_lats: list[float] = []
    g_lats: list[float] = []
    d_lats: list[float] = []

    for i in range(thread_id, ops, n_threads):
        key = f"k{i}"

        t = time.perf_counter_ns()
        store.set(key, value)
        w_lats.append(time.perf_counter_ns() - t)

        t = time.perf_counter_ns()
        store.get(key)
        g_lats.append(time.perf_counter_ns() - t)

        t = time.perf_counter_ns()
        store.delete(key)
        d_lats.append(time.perf_counter_ns() - t)

    write_out.extend(w_lats)
    get_out.extend(g_lats)
    delete_out.extend(d_lats)


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #


def main(ops: int, n_threads: int, value_size: int) -> None:
    store = LocalStore()
    value = b"x" * value_size

    print(f"\nLocalStore benchmark")
    print(f"  ops         : {ops:,}")
    print(f"  threads     : {n_threads}")
    print(f"  value size  : {value_size} bytes\n")

    write_lats: list[list[float]] = [[] for _ in range(n_threads)]
    get_lats: list[list[float]] = [[] for _ in range(n_threads)]
    del_lats: list[list[float]] = [[] for _ in range(n_threads)]

    threads = [
        threading.Thread(
            target=run_worker,
            args=(
                tid,
                n_threads,
                ops,
                store,
                value,
                write_lats[tid],
                get_lats[tid],
                del_lats[tid],
            ),
            daemon=True,
        )
        for tid in range(n_threads)
    ]

    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_elapsed = time.perf_counter() - t0

    # Merge per-thread results
    all_write = [x for sub in write_lats for x in sub]
    all_get = [x for sub in get_lats for x in sub]
    all_del = [x for sub in del_lats for x in sub]

    print_results("SET   ", all_write, total_elapsed)
    print_results("GET   ", all_get, total_elapsed)
    print_results("DELETE", all_del, total_elapsed)

    # Combined throughput (all 3 ops ran in the same elapsed window)
    total_ops = len(all_write) + len(all_get) + len(all_del)
    print(
        f"\n  combined throughput : {total_ops / total_elapsed:,.0f} ops/s  "
        f"({total_ops:,} ops in {total_elapsed:.4f}s)"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LocalStore micro-benchmark")
    p.add_argument("--ops", type=int, default=1_000_000)
    p.add_argument(
        "--threads", type=int, default=1, help="concurrent writer threads (default 1)"
    )
    p.add_argument("--value-size", type=int, default=64)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.ops, args.threads, args.value_size)
