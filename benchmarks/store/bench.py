"""
LocalStore micro-benchmark: raw in-memory set/get/delete throughput.

No network, no nodes – measures the ceiling speed of the local store itself.

Usage:
    .venv/bin/python benchmarks/store/bench.py [--ops 1000000] [--threads 1] [--value-size 64]
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from benchmarks.framework import print_results
from dlstorage.store import LocalStore


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
    """Each thread owns a stride of keys — no lock needed for distribution."""
    w: list[float] = []
    g: list[float] = []
    d: list[float] = []
    for i in range(thread_id, ops, n_threads):
        key = f"k{i}"
        t = time.perf_counter_ns()
        store.set(key, value)
        w.append(time.perf_counter_ns() - t)

        t = time.perf_counter_ns()
        store.get(key)
        g.append(time.perf_counter_ns() - t)

        t = time.perf_counter_ns()
        store.delete(key)
        d.append(time.perf_counter_ns() - t)

    write_out.extend(w)
    get_out.extend(g)
    delete_out.extend(d)


def main(ops: int, n_threads: int, value_size: int) -> None:
    store = LocalStore()
    value = b"x" * value_size

    print(f"\nLocalStore micro-benchmark")
    print(f"  ops     : {ops:,}")
    print(f"  threads : {n_threads}")
    print(f"  value   : {value_size} bytes\n")

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

    all_write = [x for sub in write_lats for x in sub]
    all_get = [x for sub in get_lats for x in sub]
    all_del = [x for sub in del_lats for x in sub]

    # All three phases ran interleaved in the same elapsed window.
    print_results("SET   ", all_write, total_elapsed)
    print_results("GET   ", all_get, total_elapsed)
    print_results("DELETE", all_del, total_elapsed)

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
