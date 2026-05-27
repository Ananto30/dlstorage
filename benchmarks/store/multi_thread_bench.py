"""
LocalStore multi-threaded benchmark: separate SET / GET / DELETE phases.

Unlike bench.py (which interleaves all three ops per key), this file runs
each operation as a pure phase so contention characteristics are isolated.
Supports a thread-count sweep (--sweep) to show horizontal scaling.

Usage:
    .venv/bin/python benchmarks/store/multi_thread_bench.py [--ops 1_000_000] [--threads 8] [--sweep] [--value-size 64]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from benchmarks.framework import (SyncProgress, fmt_ns, percentile,
                                  print_results, sync_run_phase)
from dlstorage.store import LocalStore


def _make_set_worker(
    store: LocalStore, value: bytes, ops: int, n: int, progress: SyncProgress
):
    def worker(tid: int) -> tuple[list[float], int]:
        lats: list[float] = []
        for i in range(tid, ops, n):
            t = time.perf_counter_ns()
            store.set(f"k{i}", value)
            lats.append(time.perf_counter_ns() - t)
            progress.tick()
        return lats, 0

    return worker


def _make_get_worker(store: LocalStore, ops: int, n: int, progress: SyncProgress):
    def worker(tid: int) -> tuple[list[float], int]:
        lats: list[float] = []
        errors = 0
        for i in range(tid, ops, n):
            t = time.perf_counter_ns()
            v = store.get(f"k{i}")
            lats.append(time.perf_counter_ns() - t)
            if v is None:
                errors += 1
            progress.tick()
        return lats, errors

    return worker


def _make_delete_worker(store: LocalStore, ops: int, n: int, progress: SyncProgress):
    def worker(tid: int) -> tuple[list[float], int]:
        lats: list[float] = []
        for i in range(tid, ops, n):
            t = time.perf_counter_ns()
            store.delete(f"k{i}")
            lats.append(time.perf_counter_ns() - t)
            progress.tick()
        return lats, 0

    return worker


def run_one(ops: int, n_threads: int, value_size: int) -> dict[str, float]:
    """Run all three phases and return throughput for each."""
    store = LocalStore()
    value = b"x" * value_size

    results: dict[str, float] = {}

    for phase_name, make_worker, needs_prepop in [
        ("SET   ", _make_set_worker, False),
        ("GET   ", _make_get_worker, True),
        ("DELETE", _make_delete_worker, False),
    ]:
        # Ensure keys exist before GET phase
        if needs_prepop:
            _prepop(store, ops, value)

        progress = SyncProgress(phase_name, ops)

        if phase_name.startswith("SET"):
            wfn = make_worker(store, value, ops, n_threads, progress)
        elif phase_name.startswith("GET"):
            wfn = make_worker(store, ops, n_threads, progress)
        else:
            wfn = make_worker(store, ops, n_threads, progress)

        t0 = time.perf_counter()
        lats, errors = sync_run_phase(phase_name, ops, n_threads, wfn)
        elapsed = time.perf_counter() - t0

        print(f"  {phase_name}  {ops:>9,} / {ops:,}")
        print_results(phase_name, lats, elapsed, errors=errors)
        results[phase_name.strip()] = len(lats) / elapsed if elapsed > 0 else 0

    return results


def _prepop(store: LocalStore, ops: int, value: bytes) -> None:
    """Fast single-threaded pre-populate (no timing)."""
    for i in range(ops):
        store.set(f"k{i}", value)


def run_sweep(ops: int, max_threads: int, value_size: int) -> None:
    """Run at 1, 2, 4, 8, … up to max_threads and print a summary table."""
    thread_counts: list[int] = []
    t = 1
    while t <= max_threads:
        thread_counts.append(t)
        t *= 2
    if thread_counts[-1] != max_threads:
        thread_counts.append(max_threads)

    header = (
        f"{'threads':>8}  {'SET ops/s':>14}  {'GET ops/s':>14}  {'DELETE ops/s':>14}"
    )
    rows: list[str] = []

    for n in thread_counts:
        print(f"\n{'═' * 60}")
        print(f"  threads = {n}")
        print(f"{'═' * 60}")
        results = run_one(ops, n, value_size)
        row = (
            f"{n:>8}  "
            f"{results.get('SET', 0):>14,.0f}  "
            f"{results.get('GET', 0):>14,.0f}  "
            f"{results.get('DELETE', 0):>14,.0f}"
        )
        rows.append(row)

    print(f"\n\n{'═' * 60}")
    print("  Scaling summary")
    print(f"{'═' * 60}")
    print(f"  ops / thread-count : {ops:,}")
    print(f"  value size         : {value_size} bytes\n")
    print(f"  {header}")
    print(f"  {'─' * (len(header))}")
    for row in rows:
        print(f"  {row}")
    print()


def main() -> None:
    p = argparse.ArgumentParser(description="LocalStore multi-threaded phase benchmark")
    p.add_argument("--ops", type=int, default=1_000_000)
    p.add_argument(
        "--threads", type=int, default=8, help="thread count (or sweep ceiling)"
    )
    p.add_argument(
        "--sweep", action="store_true", help="sweep 1→2→4→…→threads and print table"
    )
    p.add_argument("--value-size", type=int, default=64)
    args = p.parse_args()

    print(f"\nLocalStore multi-threaded benchmark")
    print(f"  ops        : {args.ops:,}")
    print(f"  threads    : {args.threads}")
    print(f"  value size : {args.value_size} bytes")
    print(f"  sweep      : {args.sweep}")

    if args.sweep:
        run_sweep(args.ops, args.threads, args.value_size)
    else:
        run_one(args.ops, args.threads, args.value_size)


if __name__ == "__main__":
    main()
