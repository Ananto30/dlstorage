"""
Shared utilities for all dlstorage benchmarks.

Exports
-------
percentile(data, p)           interpolated percentile
fmt_ns(ns)                    nanoseconds → human-readable string
print_results(...)            formatted result table (errors optional)
try_uvloop()                  apply uvloop if available; return loop name
async_run_phase(...)          persistent async worker-pool runner
SyncProgress                  thread-safe progress reporter for sync workers
sync_run_phase(...)           threaded worker-pool runner
"""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Awaitable, Callable

#############
# Formatting
#############


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
    label: str,
    latencies: list[float],
    elapsed: float,
    *,
    errors: int = 0,
) -> None:
    n = len(latencies) + errors
    if n == 0:
        return
    latencies.sort()
    ops_s = n / elapsed if elapsed > 0 else 0
    print(f"\n{'─' * 54}")
    print(f"  {label}")
    print(f"{'─' * 54}")
    print(f"  total ops   : {n:>12,}")
    if errors:
        print(f"  errors      : {errors:>12,}  ({errors / n * 100:.2f}%)")
    print(f"  elapsed     : {elapsed:>12.2f} s")
    print(f"  throughput  : {ops_s:>12,.0f} ops/s")
    if latencies:
        print(f"  p50 latency : {fmt_ns(percentile(latencies, 50)):>12}")
        print(f"  p95 latency : {fmt_ns(percentile(latencies, 95)):>12}")
        print(f"  p99 latency : {fmt_ns(percentile(latencies, 99)):>12}")
        print(f"  max latency : {fmt_ns(latencies[-1]):>12}")
    print(f"{'─' * 54}")


############
# Event loop
############


def try_uvloop() -> str:
    """Apply uvloop EventLoopPolicy if installed. Returns 'uvloop' or 'asyncio'."""
    try:
        import uvloop  # type: ignore[import]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        return "uvloop"
    except ImportError:
        return "asyncio"


###################
# Async worker pool
###################


async def async_run_phase(
    label: str | None,
    ops: int,
    concurrency: int,
    op_fn: Callable[[int], Awaitable[bool]],
    *,
    offset: int = 0,
) -> tuple[list[float], int]:
    """
    Run *ops* operations using *concurrency* persistent worker coroutines.

    op_fn(index: int) -> bool   True = success, False = error.

    offset: added to every index passed to op_fn (useful when splitting a
            key-space across multiple processes).
    label:  progress prefix printed every 100 k ops; pass None to suppress
            all printing (e.g. when running inside a subprocess).

    No lock needed: asyncio is single-threaded and there is no ``await``
    between the counter read and increment, so no other coroutine can
    interleave.
    """
    counter = [0]
    reported = [0]
    latencies: list[float] = []
    errors = [0]

    async def worker() -> None:
        while True:
            i = counter[0]
            if i >= ops:
                return
            counter[0] += 1
            t0 = time.perf_counter_ns()
            ok = await op_fn(offset + i)
            dt = time.perf_counter_ns() - t0
            if ok:
                latencies.append(dt)
            else:
                errors[0] += 1
            if label is not None:
                done = counter[0]
                if done - reported[0] >= 100_000:
                    reported[0] = (done // 100_000) * 100_000
                    print(f"  {label}  {done:>9,} / {ops:,}", end="\r", flush=True)

    await asyncio.gather(*[worker() for _ in range(concurrency)])
    if label is not None:
        print(f"  {label}  {ops:>9,} / {ops:,}")
    return latencies, errors[0]


##################
# Sync worker pool
##################


class SyncProgress:
    """Thread-safe milestone progress reporter for sync worker threads."""

    def __init__(self, label: str, ops: int) -> None:
        self.label = label
        self.ops = ops
        self._lock = threading.Lock()
        self._done = 0
        self._last = 0

    def tick(self, count: int = 1) -> None:
        with self._lock:
            self._done += count
            milestone = (self._done // 100_000) * 100_000
            if milestone > self._last and milestone <= self.ops:
                self._last = milestone
                print(
                    f"  {self.label}  {milestone:>9,} / {self.ops:,}",
                    end="\r",
                    flush=True,
                )


def sync_run_phase(
    label: str,
    ops: int,
    concurrency: int,
    worker_fn: Callable[[int], tuple[list[float], int]],
) -> tuple[list[float], int]:
    """
    Run *ops* operations using *concurrency* OS threads via ThreadPoolExecutor.

    worker_fn(thread_id: int) -> (latencies_ns, errors)
    Each worker handles a stride of keys: ``range(thread_id, ops, concurrency)``.
    """
    all_latencies: list[float] = []
    total_errors = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = [pool.submit(worker_fn, tid) for tid in range(concurrency)]
        for fut in as_completed(futs):
            lats, errs = fut.result()
            all_latencies.extend(lats)
            total_errors += errs
    return all_latencies, total_errors
