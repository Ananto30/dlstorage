"""
Redis benchmark: 1 000 000 reads + 1 000 000 writes against a local Redis instance.

Mirrors benchmark.py exactly (same worker-pool pattern, same output format)
so results are directly comparable to the dlstorage async benchmark.

Dependencies:
    pip install redis   (redis-py ships redis.asyncio)

Usage:
    .venv/bin/python benchmark_redis.py [--ops 1000000] [--concurrency 1024] [--value-size 64]
    .venv/bin/python benchmark_redis.py --host 127.0.0.1 --port 6379
"""

from __future__ import annotations

import argparse
import asyncio
import time

import redis.asyncio as aioredis

# --------------------------------------------------------------------------- #
# Helpers (identical to benchmark.py)                                         #
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
# Worker-pool runner (identical structure to benchmark.py)                    #
# --------------------------------------------------------------------------- #


async def run_phase(
    label: str,
    pool: aioredis.ConnectionPool,
    ops: int,
    concurrency: int,
    op_fn,  # async (index, redis_client) -> bool
) -> tuple[list[float], int]:
    counter = [0]
    reported = [0]
    latencies: list[float] = []
    errors = [0]

    async def worker() -> None:
        # Each worker owns its own client so connections are truly parallel.
        client = aioredis.Redis(connection_pool=pool)
        try:
            while True:
                i = counter[0]
                if i >= ops:
                    return
                counter[0] += 1

                t0 = time.perf_counter_ns()
                ok = await op_fn(i, client)
                dt = time.perf_counter_ns() - t0

                if ok:
                    latencies.append(dt)
                else:
                    errors[0] += 1

                done = counter[0]
                if done - reported[0] >= 100_000:
                    reported[0] = (done // 100_000) * 100_000
                    print(f"  {label}  {done:>9,} / {ops:,}", end="\r", flush=True)
        finally:
            await client.aclose()

    await asyncio.gather(*[worker() for _ in range(concurrency)])
    print(f"  {label}  {ops:>9,} / {ops:,}")
    return latencies, errors[0]


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #


async def main(
    ops: int,
    concurrency: int,
    value_size: int,
    host: str,
    port: int,
) -> None:
    pool = aioredis.ConnectionPool(
        host=host,
        port=port,
        max_connections=concurrency,
        decode_responses=False,
    )

    # Verify Redis is reachable
    probe = aioredis.Redis(connection_pool=pool)
    try:
        await probe.ping()
    except Exception as exc:
        print(f"Cannot connect to Redis at {host}:{port} – {exc}")
        return
    finally:
        await probe.aclose()

    print(f"\nRedis benchmark  ({host}:{port})")
    print(f"  ops         : {ops:,}")
    print(f"  concurrency : {concurrency}")
    print(f"  value size  : {value_size} bytes")
    print()

    value = b"x" * value_size

    # ---- WARMUP ------------------------------------------------------------ #
    print("Warming up connections …")
    warmup_clients = [aioredis.Redis(connection_pool=pool) for _ in range(concurrency)]
    await asyncio.gather(*[c.ping() for c in warmup_clients], return_exceptions=True)
    await asyncio.gather(*[c.aclose() for c in warmup_clients], return_exceptions=True)
    print("Connections warmed.\n")

    # ---- WRITES ------------------------------------------------------------ #
    async def do_write(i: int, client: aioredis.Redis) -> bool:
        return await client.set(f"k{i}", value) is not None

    print(f"Running {ops:,} writes …")
    t0 = time.perf_counter()
    write_lat, write_err = await run_phase("writes", pool, ops, concurrency, do_write)
    write_elapsed = time.perf_counter() - t0

    # ---- READS ------------------------------------------------------------- #
    async def do_read(i: int, client: aioredis.Redis) -> bool:
        return await client.get(f"k{i}") is not None

    print(f"\nRunning {ops:,} reads …")
    t0 = time.perf_counter()
    read_lat, read_err = await run_phase("reads ", pool, ops, concurrency, do_read)
    read_elapsed = time.perf_counter() - t0

    # ---- RESULTS ----------------------------------------------------------- #
    print_results("WRITES", write_lat, write_err, write_elapsed)
    print_results("READS ", read_lat, read_err, read_elapsed)

    await pool.aclose()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Redis benchmark (apples-to-apples vs dlstorage)"
    )
    p.add_argument("--ops", type=int, default=1_000_000)
    p.add_argument("--concurrency", type=int, default=1024)
    p.add_argument("--value-size", type=int, default=64)
    p.add_argument("--host", type=str, default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
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
    asyncio.run(main(args.ops, args.concurrency, args.value_size, args.host, args.port))
