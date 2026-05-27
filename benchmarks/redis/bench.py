"""
Redis benchmark: 100_000 reads + 100_000 writes against a local Redis instance.

Mirrors nodes/async_bench.py exactly (same worker-pool pattern, same output
format) so results are directly comparable to the dlstorage async benchmark.
Each worker process runs its own asyncio event loop + connection pool so the
client side is not bottlenecked by Python's GIL.

Dependencies:
    uv add redis   (redis-py ships redis.asyncio)

Usage:
    .venv/bin/python benchmarks/redis/bench.py [--ops 100000] [--concurrency 1024]
    .venv/bin/python benchmarks/redis/bench.py --host 127.0.0.1 --port 6379
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import redis.asyncio as aioredis

from benchmarks.framework import async_run_phase, print_results, try_uvloop

# ---------------------------------------------------------------------------
# Per-process worker logic
# ---------------------------------------------------------------------------


async def _worker_loop(
    host: str,
    port: int,
    concurrency: int,
    cmd_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
) -> None:
    loop = asyncio.get_event_loop()
    pool = aioredis.ConnectionPool.from_url(
        f"redis://{host}:{port}", max_connections=concurrency
    )
    client = aioredis.Redis(connection_pool=pool)

    await asyncio.gather(
        *[client.ping() for _ in range(concurrency)], return_exceptions=True
    )
    result_queue.put("ready")

    while True:
        cmd = await loop.run_in_executor(None, cmd_queue.get)
        if cmd is None:
            break

        phase, start_i, end_i, value_size = cmd
        value = b"x" * value_size

        if phase == "write":

            async def op(i: int, _v: bytes = value) -> bool:
                return await client.set(f"k{i}", _v) is not None

        else:

            async def op(i: int) -> bool:
                return await client.get(f"k{i}") is not None

        result_queue.put(
            await async_run_phase(
                None, end_i - start_i, concurrency, op, offset=start_i
            )
        )

    await client.aclose()


def _worker_process(
    host: str,
    port: int,
    concurrency: int,
    cmd_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
) -> None:
    try_uvloop()
    asyncio.run(_worker_loop(host, port, concurrency, cmd_queue, result_queue))


# ---------------------------------------------------------------------------
# Main coordinator
# ---------------------------------------------------------------------------


def main(
    ops: int,
    concurrency: int,
    value_size: int,
    host: str,
    port: int,
    workers: int,
) -> None:
    cmd_queues: list[multiprocessing.Queue] = [
        multiprocessing.Queue() for _ in range(workers)
    ]
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    procs = [
        multiprocessing.Process(
            target=_worker_process,
            args=(host, port, concurrency, cmd_queues[i], result_queue),
            daemon=True,
        )
        for i in range(workers)
    ]

    print(f"\nRedis benchmark  ({host}:{port})")
    print(f"  ops         : {ops:,}")
    print(f"  concurrency : {concurrency}  (per worker)")
    print(f"  value size  : {value_size} bytes")
    print(f"  workers     : {workers}\n")

    for p in procs:
        p.start()

    print("Warming up connections …")
    for _ in range(workers):
        assert result_queue.get() == "ready"
    print("Connections warmed.\n")

    ops_per_worker = ops // workers

    print(f"Running {ops:,} writes …")
    for i, q in enumerate(cmd_queues):
        q.put(("write", i * ops_per_worker, (i + 1) * ops_per_worker, value_size))
    t0 = time.perf_counter()
    write_results = [result_queue.get() for _ in range(workers)]
    write_elapsed = time.perf_counter() - t0

    print(f"\nRunning {ops:,} reads …")
    for i, q in enumerate(cmd_queues):
        q.put(("read", i * ops_per_worker, (i + 1) * ops_per_worker, value_size))
    t0 = time.perf_counter()
    read_results = [result_queue.get() for _ in range(workers)]
    read_elapsed = time.perf_counter() - t0

    for q in cmd_queues:
        q.put(None)
    for p in procs:
        p.join(timeout=5)

    write_lat = [lat for lats, _ in write_results for lat in lats]
    write_err = sum(e for _, e in write_results)
    read_lat = [lat for lats, _ in read_results for lat in lats]
    read_err = sum(e for _, e in read_results)

    print_results("WRITES", write_lat, write_elapsed, errors=write_err)
    print_results("READS ", read_lat, read_elapsed, errors=read_err)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis benchmark (vs dlstorage)")
    p.add_argument("--ops", type=int, default=100_000)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--value-size", type=int, default=4*1024)
    p.add_argument("--host", type=str, default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count(),
        help="client processes (default: cpu count)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        import resource

        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 65536), hard))
    except Exception:
        pass
    main(
        args.ops, args.concurrency, args.value_size, args.host, args.port, args.workers
    )
