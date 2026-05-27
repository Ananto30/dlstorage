"""
Async benchmark: 100_000 reads + 100_000 writes across a 3-node local cluster.
Each node runs in its own process so all three can run on separate CPU cores.

Usage:
    .venv/bin/python benchmarks/nodes/async_bench.py [--ops 100000] [--concurrency 1024] [--value-size 64]
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from benchmarks.framework import async_run_phase, print_results, try_uvloop
from dlstorage import StaticDiscovery, AsyncStorageNode
from dlstorage.types import Message, MessageType

ADDRS = ["127.0.0.1:7101", "127.0.0.1:7102", "127.0.0.1:7103"]


async def _node_loop(
    host: str,
    port: int,
    all_addrs: list[str],
    concurrency: int,
    cmd_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
) -> None:
    loop = asyncio.get_event_loop()
    node = AsyncStorageNode(
        StaticDiscovery([a for a in all_addrs if a != f"{host}:{port}"]),
        host,
        port,
        replication=1,
    )
    await node.start()
    await asyncio.sleep(0.05)

    # Warm up connections to peers so handshakes don't skew timed phases.
    batch = min(concurrency, 128)
    ping = Message(MessageType.PING, {})
    for peer in [p for p in node._ring.nodes() if p != node.info]:
        for _ in range(0, concurrency, batch):
            await asyncio.gather(
                *[node._pool.execute(peer.host, peer.port, ping) for _ in range(batch)],
                return_exceptions=True,
            )

    result_queue.put("ready")

    while True:
        # run_in_executor keeps the event loop alive while waiting for a command
        cmd = await loop.run_in_executor(None, cmd_queue.get)
        if cmd is None:
            break

        phase, start_i, end_i, value_size = cmd
        value = b"x" * value_size

        if phase == "write":

            async def op(i: int, _v: bytes = value) -> bool:
                return await node.set(f"k{i}", _v)

        else:

            async def op(i: int) -> bool:
                return await node.get(f"k{i}") is not None

        result_queue.put(
            await async_run_phase(
                None, end_i - start_i, concurrency, op, offset=start_i
            )
        )

    await node.stop()


def _node_process(
    addr: str,
    all_addrs: list[str],
    concurrency: int,
    cmd_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
) -> None:
    try_uvloop()
    host, port_s = addr.split(":")
    asyncio.run(
        _node_loop(host, int(port_s), all_addrs, concurrency, cmd_queue, result_queue)
    )


def main(ops: int, concurrency: int, value_size: int) -> None:
    n = len(ADDRS)
    cmd_queues: list[multiprocessing.Queue] = [
        multiprocessing.Queue() for _ in range(n)
    ]
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    procs = [
        multiprocessing.Process(
            target=_node_process,
            args=(ADDRS[i], ADDRS, concurrency, cmd_queues[i], result_queue),
            daemon=True,
        )
        for i in range(n)
    ]

    print(f"\ndlstorage async benchmark  (one process per node)")
    print(f"  ops         : {ops:,}")
    print(f"  concurrency : {concurrency}  (per node)")
    print(f"  value size  : {value_size} bytes")
    print(f"  nodes       : {n}")
    print(f"  replication : 1\n")

    for p in procs:
        p.start()

    print("Warming up connections …")
    for _ in range(n):
        assert result_queue.get() == "ready"
    print("Connections warmed.\n")

    # Split ops evenly: node i handles keys [start_i, end_i)
    ops_per_node = ops // n

    print(f"Running {ops:,} writes …")
    for i, q in enumerate(cmd_queues):
        q.put(("write", i * ops_per_node, (i + 1) * ops_per_node, value_size))
    t0 = time.perf_counter()
    write_results = [result_queue.get() for _ in range(n)]
    write_elapsed = time.perf_counter() - t0

    print(f"\nRunning {ops:,} reads …")
    for i, q in enumerate(cmd_queues):
        q.put(("read", i * ops_per_node, (i + 1) * ops_per_node, value_size))
    t0 = time.perf_counter()
    read_results = [result_queue.get() for _ in range(n)]
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
    p = argparse.ArgumentParser(description="dlstorage async benchmark")
    p.add_argument("--ops", type=int, default=100_000)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--value-size", type=int, default=4*1024)
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
