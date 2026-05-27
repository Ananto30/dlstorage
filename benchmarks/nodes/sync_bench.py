"""
Sync benchmark: 100_000 reads + 100_000 writes across a 3-node local cluster
using SyncStorageNode (blocking sockets, MuxConnectionPool).
Each node runs in its own process so all three can run on separate CPU cores.

Usage:
    .venv/bin/python benchmarks/nodes/sync_bench.py [--ops 100000] [--concurrency 64] [--value-size 64]
"""

from __future__ import annotations

import argparse
import multiprocessing
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from benchmarks.framework import print_results, sync_run_phase
from dlstorage import StaticDiscovery, StorageNode
from dlstorage.connection_pool.sync import ConnectionPool
from dlstorage.types import Message, MessageType

ADDRS = ["127.0.0.1:7201", "127.0.0.1:7202", "127.0.0.1:7203"]
REPLICATION = 3


def _node_process(
    addr: str,
    all_addrs: list[str],
    concurrency: int,
    cmd_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
) -> None:
    host, port_s = addr.split(":")
    node = StorageNode(
        StaticDiscovery(all_addrs),
        host,
        int(port_s),
        replication=REPLICATION,
        # connection_pool=ConnectionPool(max_per_peer=64),
    )
    node.start()
    time.sleep(0.1)

    ping = Message(MessageType.PING, {})
    for peer in node._ring.nodes():
        if peer == node.info:
            continue
        for _ in range(min(concurrency, node._pool._max)):
            node._pool.execute(peer.host, peer.port, ping)

    result_queue.put("ready")

    while True:
        cmd = cmd_queue.get()
        if cmd is None:
            break

        phase, start_i, end_i, value_size = cmd
        value = b"x" * value_size
        ops_local = end_i - start_i

        if phase == "write":

            def worker_fn(
                thread_id: int,
                _v: bytes = value,
                _si: int = start_i,
                _ops: int = ops_local,
            ) -> tuple[list[float], int]:
                latencies: list[float] = []
                errors = 0
                for i in range(thread_id, _ops, concurrency):
                    t0 = time.perf_counter_ns()
                    ok = node.set(f"k{_si + i}", _v)
                    dt = time.perf_counter_ns() - t0
                    if ok:
                        latencies.append(dt)
                    else:
                        errors += 1
                return latencies, errors

        else:

            def worker_fn(
                thread_id: int, _si: int = start_i, _ops: int = ops_local
            ) -> tuple[list[float], int]:
                latencies: list[float] = []
                errors = 0
                for i in range(thread_id, _ops, concurrency):
                    t0 = time.perf_counter_ns()
                    val = node.get(f"k{_si + i}")
                    dt = time.perf_counter_ns() - t0
                    if val is not None:
                        latencies.append(dt)
                    else:
                        errors += 1
                return latencies, errors

        result_queue.put(sync_run_phase("", ops_local, concurrency, worker_fn))

    node.stop()


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

    print(f"\ndlstorage SYNC benchmark  (one process per node)")
    print(f"  ops         : {ops:,}")
    print(f"  concurrency : {concurrency}  (OS threads, per node)")
    print(f"  value size  : {value_size} bytes")
    print(f"  nodes       : {n}")
    print(f"  replication : {REPLICATION}\n")

    for p in procs:
        p.start()

    print("Warming up connections …")
    for _ in range(n):
        assert result_queue.get() == "ready"
    print("Connections warmed.\n")

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
    p = argparse.ArgumentParser(description="dlstorage sync benchmark")
    p.add_argument("--ops", type=int, default=100_000)
    p.add_argument(
        "--concurrency", type=int, default=64, help="OS threads per node (default 64)"
    )
    p.add_argument("--value-size", type=int, default=4 * 1024)
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
