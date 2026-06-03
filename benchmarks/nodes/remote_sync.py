"""
Remote sync benchmark: starts ONE local StorageNode that joins a pre-existing
cluster and drives all reads/writes through it.  Remote peers must already be
running before you launch this script.

Usage:
    .venv/bin/python benchmarks/nodes/remote_sync.py \
        --peers 10.0.0.1:7201 10.0.0.2:7201 10.0.0.3:7201 \
        [--local-port 7299] \
        [--ops 100000] \
        [--concurrency 64] \
        [--value-size 4096]

The local node binds to 127.0.0.1:<local-port> and is used purely as the
benchmark driver; all replication traffic flows to the remote peers.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from benchmarks.framework import print_results, sync_run_phase
from dlstorage import StaticDiscovery, StorageNode
from dlstorage.types import Message, MessageType

DEFAULT_PEERS = ["127.0.0.1:7201", "127.0.0.1:7202", "127.0.0.1:7203"]
DEFAULT_LOCAL_PORT = 7299
REPLICATION = 2


def main(
    peers: list[str],
    local_port: int,
    ops: int,
    concurrency: int,
    value_size: int,
) -> None:
    all_addrs = peers + [f"127.0.0.1:{local_port}"]

    node = StorageNode(
        StaticDiscovery(all_addrs),
        host="127.0.0.1",
        port=local_port,
        replication=REPLICATION,
    )
    node.start()
    time.sleep(0.2)

    # Warm up one connection to each remote peer.
    ping = Message(MessageType.PING, {})
    for peer in node._ring.nodes():
        if peer == node.info:
            continue
        for _ in range(min(concurrency, node._pool._max)):
            node._pool.execute(peer.host, peer.port, ping)

    print(f"\ndlstorage REMOTE SYNC benchmark")
    print(f"  local node  : 127.0.0.1:{local_port}")
    print(f"  remote peers: {peers}")
    print(f"  ops         : {ops:,}")
    print(f"  concurrency : {concurrency}  (OS threads)")
    print(f"  value size  : {value_size} bytes")
    print(f"  replication : {REPLICATION}\n")

    value = b"x" * value_size

    # ── Writes ────────────────────────────────────────────────────────────────
    def write_worker(thread_id: int) -> tuple[list[float], int]:
        latencies: list[float] = []
        errors = 0
        for i in range(thread_id, ops, concurrency):
            t0 = time.perf_counter_ns()
            ok = node.set(f"k{i}", value)
            dt = time.perf_counter_ns() - t0
            if ok:
                latencies.append(dt)
            else:
                errors += 1
        return latencies, errors

    print(f"Running {ops:,} writes …")
    t0 = time.perf_counter()
    write_lats, write_err = sync_run_phase("", ops, concurrency, write_worker)
    write_elapsed = time.perf_counter() - t0

    # ── Reads ─────────────────────────────────────────────────────────────────
    def read_worker(thread_id: int) -> tuple[list[float], int]:
        latencies: list[float] = []
        errors = 0
        for i in range(thread_id, ops, concurrency):
            t0 = time.perf_counter_ns()
            val = node.get(f"k{i}")
            dt = time.perf_counter_ns() - t0
            if val is not None:
                latencies.append(dt)
            else:
                errors += 1
        return latencies, errors

    print(f"Running {ops:,} reads …")
    t0 = time.perf_counter()
    read_lats, read_err = sync_run_phase("", ops, concurrency, read_worker)
    read_elapsed = time.perf_counter() - t0

    node.stop()

    print_results("WRITES", write_lats, write_elapsed, errors=write_err)
    print_results("READS ", read_lats, read_elapsed, errors=read_err)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dlstorage remote sync benchmark")
    p.add_argument(
        "--peers",
        nargs="+",
        default=DEFAULT_PEERS,
        metavar="HOST:PORT",
        help="addresses of already-running remote nodes (default: 3 localhost nodes)",
    )
    p.add_argument(
        "--local-port",
        type=int,
        default=DEFAULT_LOCAL_PORT,
        help=f"port for the local benchmark node (default {DEFAULT_LOCAL_PORT})",
    )
    p.add_argument("--ops", type=int, default=100_000)
    p.add_argument(
        "--concurrency",
        type=int,
        default=64,
        help="OS threads (default 64)",
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
    main(args.peers, args.local_port, args.ops, args.concurrency, args.value_size)
