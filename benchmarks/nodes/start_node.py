"""
Start a single StorageNode and keep it running until Ctrl-C.

Usage:
    # Sync node (default)
    .venv/bin/python benchmarks/nodes/start_node.py --port 7201 --peers 127.0.0.1:7202 127.0.0.1:7203

    # Async node
    .venv/bin/python benchmarks/nodes/start_node.py --async --port 7201 --peers 127.0.0.1:7202 127.0.0.1:7203

    # Advertise a different host (e.g. inside Docker / k8s)
    .venv/bin/python benchmarks/nodes/start_node.py --port 7201 --advertise-host 10.0.0.1 --peers 10.0.0.2:7201 10.0.0.3:7201
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dlstorage import AsyncStorageNode, StaticDiscovery, StorageNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("start_node")


# ── Sync ──────────────────────────────────────────────────────────────────────


def run_sync(
    host: str,
    port: int,
    advertise_host: str | None,
    peers: list[str],
    replication: int,
) -> None:
    node = StorageNode(
        StaticDiscovery(peers),
        host=host,
        port=port,
        advertise_host=advertise_host,
        replication=replication,
    )
    node.start()
    logger.info("Sync node listening on %s:%d  peers=%s", host, port, peers)
    logger.info("Press Ctrl-C to stop.")

    try:
        signal.pause()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Stopping node …")
        node.stop()


# ── Async ─────────────────────────────────────────────────────────────────────


async def run_async(
    host: str,
    port: int,
    advertise_host: str | None,
    peers: list[str],
    replication: int,
) -> None:
    node = AsyncStorageNode(
        StaticDiscovery(peers),
        host=host,
        port=port,
        advertise_host=advertise_host,
        replication=replication,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async with node:
        logger.info("Async node listening on %s:%d  peers=%s", host, port, peers)
        logger.info("Press Ctrl-C to stop.")
        await stop_event.wait()

    logger.info("Node stopped.")


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Start a single dlstorage node")
    p.add_argument(
        "--host",
        default="0.0.0.0",
        help="bind address (default 0.0.0.0)",
    )
    p.add_argument(
        "--port",
        type=int,
        default=7201,
        help="TCP port (default 7201)",
    )
    p.add_argument(
        "--advertise-host",
        default=None,
        metavar="HOST",
        help="host to advertise to peers (useful behind NAT / Docker)",
    )
    p.add_argument(
        "--peers",
        nargs="*",
        default=[],
        metavar="HOST:PORT",
        help="addresses of other nodes in the cluster",
    )
    p.add_argument(
        "--replication",
        type=int,
        default=2,
        help="replication factor (default 2)",
    )
    p.add_argument(
        "--async",
        dest="use_async",
        action="store_true",
        help="use AsyncStorageNode (asyncio) instead of the sync node",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.use_async:
        try:
            import uvloop  # type: ignore[import-untyped]

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass
        asyncio.run(
            run_async(
                args.host,
                args.port,
                args.advertise_host,
                args.peers,
                args.replication,
            )
        )
    else:
        run_sync(
            args.host,
            args.port,
            args.advertise_host,
            args.peers,
            args.replication,
        )
