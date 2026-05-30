"""
Quick-start demo: 3-node cluster using StaticDiscovery.

Run with:  python examples/quickstart.py
"""

import asyncio
import logging

from dlstorage import AsyncStorageNode, StaticDiscovery

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


async def main() -> None:
    addrs = ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]

    nodes = [
        AsyncStorageNode(
            StaticDiscovery([a for a in addrs if a != addr]),
            "127.0.0.1",
            int(addr.split(":")[1]),
            replication=2,
        )
        for addr in addrs
    ]

    for node in nodes:
        await node.start()

    # Let PEER_ANNOUNCE messages propagate
    await asyncio.sleep(0.1)

    primary = nodes[0]
    print("\n=== Cluster ready ===")
    print(primary)

    # ------------------------------------------------------------------ #
    # Basic set / get / delete                                             #
    # ------------------------------------------------------------------ #
    await primary.set("greeting", "hello world")
    print("get greeting  :", await primary.get("greeting"))

    await primary.set("counter", 42, ttl=10.0)
    print("get counter   :", await primary.get("counter"))

    # Any Python object works
    await primary.set("data", {"user": "alice", "scores": [10, 20, 30]})
    print("get data      :", await primary.get("data"))

    # Read the same key from a different node (routed automatically)
    print("get data node2:", await nodes[1].get("data"))
    print("get data node3:", await nodes[2].get("data"))

    await primary.delete("greeting")
    print("after delete  :", await primary.get("greeting"))  # None

    print("\n=== Stopping cluster ===")
    for node in nodes:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
