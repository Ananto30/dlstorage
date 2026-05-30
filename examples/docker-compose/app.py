"""
FastAPI + Docker Compose example using ARecordDiscovery.

Docker Compose resolves service names via A records.  When a Compose service
has multiple replicas (``deploy.replicas``), its DNS name returns one A record
per container, so ARecordDiscovery can discover all peers automatically from a
single shared hostname – no static peer lists required.

Usage:
    docker compose up --build --scale storage=3

Then hit the API gateway (or any storage container directly):
    curl -X PUT "http://localhost:8000/keys/hello?value=world"
    curl "http://localhost:8000/keys/hello"
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s")
logging.getLogger("dlstorage").setLevel(
    os.getenv("DLSTORAGE_LOG_LEVEL", "INFO").upper()
)


from dlstorage import ARecordDiscovery, AsyncStorageNode

# Host/port this node binds for peer-to-peer TCP connections.
DLS_PORT = int(os.getenv("DLSTORAGE_PORT", "7001"))

# DNS name that resolves to all storage peers (all replicas of the service).
# In Docker Compose this is just the service name, e.g. "storage".
DLS_DISCOVERY_HOST = os.getenv("DLSTORAGE_DISCOVERY_HOST", "storage")

_node: AsyncStorageNode | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _node
    _node = AsyncStorageNode(
        ARecordDiscovery(DLS_DISCOVERY_HOST, DLS_PORT),
        port=DLS_PORT,
    )
    await _node.start()
    yield
    await _node.stop()
    _node = None


app = FastAPI(title="dlstorage node", lifespan=lifespan)


def _node_or_503() -> AsyncStorageNode:
    if _node is None:
        raise HTTPException(status_code=503, detail="node not ready")
    return _node


@app.get("/keys/{key}")
async def get_key(key: str) -> Any:
    value = await _node_or_503().get(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"key {key!r} not found")
    return value


@app.put("/keys/{key}", status_code=204)
async def set_key(key: str, value: Any) -> None:
    ok = await _node_or_503().set(key, value)
    if not ok:
        raise HTTPException(status_code=500, detail="write failed on all replicas")


@app.delete("/keys/{key}", status_code=204)
async def delete_key(key: str) -> None:
    ok = await _node_or_503().delete(key)
    if not ok:
        raise HTTPException(status_code=404, detail=f"key {key!r} not found")


@app.get("/health")
async def health() -> dict:
    node = _node_or_503()
    return {
        "address": node.info.address,
        "peers": len(node._ring) - 1,
        "keys": len(node._store),
    }
