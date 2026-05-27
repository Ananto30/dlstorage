"""
FastAPI integration example.

AsyncStorageNode uses asyncio exclusively, so it shares uvicorn's event loop
with no extra threading or process management.  The lifespan context manager
starts and stops the node around the server's lifetime.

Install extras:
    uv add fastapi uvicorn

Run a 3-node cluster (three terminals):
    DLSTORAGE_PORT=7001 uvicorn examples.fastapi_server:app --port 8001 --log-level debug
    DLSTORAGE_PORT=7002 uvicorn examples.fastapi_server:app --port 8002 --log-level debug
    DLSTORAGE_PORT=7003 uvicorn examples.fastapi_server:app --port 8003 --log-level debug

Then hit any node:
    curl -X PUT "http://localhost:8001/keys/hello?value=world"
    curl "http://localhost:8002/keys/hello"
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

# basicConfig gives the root logger a StreamHandler so that dlstorage records
# (which propagate up the hierarchy) are not silently dropped.  uvicorn only
# attaches handlers to its own loggers, leaving root with none by default.
# basicConfig is a no-op if root already has handlers, so it won't conflict.
logging.basicConfig(format="%(levelname)s:%(name)s:%(message)s")
logging.getLogger("dlstorage").setLevel(
    os.getenv("DLSTORAGE_LOG_LEVEL", "DEBUG").upper()
)

from dlstorage import AsyncStorageNode, GossipDiscovery

DLS_HOST = os.getenv("DLSTORAGE_HOST", "127.0.0.1")
DLS_PORT = int(os.getenv("DLSTORAGE_PORT", "7001"))
SEED_ADDR = os.getenv("DLSTORAGE_SEED", "127.0.0.1:7001")

_node: AsyncStorageNode | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _node
    _node = AsyncStorageNode(
        GossipDiscovery(SEED_ADDR),
        DLS_HOST,
        DLS_PORT,
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
