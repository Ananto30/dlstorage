"""
Flask integration example.

StorageNode is synchronous (blocking sockets, background threads), so it fits
Flask naturally — route handlers call get/set/delete directly with no asyncio
bridge or extra threads required.

Install extras:
    uv add flask

Run a 3-node cluster (three terminals):
    DLSTORAGE_PORT=7001 DLSTORAGE_PEERS=127.0.0.1:7002,127.0.0.1:7003 python examples/flask_server.py
    DLSTORAGE_PORT=7002 DLSTORAGE_PEERS=127.0.0.1:7001,127.0.0.1:7003 FLASK_PORT=8002 python examples/flask_server.py
    DLSTORAGE_PORT=7003 DLSTORAGE_PEERS=127.0.0.1:7001,127.0.0.1:7002 FLASK_PORT=8003 python examples/flask_server.py

Note: gunicorn prefork workers (default) fork after module load, so the node's
internal threads are not cloned into workers.  Use --worker-class gthread or
--workers 1 so the node started at import time stays alive in the same process.
"""

from __future__ import annotations

import atexit
import os

from flask import Flask, abort, jsonify, request

from dlstorage import StaticDiscovery
from dlstorage.node.sync import StorageNode

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HOST = os.getenv("DLSTORAGE_HOST", "127.0.0.1")
PORT = int(os.getenv("DLSTORAGE_PORT", "7001"))
PEERS = [
    p
    for p in os.getenv("DLSTORAGE_PEERS", "127.0.0.1:7002,127.0.0.1:7003").split(",")
    if p
]
REPLICATION = int(os.getenv("DLSTORAGE_REPLICATION", "1"))

# ---------------------------------------------------------------------------
# Node — started once at import time, stopped on process exit
# ---------------------------------------------------------------------------

_node = StorageNode(StaticDiscovery(PEERS), HOST, PORT, replication=REPLICATION)
_node.start()


@atexit.register
def _stop_node() -> None:
    _node.stop()


# ---------------------------------------------------------------------------
# App and routes
# ---------------------------------------------------------------------------

app = Flask(__name__)


@app.get("/keys/<key>")
def get_key(key: str):
    value = _node.get(key)
    if value is None:
        abort(404, f"key {key!r} not found")
    return jsonify(value)


@app.put("/keys/<key>")
def set_key(key: str):
    value = request.get_json(force=True)
    ok = _node.set(key, value)
    if not ok:
        abort(500, "write failed on all replicas")
    return "", 204


@app.delete("/keys/<key>")
def delete_key(key: str):
    ok = _node.delete(key)
    if not ok:
        abort(404, f"key {key!r} not found")
    return "", 204


@app.get("/health")
def health():
    return jsonify(
        {
            "address": _node.info.address,
            "peers": len(_node._ring) - 1,
            "keys": len(_node._store),
        }
    )


if __name__ == "__main__":
    app.run(port=int(os.getenv("FLASK_PORT", "8001")))
