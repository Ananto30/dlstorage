# DLStorage

Distributed Local Storage for Python. Can replace redis, memcached while appropriate. Uses a gossip protocol for discovery and consistent hashing for replication, eventually consistent merge policies for conflict resolution. Designed to be simple, fast and easy to use.

## Requirements

* Python 3.13 or higher
* [uv](https://docs.astral.sh/uv/)

## Installation

```bash
# build and install the package
uv build
uv pip install dist/dlstorage-*.whl
```

## Usage

```python
import asyncio
from dlstorage import AsyncStorageNode, StaticDiscovery

async def main():
    node = AsyncStorageNode(
        # there are other discovery options possible like DNSDiscovery, GossipDiscovery
        discovery=StaticDiscovery(["127.0.0.1:7001", "127.0.0.1:7002"])
        port=7001
    )
    await node.start()  # start the node and join the cluster, this function is non-blocking
 
    await node.set("key1", "value1")  # set a key-value pair
    value = await node.get("key1")  # get the value for a key
    print(value)  # should print "value1"

if __name__ == "__main__":
    asyncio.run(main())
```

Run another node in a separate terminal:

```python
import asyncio
from dlstorage import AsyncStorageNode, StaticDiscovery

async def main():
    node = AsyncStorageNode(
        discovery=StaticDiscovery(["127.0.0.1:7001", "127.0.0.1:7002"]),
        port=7002
    )
    await node.start()

    value = await node.get("key1")  # get the value for a key set by the first node
    print(value)  # should print "value1"

if __name__ == "__main__":
    asyncio.run(main())
```

## Pluggable

DLStorage is designed to be extensible and pluggable. You can implement your own discovery mechanisms, merge policies, ring implementations etc.

Default configurations are provided for ease of use, but you can swap out components as needed. For example, for merge policies default is LWW([Last Write Wins](/dlstorage/consistency/lww.py)), but you can swap with FHW([First Hit Wins](/dlstorage/consistency/fhw.py)) or implement your own. There are always trade-offs to consider when choosing a merge policy, so you can pick the one that best fits your use case.

## Can be embedded to any server

DLStorage can be used as a library within your Python applications, or you can run it as a standalone server that other applications can connect to over the network.
We prefer the library approach to keep things simple, so you don't have to manage different deployments.

Let's say you can use along with FastAPI to provide an HTTP API for your storage cluster:

```python
from fastapi import FastAPI
from dlstorage import AsyncStorageNode, DNSDiscovery
from contextlib import asynccontextmanager

_node: AsyncStorageNode | None = None

async def _product_bootstrap():
    # This is just an example of how you can bootstrap some data into the cluster
    # In real life you might want to load from a database or some other source
    await _node.set("product:123", {"name": "Widget", "price": 9.99})

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _node
    _node = AsyncStorageNode(
        discovery=DNSDiscovery("_storage._tcp.my-svc.default.svc.cluster.local"), # can be found via DNS SRV records in Kubernetes
    )
    await _node.start()
    await _product_bootstrap()
    yield
    await _node.stop()
    _node = None

app = FastAPI(title="My E-commerce", lifespan=lifespan)

@app.get("/products/{product_id}")
async def get_product(product_id: str):
    # retrieve product information from the storage cluster
    product_info = await _node.get(f"product:{product_id}")
    if product_info is None:
        return {"error": "Product not found"}
    return {"product_id": product_id, "info": product_info}
```

## Kubernetes Discovery

DLStorage can be easily deployed in Kubernetes and use DNS-based discovery to find other nodes in the cluster. You don't need to worry about service discovery or load balancing, as the nodes will automatically find each other using DNS SRV records.

Example is already provided in the previous section.

<img width="1346" height="1234" alt="image" src="https://github.com/user-attachments/assets/ca93d730-5a80-4e2d-9401-7c1faf3d08b0" />
