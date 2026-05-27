"""
Profiler for the sync dlstorage benchmark (SyncStorageNode / MuxConnectionPool).

Two passes:
  1. cProfile pass  – single-threaded hot loop under cProfile; top-20 by tottime
  2. Step-timing pass – per-layer median latency breakdown

    Layer                     example budget
    ──────────────────────────────────────────
    LocalStore.set/get        baseline (no network)
    RendezvousRing lookup     ring overhead
    pickle.dumps              encode cost
    pickle.loads              decode cost
    raw TCP roundtrip         warm socket, loopback kernel cost
    full node.set / .get      all-in cost  (MuxConnectionPool + selector thread)

Usage:
    .venv/bin/python profiler_sync.py [--ops 10000] [--save profile.prof] [--no-cprofile]
"""

from __future__ import annotations

import argparse
import cProfile
import io
import os
import pickle
import pstats
import socket
import threading
import time

from dlstorage import StaticDiscovery, StorageNode
from dlstorage.connection_pool.mux import MuxConnectionPool
from dlstorage.ring import RendezvousRing
from dlstorage.store import LocalStore
from dlstorage.types import Message, MessageType, NodeInfo

ADDRS = ["127.0.0.1:7401", "127.0.0.1:7402", "127.0.0.1:7403"]
VALUE = b"x" * 64
ECHO_PORT = 7499


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def fmt_ns(ns: float) -> str:
    if ns < 1_000:
        return f"{ns:.1f} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.2f} µs"
    return f"{ns / 1_000_000:.3f} ms"


def section(title: str) -> None:
    print(f"\n{'━' * 60}")
    print(f"  {title}")
    print(f"{'━' * 60}")


def _timeit(fn, reps: int) -> float:
    """Return median nanoseconds per call."""
    times: list[int] = []
    for _ in range(reps):
        t0 = time.perf_counter_ns()
        fn()
        times.append(time.perf_counter_ns() - t0)
    times.sort()
    return float(times[len(times) // 2])


# --------------------------------------------------------------------------- #
# Node factory                                                                 #
# --------------------------------------------------------------------------- #


def _make_nodes() -> list[StorageNode]:
    return [
        StorageNode(
            StaticDiscovery(ADDRS),
            host="127.0.0.1",
            port=int(addr.split(":")[1]),
        )
        for addr in ADDRS
    ]


# --------------------------------------------------------------------------- #
# 1. cProfile pass                                                             #
# --------------------------------------------------------------------------- #


def _bench_loop(node: StorageNode, ops: int) -> None:
    """Single-threaded set+get loop profiled by cProfile."""
    for i in range(ops):
        node.set(f"k{i}", VALUE)
        node.get(f"k{i}")


def run_cprofile(ops: int, save: str | None) -> None:
    nodes = _make_nodes()
    for n in nodes:
        n.start()
    time.sleep(0.1)

    pr = cProfile.Profile()
    pr.enable()
    _bench_loop(nodes[0], ops)
    pr.disable()

    for n in nodes:
        n.stop()

    stream = io.StringIO()
    # Sort by tottime (time IN the function itself) instead of cumtime.
    # cumtime is inflated for multithreaded code: MuxPool's background selector
    # thread bleeds into the main thread's cumulative accounting in cProfile.
    ps = pstats.Stats(pr, stream=stream).sort_stats("tottime")
    ps.print_stats(20)

    section(f"cProfile  top-20 by tottime  ({ops:,} set+get pairs, single thread)")
    home = os.path.expanduser("~")
    for line in stream.getvalue().splitlines():
        print(line.replace(home, "~"))

    if save:
        pr.dump_stats(save)
        print(f"\n  Profile saved to: {save}")
        print(f"  Visualise with:  snakeviz {save}")


# --------------------------------------------------------------------------- #
# 2. Step-timing pass                                                          #
# --------------------------------------------------------------------------- #


def _start_echo_server() -> threading.Event:
    """Echo server that loops per connection, supporting warm reuse."""
    stop = threading.Event()

    def _handle(c: socket.socket) -> None:
        with c:
            while True:
                data = c.recv(65536)
                if not data:
                    return
                c.sendall(data)

    def serve() -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", ECHO_PORT))
        srv.listen(256)
        srv.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            threading.Thread(target=_handle, args=(conn,), daemon=True).start()
        srv.close()

    threading.Thread(target=serve, daemon=True).start()
    time.sleep(0.05)
    return stop


def run_step_timing(ops: int) -> None:
    section("Step-timing breakdown  (median per operation)")

    reps = min(ops, 5000)

    # ── 1. LocalStore ────────────────────────────────────────────────────── #
    store = LocalStore()
    store_set_ns = _timeit(lambda: store.set("bench", VALUE), reps)
    store_get_ns = _timeit(lambda: store.get("bench"), reps)

    # ── 2. RendezvousRing lookup ─────────────────────────────────────────── #
    ring = RendezvousRing()
    for a in ADDRS:
        ring.add(NodeInfo.from_address(a))
    ring_ns = _timeit(lambda: ring.get_node("bench-key"), reps)

    # ── 3. Pickle encode / decode ─────────────────────────────────────────── #
    msg = Message(MessageType.SET, {"key": "bench", "value": VALUE})
    enc_ns = _timeit(lambda: pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL), reps)
    enc_data = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    dec_ns = _timeit(lambda: pickle.loads(enc_data), reps)  # noqa: S301

    # ── 4. Raw loopback TCP roundtrip (warm reused socket, like pool does) ─── #
    echo_stop = _start_echo_server()
    _rtt_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _rtt_sock.connect(("127.0.0.1", ECHO_PORT))
    for _ in range(10):  # warmup: let the connection settle
        _rtt_sock.sendall(enc_data)
        _rtt_sock.recv(65536)

    def _warm_rtt() -> None:
        _rtt_sock.sendall(enc_data)
        _rtt_sock.recv(65536)

    raw_rtt_ns = _timeit(_warm_rtt, min(reps, 500))
    _rtt_sock.close()
    echo_stop.set()

    # ── 5. Full node.set / node.get (warm MuxConnectionPool) ─────────────── #
    nodes = _make_nodes()
    for n in nodes:
        n.start()
    time.sleep(0.1)

    # Pre-warm pool so timed calls don't include TCP handshake overhead
    ping = Message(MessageType.PING, {})
    for node in nodes:
        for peer in node._ring.nodes():
            if peer == node.info:
                continue
            for _ in range(4):
                node._pool.execute(peer.host, peer.port, ping)

    n_timed = min(reps, 500)
    key_idx = [0]

    def full_set() -> None:
        k = f"k{key_idx[0]}"
        key_idx[0] += 1
        nodes[0].set(k, VALUE)

    # Pre-build the key list so full_get cycles through all set keys,
    # giving a representative mix of local and remote routing.
    _get_keys: list[str] = []
    _get_i = [0]

    def full_get() -> None:
        k = _get_keys[_get_i[0] % len(_get_keys)]
        _get_i[0] += 1
        nodes[0].get(k)

    full_set_ns = _timeit(full_set, n_timed)
    _get_keys[:] = [f"k{i}" for i in range(key_idx[0])]
    full_get_ns = _timeit(full_get, n_timed)

    for n in nodes:
        n.stop()

    # ── Print table ──────────────────────────────────────────────────────── #
    rows: list[tuple[str, float, str]] = [
        ("LocalStore.set", store_set_ns, "pure dict write + lock"),
        ("LocalStore.get", store_get_ns, "pure dict read + lock"),
        ("RendezvousRing lookup", ring_ns, "sha256 × n_nodes"),
        ("pickle.dumps (msg)", enc_ns, "encode request"),
        ("pickle.loads (msg)", dec_ns, "decode response"),
        ("raw TCP roundtrip", raw_rtt_ns, "warm socket, loopback kernel cost"),
        ("node.set  (full)", full_set_ns, "ring + MuxPool + pickle + TCP"),
        ("node.get  (full)", full_get_ns, "ring + MuxPool + pickle + TCP"),
    ]

    baseline = full_set_ns or 1.0
    print(f"\n  {'Operation':<26}  {'Median':>10}  {'% of node.set':>14}  Notes")
    print(f"  {'─' * 26}  {'─' * 10}  {'─' * 14}  {'─' * 30}")
    for name, ns, note in rows:
        print(f"  {name:<26}  {fmt_ns(ns):>10}  {ns / baseline * 100:>13.1f}%  {note}")

    print()
    protocol_ns = raw_rtt_ns + enc_ns + dec_ns
    mux_overhead = max(0.0, full_set_ns - protocol_ns - ring_ns)
    print(f"  Protocol overhead (encode+decode+RTT) : {fmt_ns(protocol_ns)}")
    print(f"  node.set total                        : {fmt_ns(full_set_ns)}")
    print(f"  MuxPool / selector thread overhead    : {fmt_ns(mux_overhead)}")


# --------------------------------------------------------------------------- #
# Entry point                                                                  #
# --------------------------------------------------------------------------- #


def main(ops: int, save: str | None, skip_cprofile: bool) -> None:
    if not skip_cprofile:
        run_cprofile(ops, save)
    run_step_timing(ops)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dlstorage sync benchmark profiler")
    p.add_argument(
        "--ops",
        type=int,
        default=10_000,
        help="ops for cProfile pass (default 10 000)",
    )
    p.add_argument(
        "--save",
        type=str,
        default=None,
        help="save .prof file for snakeviz (e.g. --save out.prof)",
    )
    p.add_argument(
        "--no-cprofile",
        action="store_true",
        help="skip cProfile pass, only show step-timing breakdown",
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
    main(args.ops, args.save, args.no_cprofile)
