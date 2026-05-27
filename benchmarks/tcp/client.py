import socket
import time
import argparse

PAYLOAD_SIZE = 1024  # 1 KB


def run_once(host, port, duration):
    payload = b"a" * PAYLOAD_SIZE
    mv_payload = memoryview(payload)  # zero-copy slicing for partial sends
    recv_buf = bytearray(PAYLOAD_SIZE)  # allocated once, reused every iteration
    recv_view = memoryview(recv_buf)
    ops = 0
    end_time = time.perf_counter() + duration
    with socket.create_connection((host, port)) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while time.perf_counter() < end_time:
            s.sendall(mv_payload)
            recvd = 0
            while recvd < PAYLOAD_SIZE:
                n = s.recv_into(recv_view[recvd:])
                if n == 0:
                    raise RuntimeError("socket connection broken")
                recvd += n
            ops += 1
    return ops


def main():
    p = argparse.ArgumentParser(description="TCP 1KB round-trip benchmark (ops/s)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--duration", type=float, default=5.0, help="seconds to run")
    p.add_argument(
        "--warmup", type=float, default=1.0, help="warmup seconds (not counted)"
    )
    args = p.parse_args()

    try:
        run_once(args.host, args.port, args.warmup)
    except Exception as e:
        print("Warmup failed:", e)
        return

    start = time.perf_counter()
    ops = run_once(args.host, args.port, args.duration)
    elapsed = time.perf_counter() - start
    print(f"Duration measured: {elapsed:.6f} s")
    print(f"Total ops:         {ops}")
    print(f"Ops/sec:           {ops / elapsed:.2f}")


if __name__ == "__main__":
    main()
