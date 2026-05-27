import argparse
import socket

HOST = "0.0.0.0"
BUF_SIZE = 1024


def main():
    p = argparse.ArgumentParser(description="TCP 1KB echo server")
    p.add_argument("--host", default=HOST)
    p.add_argument("--port", type=int, default=9000)
    args = p.parse_args()

    buf = bytearray(BUF_SIZE)
    view = memoryview(buf)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((args.host, args.port))
        srv.listen(128)
        print(f"Listening on {args.host}:{args.port}")
        while True:
            conn, _ = srv.accept()
            with conn:
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                try:
                    while True:
                        recvd = 0
                        while recvd < BUF_SIZE:
                            n = conn.recv_into(view[recvd:])
                            if n == 0:
                                raise ConnectionResetError
                            recvd += n
                        conn.sendall(buf)
                except (ConnectionResetError, BrokenPipeError, OSError):
                    pass


if __name__ == "__main__":
    main()
