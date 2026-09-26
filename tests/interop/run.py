#!/usr/bin/env python3
"""Cross-process interop: never link both RpcHeader implementations together."""
import socket
import subprocess
import sys
import tempfile
import time


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def ready(number, process):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("server exited before listen")
        try:
            with socket.create_connection(("127.0.0.1", number), 0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError("server did not listen")


def pair(server_binary, client_binary):
    number = port()
    with tempfile.TemporaryFile(mode="w+t") as output:
        server = subprocess.Popen(
            [server_binary, "server", str(number)],
            stdout=output, stderr=subprocess.STDOUT,
        )
        try:
            ready(number, server)
            for _ in range(3):
                client = subprocess.run(
                    [client_binary, "client", str(number)],
                    capture_output=True, text=True, timeout=10,
                )
                if client.returncode:
                    output.seek(0)
                    raise RuntimeError(
                        f"client failed: {client.stdout} {client.stderr}\n"
                        f"server: {output.read()}"
                    )
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: run.py NEW_BINARY LEGACY_BINARY")
    new, legacy = sys.argv[1:]
    pair(legacy, new)
    print("new client -> legacy server: 3 calls passed")
    pair(new, legacy)
    print("legacy client -> new server: 3 calls passed")


if __name__ == "__main__":
    main()
