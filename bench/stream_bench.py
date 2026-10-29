"""Lot 1: a long server stream through each transport, server memory sampled along it.

    ../fastgrpc/.venv/bin/python bench/stream_bench.py [--messages 200000] [--slow]

The server (pinned to CPU 0) streams `--messages` medium messages for one `Small(id=N)`
request; the client (pinned to CPUs 2-3, raw `grpc.aio`, no decoding) reads them, sleeping
1 ms every 100 messages with `--slow` so that backpressure stays engaged. The server's RSS
is sampled at each tenth of the stream.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
FASTGRPC = HERE.parents[1] / "fastgrpc"
sys.path.insert(0, str(FASTGRPC / "benches"))

from contract import make_models  # noqa: E402
from e2e_bench import free_port, pinned, wait_for_port  # noqa: E402

from fastgrpc3 import Schema, SchemaRegistry  # noqa: E402

SERVERS = {
    "fastgrpc3": [sys.executable, str(HERE / "fg3_streams.py"), "{port}"],
    "tonic-B": [sys.executable, str(HERE / "python_server.py"), "queue", "{port}"],
}
CLIENT = """
import asyncio, sys, time
import grpc.aio
port, request, slow = int(sys.argv[1]), bytes.fromhex(sys.argv[2]), sys.argv[3] == "1"
async def main():
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        count, started = 0, time.perf_counter()
        async for _ in channel.unary_stream("/bench.v1.Streams/Down")(request):
            count += 1
            if slow and count % 100 == 0:
                await asyncio.sleep(0.001)
            if count % (int(sys.argv[4]) // 10) == 0:
                print(count, flush=True)
        print(f"done {count} {time.perf_counter() - started:.2f}", flush=True)
asyncio.run(main())
"""


def rss_mib(pid: int) -> float:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024
    raise RuntimeError("no VmRSS")


def run(name: str, messages: int, slow: bool) -> str:
    port = free_port()
    command = [part.replace("{port}", str(port)) for part in SERVERS[name]]
    server = subprocess.Popen(pinned("0", command), cwd=HERE.parent, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for_port(port)
        models = make_models()
        registry = SchemaRegistry(default_package="bench.v1")
        registry.add_model(models.small)
        request = Schema.from_registry(registry).codec(models.small).encode(models.small(id=messages))
        args = [sys.executable, "-c", CLIENT, str(port), request.hex(), "1" if slow else "0", str(messages)]
        client = subprocess.Popen(pinned("2-3", args), stdout=subprocess.PIPE, text=True)
        samples, summary = [], ""
        assert client.stdout is not None
        for line in client.stdout:
            if line.startswith("done"):
                summary = line.split()
            else:
                samples.append(rss_mib(server.pid))
        client.wait()
        count, seconds = int(summary[1]), float(summary[2])
        curve = " ".join(f"{s:.1f}" for s in samples)
        return (
            f"| {name} | {'slow' if slow else 'fast'} | {count:,} | {seconds:.2f} s | "
            f"{count / seconds:,.0f} msg/s | {curve} | {samples[-1] - samples[0]:+.1f} |"
        )
    finally:
        server.terminate()
        server.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--messages", type=int, default=200_000)
    parser.add_argument("--servers", nargs="+", default=list(SERVERS))
    args = parser.parse_args()
    print("| server | client | messages | time | rate | server RSS MiB at each tenth | growth |")
    print("|---|---|---:|---:|---:|---|---:|")
    for slow in (False, True):
        for name in args.servers:
            print(run(name, args.messages, slow), flush=True)


if __name__ == "__main__":
    main()
