"""PGO training: a fastgrpc3 app on the (instrumented) tonic transport, loaded by a client.

    python scripts/pgo-train.py

The server runs in a child process and is stopped with SIGTERM, so that it exits normally
and writes its profile. The load covers what servers do: unary calls of three sizes at
concurrency 1, 16 and 64, the three kinds of stream, errors with details, and calls cut by
their deadline.
"""

import asyncio
import signal
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator

from pydantic import BaseModel

from fastgrpc3 import (
    ErrorInfo,
    GrpcApp,
    GrpcClient,
    GrpcError,
    GrpcService,
    Insecure,
    StatusCode,
)
from fastgrpc3.api.client import Channel


class Blob(BaseModel):
    data: bytes = b""
    n: int = 0


class TrainClient(GrpcClient):
    package = "pgo.v1"
    service = "Train"
    echo = GrpcClient.unary("Echo", Blob, Blob)
    slow = GrpcClient.unary("Slow", Blob, Blob)
    fail = GrpcClient.unary("Fail", Blob, Blob)
    many = GrpcClient.unary_stream("Many", Blob, Blob)
    sum = GrpcClient.stream_unary("Sum", Blob, Blob)
    echoes = GrpcClient.stream_stream("Echoes", Blob, Blob)


def serve(port: int) -> None:
    service = GrpcService("Train", package="pgo.v1")

    @service.unary()
    async def Echo(req: Blob) -> Blob:
        return req

    @service.unary()
    async def Slow(req: Blob) -> Blob:
        await asyncio.sleep(1)
        return req

    @service.unary()
    async def Fail(req: Blob) -> Blob:
        raise GrpcError(StatusCode.NOT_FOUND, "no such blob", [ErrorInfo(reason="MISSING")])

    @service.unary_stream()
    async def Many(req: Blob) -> AsyncIterator[Blob]:
        for i in range(req.n):
            yield Blob(n=i)

    @service.stream_unary()
    async def Sum(requests: AsyncIterator[Blob]) -> Blob:
        return Blob(n=sum([len(r.data) async for r in requests]))

    @service.stream_stream()
    async def Echoes(requests: AsyncIterator[Blob]) -> AsyncIterator[Blob]:
        async for request in requests:
            yield request

    app = GrpcApp(tls=Insecure(), address=f"127.0.0.1:{port}", transport="tonic")
    app.add_service(service)
    asyncio.run(app.run(grace=1.0))


async def load(port: int) -> None:
    async with Channel(f"127.0.0.1:{port}", tls=Insecure(), default_timeout=30) as channel:
        client = TrainClient(channel)

        async def burst(data: bytes, concurrency: int, seconds: float) -> None:
            end = time.monotonic() + seconds

            async def worker() -> None:
                while time.monotonic() < end:
                    await client.echo(Blob(data=data))

            await asyncio.gather(*(worker() for _ in range(concurrency)))

        for size in (9, 300, 20_000):
            for concurrency in (1, 16, 64):
                await burst(b"x" * size, concurrency, 1.5)

        assert len([m async for m in client.many(Blob(n=20_000))]) == 20_000
        assert (await client.sum([Blob(data=b"x" * 100)] * 5_000)).n == 500_000

        async def requests() -> AsyncIterator[Blob]:
            for i in range(5_000):
                yield Blob(n=i)

        assert len([m async for m in client.echoes(requests())]) == 5_000

        async def refused(call: object) -> None:
            try:
                await call  # type: ignore[misc]
            except GrpcError:
                return
            raise AssertionError("the call should have failed")

        await asyncio.gather(*(refused(client.fail(Blob())) for _ in range(500)))
        await asyncio.gather(*(refused(client.slow(Blob(), timeout=0.05)) for _ in range(200)))


def main() -> None:
    if len(sys.argv) == 3 and sys.argv[1] == "--serve":
        serve(int(sys.argv[2]))
        return
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = subprocess.Popen([sys.executable, __file__, "--serve", str(port)])
    try:
        deadline = time.monotonic() + 30
        while socket.socket().connect_ex(("127.0.0.1", port)) != 0:
            if time.monotonic() > deadline or server.poll() is not None:
                raise RuntimeError("the training server did not start")
            time.sleep(0.1)
        started = time.monotonic()
        asyncio.run(load(port))
        print(f"PGO training load: {time.monotonic() - started:.1f} s")
    finally:
        server.send_signal(signal.SIGTERM)
    if server.wait(timeout=30) != 0:
        raise RuntimeError("the training server did not exit cleanly: no profile")


if __name__ == "__main__":
    main()
