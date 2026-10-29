"""The bench's services on the tonic transport, with Python handlers.

    python bench/python_server.py direct|queue PORT [TOKIO_THREADS]

`bench.v1.Bench/{Small,Medium,Large}` echo their request; each call does what fastgrpc3's
fast path does on `grpc.aio`: decode and validate the request (`MessageCodec`), await the
handler, encode the response. In `queue` mode, `bench.v1.Streams/Down` streams `id` medium
messages back for a `Small(id=...)` request, as `fastgrpc/benches/stream_bench.py` does.
"""

import asyncio
import signal
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

FASTGRPC = Path(__file__).resolve().parents[2] / "fastgrpc"
sys.path.insert(0, str(FASTGRPC / "benches"))

from bridge import Bridge, streaming  # noqa: E402
from contract import make_models, sample  # noqa: E402
from e2e_servers import fastgrpc3_app  # noqa: E402
from fastgrpc3_transport import Call, Server  # noqa: E402

from fastgrpc3 import Schema, SchemaRegistry  # noqa: E402

Handle = Callable[[bytes], Awaitable[bytes]]


def routes() -> dict[str, Handle]:
    """`/package.Service/Method` -> bytes in, bytes out, through the app's codecs."""
    app, _ = fastgrpc3_app(0)
    schema = Schema.from_registry(app.schema_registry)
    table: dict[str, Handle] = {}
    for service in app.services:
        for name, handler in service.handlers.items():
            request = schema.codec(handler.method.request)
            response = schema.codec(handler.method.response)
            table[f"/{service.full_name}/{name}"] = _handle(handler.function, request, response)
    return table


def _handle(function: Any, request: Any, response: Any) -> Handle:
    decode, encode = request.decode, response.encode

    async def handle(body: bytes) -> bytes:
        return encode(await function(decode(body)))

    return handle


async def direct(port: int, stop: asyncio.Event, threads: int) -> None:
    table = routes()

    async def handler(path: str, body: bytes) -> bytes:
        return await table[path](body)

    server = Server(threads=threads)
    for path in table:
        server.add_route(path, "unary")
    server.start_direct(handler, asyncio.get_running_loop(), port)
    await stop.wait()
    server.stop()


async def queue(port: int, stop: asyncio.Event, threads: int) -> None:
    table = routes()
    handlers: dict[str, Callable[[Call], Awaitable[None]]] = {}
    for path, handle in table.items():
        handlers[path] = _unary(handle)
    bridge = Bridge(Server(threads=threads), handlers)
    handlers["/bench.v1.Streams/Down"] = _down(bridge)
    bridge.start(port)
    await stop.wait()
    bridge.stop()


def _unary(handle: Handle) -> Callable[[Call], Awaitable[None]]:
    async def run(call: Call) -> None:
        call.reply(await handle(call.body))

    return run


def _down(bridge: Bridge) -> Callable[[Call], Awaitable[None]]:
    """`Small(id=n)` -> n medium messages, encoded one by one as fastgrpc3 does."""
    models = make_models()
    registry = SchemaRegistry(default_package="bench.v1")
    registry.add_model(models.small)
    registry.add_model(models.medium)
    schema = Schema.from_registry(registry)
    small, medium = schema.codec(models.small), schema.codec(models.medium)
    template = sample(models, "medium")

    @streaming
    async def down(call: Call) -> None:
        for _ in range(small.decode(call.body).id):  # type: ignore[attr-defined]
            if not await bridge.send(call, medium.encode(template)):
                return  # the client is gone
        call.finish()

    return down


def main() -> None:
    mode, port = sys.argv[1], int(sys.argv[2])
    threads = int(sys.argv[3]) if len(sys.argv) > 3 else 1

    async def serve() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        await {"direct": direct, "queue": queue}[mode](port, stop, threads)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
