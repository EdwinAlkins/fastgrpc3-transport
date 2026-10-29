"""The bench's echo service on the tonic transport, with Python handlers.

    python bench/python_server.py direct|queue PORT [TOKIO_THREADS]

Each call does what fastgrpc3's fast path does on `grpc.aio`: decode and validate the
request (`MessageCodec`), await the handler, encode the response.
"""

import asyncio
import signal
import socket
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

FASTGRPC = Path(__file__).resolve().parents[2] / "fastgrpc"
sys.path.insert(0, str(FASTGRPC / "benches"))

from e2e_servers import fastgrpc3_app  # noqa: E402
from fastgrpc3_transport import Call, Server  # noqa: E402

from fastgrpc3 import Schema  # noqa: E402

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
    server.start_direct(handler, asyncio.get_running_loop(), port)
    await stop.wait()
    server.stop()


async def queue(port: int, stop: asyncio.Event, threads: int) -> None:
    table = routes()
    loop = asyncio.get_running_loop()
    server = Server(threads=threads)
    _, fd = server.start_queue(port)
    wake = socket.socket(fileno=fd)

    async def run(call: Call) -> None:
        try:
            call.reply(await table[call.path](call.body))
        except Exception as e:  # noqa: BLE001 - the spike answers INTERNAL
            call.fail(13, str(e))

    def pump() -> None:
        try:
            while wake.recv(4096):
                pass
        except BlockingIOError:
            pass
        for call in server.take():
            loop.create_task(run(call))

    loop.add_reader(fd, pump)
    await stop.wait()
    loop.remove_reader(fd)
    server.stop()


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
