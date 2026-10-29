"""The asyncio side of variant B: drain `Server.take()` and run one task per call.

    bridge = Bridge(server, handlers)       # handlers: path -> async def handle(call)
    port = bridge.start(port)
    ...
    await bridge.send(call, data)            # in a server stream: waits while it is full
    bridge.stop()

A unary handler answers with `call.reply(bytes)`; a server stream sends with
`await bridge.send(call, bytes)` (False: the client is gone, stop) and ends with
`call.finish()`. An exception fails the call with `INTERNAL`.
"""

import asyncio
import socket
from collections.abc import Awaitable, Callable

from fastgrpc3_transport import CLOSED, SENT, Call, Server

Handler = Callable[[Call], Awaitable[None]]


class Bridge:
    def __init__(self, server: Server, handlers: dict[str, Handler]) -> None:
        self.server = server
        self.handlers = handlers
        self.tasks: set[asyncio.Task[None]] = set()
        self._waiting: dict[int, asyncio.Future[None]] = {}
        self._wake: socket.socket | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        #: How many calls each `take()` returned (a batch per wake-up).
        self.batches: list[int] = []

    def start(self, port: int = 0) -> int:
        for path, handler in self.handlers.items():
            kind = "server_stream" if getattr(handler, "streaming", False) else "unary"
            self.server.add_route(path, kind)
        port, fd = self.server.start_queue(port)
        self._wake = socket.socket(fileno=fd)
        self._loop = asyncio.get_running_loop()
        self._loop.add_reader(fd, self._pump)
        return port

    def stop(self) -> None:
        assert self._loop is not None and self._wake is not None
        self._loop.remove_reader(self._wake.fileno())
        self.server.stop()
        self._wake.close()

    async def send(self, call: Call, data: bytes) -> bool:
        """One message of a server stream; waits while its channel is full. False when the
        client is gone."""
        while True:
            state = call.try_send(data)
            if state == SENT:
                return True
            if state == CLOSED:
                return False
            assert self._loop is not None
            waiter = self._loop.create_future()
            self._waiting[call.id] = waiter
            call.when_writable()
            await waiter

    def _pump(self) -> None:
        assert self._wake is not None and self._loop is not None
        try:
            while self._wake.recv(4096):
                pass
        except BlockingIOError:
            pass
        calls, writable = self.server.take()
        self.batches.append(len(calls))
        for call_id in writable:
            waiter = self._waiting.pop(call_id, None)
            if waiter is not None and not waiter.done():
                waiter.set_result(None)
        for call in calls:
            task = self._loop.create_task(self._run(call))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    async def _run(self, call: Call) -> None:
        try:
            await self.handlers[call.path](call)
        except Exception as e:  # noqa: BLE001 - the prototype answers INTERNAL
            call.fail(13, f"{type(e).__name__}: {e}")


def streaming(handler: Handler) -> Handler:
    """Mark a handler as a server stream (its route is declared `server_stream`)."""
    handler.streaming = True  # type: ignore[attr-defined]
    return handler
