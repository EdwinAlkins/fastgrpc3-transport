"""Variant B through a real gRPC client: answers, failures, and batches without lost
wake-ups."""

import asyncio
import socket
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import grpc
import grpc.aio
from fastgrpc3_transport import Call, Server

@asynccontextmanager
async def serving(answer: Callable[[Call], object]) -> AsyncIterator[tuple[grpc.aio.Channel, list[int]]]:
    """A queue server whose calls are answered by `answer`; yields a channel and the size
    of every batch the pump took."""
    loop = asyncio.get_running_loop()
    server = Server(threads=1)
    port, fd = server.start_queue(0)
    wake = socket.socket(fileno=fd)
    batches: list[int] = []

    async def run(call: Call) -> None:
        result = answer(call)
        if asyncio.iscoroutine(result):
            await result

    def pump() -> None:
        try:
            while wake.recv(4096):
                pass
        except BlockingIOError:
            pass
        calls = server.take()
        batches.append(len(calls))
        for call in calls:
            loop.create_task(run(call))

    loop.add_reader(fd, pump)
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield channel, batches
    finally:
        loop.remove_reader(fd)
        server.stop()
        wake.close()


async def test_calls_are_answered_with_their_own_bytes() -> None:
    async def echo(call: Call) -> None:
        await asyncio.sleep(0)
        call.reply(call.path.encode() + call.body)

    async with serving(echo) as (channel, batches):
        payloads = [str(i).encode() for i in range(200)]
        calls = [channel.unary_unary(f"/s.S/M{i % 3}")(p) for i, p in enumerate(payloads)]
        replies = await asyncio.gather(*calls)
    assert replies == [f"/s.S/M{i % 3}".encode() + p for i, p in enumerate(payloads)]
    assert sum(batches) == 200
    assert max(batches) > 1  # one wake-up served several calls


async def test_fail_sends_the_status() -> None:
    def refuse(call: Call) -> None:
        call.fail(grpc.StatusCode.NOT_FOUND.value[0], f"no {call.path}")

    async with serving(refuse) as (channel, _):
        try:
            await channel.unary_unary("/s.S/Get")(b"x")
        except grpc.aio.AioRpcError as e:
            assert (e.code(), e.details()) == (grpc.StatusCode.NOT_FOUND, "no /s.S/Get")
        else:
            raise AssertionError("the call succeeded")


async def test_a_call_answered_twice_keeps_the_first_answer() -> None:
    def twice(call: Call) -> None:
        call.reply(b"first")
        call.reply(b"second")
        call.fail(13, "late")

    async with serving(twice) as (channel, _):
        assert await channel.unary_unary("/s.S/Get")(b"") == b"first"
