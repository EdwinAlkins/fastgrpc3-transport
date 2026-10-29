"""Variant B through a real gRPC client: unary answers and failures, server streams with
backpressure, cancellation and errors, unknown methods."""

import asyncio
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import grpc
import grpc.aio
import pytest
from fastgrpc3_transport import Call, Server

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "bench"))

from bridge import Bridge, streaming  # noqa: E402

Handlers = dict[str, Callable[[Call], Awaitable[None]]]


@asynccontextmanager
async def serving(
    handlers: Handlers, stream_capacity: int = 16
) -> AsyncIterator[tuple[grpc.aio.Channel, Bridge]]:
    bridge = Bridge(Server(threads=1, stream_capacity=stream_capacity), handlers)
    port = bridge.start(0)
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield channel, bridge
    finally:
        bridge.stop()


# -- unary ---------------------------------------------------------------------------


async def echo(call: Call) -> None:
    await asyncio.sleep(0)
    call.reply(call.path.encode() + call.body)


async def test_calls_are_answered_with_their_own_bytes() -> None:
    handlers: Handlers = {f"/s.S/M{i}": echo for i in range(3)}
    async with serving(handlers) as (channel, bridge):
        payloads = [str(i).encode() for i in range(200)]
        calls = [channel.unary_unary(f"/s.S/M{i % 3}")(p) for i, p in enumerate(payloads)]
        replies = await asyncio.gather(*calls)
    assert replies == [f"/s.S/M{i % 3}".encode() + p for i, p in enumerate(payloads)]
    assert sum(bridge.batches) == 200
    assert max(bridge.batches) > 1  # one wake-up served several calls


async def test_fail_sends_the_status() -> None:
    async def refuse(call: Call) -> None:
        call.fail(grpc.StatusCode.NOT_FOUND.value[0], f"no {call.path}")

    async with serving({"/s.S/Get": refuse}) as (channel, _):
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await channel.unary_unary("/s.S/Get")(b"x")
    assert (raised.value.code(), raised.value.details()) == (
        grpc.StatusCode.NOT_FOUND,
        "no /s.S/Get",
    )


async def test_a_call_answered_twice_keeps_the_first_answer() -> None:
    async def twice(call: Call) -> None:
        call.reply(b"first")
        call.reply(b"second")
        call.fail(13, "late")

    async with serving({"/s.S/Get": twice}) as (channel, _):
        assert await channel.unary_unary("/s.S/Get")(b"") == b"first"


async def test_unknown_methods_are_unimplemented() -> None:
    async with serving({"/s.S/Get": echo}) as (channel, _):
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await channel.unary_unary("/s.S/Nope")(b"")
    assert raised.value.code() == grpc.StatusCode.UNIMPLEMENTED
    assert raised.value.details() == "no method /s.S/Nope"


# -- server streams -------------------------------------------------------------------


def counting_stream(
    bridge_box: list[Bridge], produced: list[int], fail_at: int
) -> Callable[[Call], Awaitable[None]]:
    """Body `b"<count>"` or `b"<count>,<size>"`: messages `b"<i>"`, padded with dots to
    `size` bytes; counts what it produced."""

    @streaming
    async def stream(call: Call) -> None:
        bridge = bridge_box[0]
        count, _, size = bytes(call.body).partition(b",")
        for i in range(int(count)):
            if i == fail_at:
                raise RuntimeError("broken")
            if not await bridge.send(call, str(i).encode().ljust(int(size or 0), b".")):
                return  # the client is gone
            produced[0] = i + 1
        call.finish()

    return stream


@asynccontextmanager
async def stream_server(
    fail_at: int = -1, stream_capacity: int = 16
) -> AsyncIterator[tuple[grpc.aio.Channel, Bridge, list[int]]]:
    box: list[Bridge] = []
    produced = [0]
    handlers: Handlers = {"/s.S/Stream": counting_stream(box, produced, fail_at)}
    async with serving(handlers, stream_capacity) as (channel, bridge):
        box.append(bridge)
        yield channel, bridge, produced


async def test_a_long_stream_arrives_whole_and_in_order() -> None:
    async with stream_server() as (channel, _, produced):
        received = [m async for m in channel.unary_stream("/s.S/Stream")(b"20000")]
    assert received == [str(i).encode() for i in range(20000)]
    assert produced[0] == 20000


async def test_a_reader_that_stops_stops_the_producer() -> None:
    """Backpressure: what is produced while the client does not read is bounded by the
    channel and HTTP/2's windows (the grpc C-core client grows its own to a few MB), not by
    the stream's length: 40,000 messages of 1 KB."""
    async with stream_server() as (channel, _, produced):
        call = channel.unary_stream("/s.S/Stream")(b"40000,1024")
        first = await call.read()
        await asyncio.sleep(0.5)  # the client stops reading
        paused_at = produced[0]
        await asyncio.sleep(0.3)
        assert produced[0] == paused_at  # the producer waits
        assert first.startswith(b"0.") and paused_at < 20000
        rest = []  # the client reads again: the stream resumes
        while (message := await call.read()) is not grpc.aio.EOF:
            rest.append(message)
    assert len(rest) == 39999 and produced[0] == 40000
    print(f"produced while the client paused: {paused_at} messages of 1 KB")


async def test_a_client_that_cancels_stops_the_producer() -> None:
    async with stream_server() as (channel, bridge, produced):
        call = channel.unary_stream("/s.S/Stream")(b"200000")
        await call.read()
        await asyncio.sleep(0.1)  # the producer fills what it can, then waits
        call.cancel()
        for _ in range(100):
            if not bridge.tasks:
                break
            await asyncio.sleep(0.02)
        assert not bridge.tasks  # the handler returned
        assert produced[0] < 200000


async def test_a_failing_stream_ends_with_its_status() -> None:
    async with stream_server(fail_at=3) as (channel, _, _produced):
        call = channel.unary_stream("/s.S/Stream")(b"10")
        received = []
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            async for message in call:
                received.append(message)
    assert received == [b"0", b"1", b"2"]
    assert raised.value.code() == grpc.StatusCode.INTERNAL
    assert raised.value.details() == "RuntimeError: broken"
