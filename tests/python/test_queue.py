"""Variant B through a real gRPC client: unary answers and failures, server streams with
backpressure, cancellation and errors, unknown methods."""

import asyncio
import sys
import time
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
    handlers: Handlers, max_batch: int = 128, **options: float
) -> AsyncIterator[tuple[grpc.aio.Channel, Bridge]]:
    """A server with `handlers` (`options` go to `Server`), and a channel to it."""
    bridge = Bridge(Server(threads=1, **options), handlers, max_batch)
    port = bridge.start(0)
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield channel, bridge
    finally:
        await bridge.stop()


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


async def test_a_burst_is_served_in_bounded_batches() -> None:
    """With `max_batch`, the pump takes a few events per loop turn and comes back for the
    rest by itself (the socket is not written again while events remain): every call is
    served, and other tasks run during the burst."""
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    async with serving({"/s.S/M0": echo}, max_batch=4) as (channel, bridge):
        running = asyncio.ensure_future(ticker())
        replies = await asyncio.gather(
            *(channel.unary_unary("/s.S/M0")(str(i).encode()) for i in range(300))
        )
        running.cancel()
    assert replies == [b"/s.S/M0" + str(i).encode() for i in range(300)]
    assert sum(bridge.batches) == 300 and max(bridge.batches) <= 4
    assert ticks > len(bridge.batches) / 2  # the ticker ran between pump turns


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
    async with serving(handlers, stream_capacity=stream_capacity) as (channel, bridge):
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


# -- admission ------------------------------------------------------------------------


def holding() -> tuple[Handlers, asyncio.Event, list[int]]:
    """`/s.S/Hold` waits for the event before answering; `/s.S/Get` answers at once."""
    release, started = asyncio.Event(), []

    async def hold(call: Call) -> None:
        started.append(call.id)
        await release.wait()
        call.reply(b"held")

    async def get(call: Call) -> None:
        call.reply(b"ok")

    return {"/s.S/Hold": hold, "/s.S/Get": get}, release, started


async def until(condition: Callable[[], bool]) -> None:
    for _ in range(200):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("timed out")


async def test_calls_beyond_the_limit_are_refused() -> None:
    handlers, release, started = holding()
    async with serving(handlers, max_concurrent_rpcs=2) as (channel, _):
        held = [asyncio.ensure_future(channel.unary_unary("/s.S/Hold")(b"")) for _ in range(2)]
        await until(lambda: len(started) == 2)
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await channel.unary_unary("/s.S/Get")(b"")
        assert raised.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
        release.set()
        assert await asyncio.gather(*held) == [b"held", b"held"]
        assert await channel.unary_unary("/s.S/Get")(b"") == b"ok"  # permits came back


async def test_a_call_may_wait_for_a_permit() -> None:
    handlers, release, started = holding()
    async with serving(handlers, max_concurrent_rpcs=1, admission_timeout=5.0) as (channel, _):
        held = asyncio.ensure_future(channel.unary_unary("/s.S/Hold")(b""))
        await until(lambda: len(started) == 1)
        waiting = asyncio.ensure_future(channel.unary_unary("/s.S/Get")(b""))
        await asyncio.sleep(0.2)
        assert not waiting.done()  # queued for a permit, not refused
        release.set()
        assert (await held, await waiting) == (b"held", b"ok")


async def test_a_stream_holds_its_permit_until_it_ends() -> None:
    box: list[Bridge] = []
    produced = [0]
    handlers: Handlers = {
        "/s.S/Stream": counting_stream(box, produced, -1),
        "/s.S/Get": holding()[0]["/s.S/Get"],
    }
    async with serving(handlers, max_concurrent_rpcs=1) as (channel, bridge):
        box.append(bridge)
        stream = channel.unary_stream("/s.S/Stream")(b"40000,1024")
        await stream.read()  # open, and paused by backpressure
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await channel.unary_unary("/s.S/Get")(b"")
        assert raised.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
        while await stream.read() is not grpc.aio.EOF:
            pass
        await until(lambda: not bridge.tasks)
        assert await channel.unary_unary("/s.S/Get")(b"") == b"ok"


# -- terminal states ------------------------------------------------------------------


async def test_handlers_that_do_not_answer_end_with_internal() -> None:
    async def silent(call: Call) -> None:
        return None  # no reply

    @streaming
    async def silent_stream(call: Call) -> None:
        return None  # no finish

    handlers: Handlers = {"/s.S/Unary": silent, "/s.S/Stream": silent_stream}
    async with serving(handlers) as (channel, _):
        with pytest.raises(grpc.aio.AioRpcError) as unary:
            await channel.unary_unary("/s.S/Unary")(b"")
        with pytest.raises(grpc.aio.AioRpcError) as stream:
            [m async for m in channel.unary_stream("/s.S/Stream")(b"")]
    assert (unary.value.code(), unary.value.details()) == (
        grpc.StatusCode.INTERNAL,
        "the handler did not answer",
    )
    assert (stream.value.code(), stream.value.details()) == (
        grpc.StatusCode.INTERNAL,
        "the handler did not finish the stream",
    )


async def test_a_cancelled_task_ends_its_call_with_cancelled() -> None:
    handlers, _release, started = holding()
    async with serving(handlers) as (channel, bridge):
        call = asyncio.ensure_future(channel.unary_unary("/s.S/Hold")(b""))
        await until(lambda: len(started) == 1)
        for task in bridge.tasks:
            task.cancel()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await call
    assert raised.value.code() == grpc.StatusCode.CANCELLED


# -- graceful stop --------------------------------------------------------------------


async def test_stop_lets_calls_in_flight_finish() -> None:
    async def slow(call: Call) -> None:
        await asyncio.sleep(0.3)
        call.reply(b"done")

    bridge = Bridge(Server(threads=1), {"/s.S/Slow": slow})
    port = bridge.start(0)
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        call = asyncio.ensure_future(channel.unary_unary("/s.S/Slow")(b""))
        await until(lambda: bool(bridge.tasks))
        started = time.monotonic()
        await bridge.stop(grace=5.0)
        elapsed = time.monotonic() - started
        assert await call == b"done"
    assert 0.2 < elapsed < 2.0  # as long as the call needed, not the whole grace


async def test_stop_is_bounded_by_the_grace_period() -> None:
    """An endless stream whose client does not read: the grace period ends it."""
    box: list[Bridge] = []
    produced = [0]
    bridge = Bridge(Server(threads=1), {"/s.S/Stream": counting_stream(box, produced, -1)})
    box.append(bridge)
    port = bridge.start(0)
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        stream = channel.unary_stream("/s.S/Stream")(b"1000000,1024")
        await stream.read()
        await until(lambda: produced[0] > 100)
        started = time.monotonic()
        await bridge.stop(grace=0.5)
        elapsed = time.monotonic() - started
        assert not bridge.tasks  # the handler was cancelled
        with pytest.raises(grpc.aio.AioRpcError):
            while await stream.read() is not grpc.aio.EOF:
                pass
    assert 0.4 < elapsed < 3.0


async def test_an_idle_connection_does_not_delay_the_stop() -> None:
    bridge = Bridge(Server(threads=1), {"/s.S/Get": echo})
    port = bridge.start(0)
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        await channel.unary_unary("/s.S/Get")(b"")  # the connection stays open, idle
        started = time.monotonic()
        await bridge.stop(grace=5.0)
        assert time.monotonic() - started < 1.0
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await channel.unary_unary("/s.S/Get")(b"", timeout=2)
    assert raised.value.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
