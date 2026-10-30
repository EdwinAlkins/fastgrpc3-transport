# Architecture — the variant B bridge

How a unary call travels between tokio (Rust, no GIL) and asyncio (Python), as built in
`src/python.rs` (`start_queue`). The design reference is
the development plan of the transport (§5, §17, not published).

```text
 tokio thread(s), no GIL                          asyncio loop, GIL
┌──────────────────────────────────┐            ┌─────────────────────────────────────┐
│ tonic: HTTP/2, gRPC framing       │            │                                     │
│ Router (any path) -> Unary        │            │                                     │
│   Pending { path, body, oneshot } │  push      │                                     │
│   ──────────────────────────────► │ Queue      │                                     │
│   first in an empty queue:        │ (Mutex)    │                                     │
│   write 1 byte to the waker ──────┼── socket ─►│ add_reader(fd) -> pump():           │
│                                   │            │   drain the socket                  │
│                                   │            │   Server.take() -> [Call, ...]      │
│                                   │            │   one task per Call: handler        │
│   await oneshot ◄─────────────────┼────────────┤   Call.reply(bytes) / fail(code)    │
│ tonic writes the response         │            │                                     │
└──────────────────────────────────┘            └─────────────────────────────────────┘
```

## Invariants

- **tokio never takes the GIL** on the unary path: it pushes plain Rust values (`String`,
  `Bytes`, a `oneshot::Sender`) under a mutex. Python objects are built in `take()`, which
  Python calls with the GIL held.
- **No lost wake-up**: a push writes to the waker only when it makes the queue non-empty;
  `take()` swaps the whole queue out under the same mutex. A call pushed after a `take()`
  finds the queue empty and writes again, so the reader fires again. Extra bytes in the
  socket only cause a spare, empty `take()`.
- **One wake-up serves a batch**: under load, many calls arrive between two pumps. This is
  why variant B is ~1.5× variant A (one wake-up and GIL acquisition per call).
- **One answer per call**: `Call.reply` / `Call.fail` take the `oneshot::Sender` out;
  later answers are ignored. A `Call` dropped without an answer drops its sender, and the
  RPC ends with `CANCELLED`.
- **Copies**: the request body is copied once into a Python `bytes` (`take()`), the
  response once back into `Bytes` (`reply()`).

## Server streams (lot 1)

```text
 Python handler                      tokio / tonic                       client
 bridge.send(call, data)
   call.try_send(data) ── SENT ───► mpsc (bounded, stream_capacity) ─► HTTP/2 ─► reads
                       ── FULL ──┐   tonic pulls a message only when HTTP/2 can send it
   await waiter ◄─ pump ◄─ queue ◄─ Writable(id) ◄─ task: sender.reserve().await
                       ── CLOSED ─► the client is gone: the handler stops
 call.finish() / call.fail(code, message)
```

- The handler never blocks the loop: `try_send` is synchronous; when the channel is full,
  `when_writable()` spawns a tokio task that waits for room, then pushes a `Writable(id)`
  event into the same queue as new calls, so the same batched wake-up resumes the handler.
- Backpressure goes from the client's HTTP/2 window back to the handler: a reader that
  stops makes the producer stop (bounded by the channel, tonic's and HTTP/2's buffers).
- `fail()` on a stream sends the status after the messages already queued (a tokio task
  waits for room); `finish()` drops the sender, which ends the stream with `OK`.
- A route unknown to the table answers `UNIMPLEMENTED` in Rust, before any Python.

## Limits, terminal states, stop (lot 2)

- **Admission**: one `Semaphore` of `max_concurrent_rpcs` permits. A call takes a permit
  before it is queued (`Queue::admit`) and keeps it for its whole life on the tokio side:
  the unary future holds it until the answer, a server stream's body (`Responses`) until
  the stream ends. So one limit bounds the queue, the calls Python holds and the tasks the
  pump started. When none is free, the call waits up to `admission_timeout` (zero: not at
  all), then gets `RESOURCE_EXHAUSTED`; waiting is a tokio future, never a blocked thread.
- **Fair pump**: `take(max_events)` hands out at most `max_events` events, oldest first,
  and says whether more remain. The pump (`Bridge._pump`) then reschedules itself with
  `loop.call_soon`: a push writes to the socket only when the queue goes from empty to
  non-empty, so leftovers would otherwise wait for the next call. Other asyncio tasks run
  between pump turns.
- **Terminal states**: every RPC ends exactly once. `reply` / `fail` / `finish` take the
  answer channel out of the `Call`; later answers are ignored. A `Call` freed without an
  answer ends its RPC with `INTERNAL` (unary: the oneshot is dropped; stream: `Drop`
  sends the status, or the dropped sender would end the stream with `OK`). The pump fails
  a cancelled handler's call with `CANCELLED`.
- **Graceful, bounded stop**: `Server.shutdown(grace)` returns at once. On the tokio
  thread, tonic stops accepting and drains the calls in flight for at most `grace`; then
  the serving future is dropped (remaining connections closed) and the runtime stops
  (`shutdown_timeout(1 s)`). The asyncio loop keeps running meanwhile, so handlers in
  flight can still answer: `await Bridge.stop(grace)` polls `finished()`, cancels the
  handlers left, then `join()`s a thread that has already ended.

## Not there yet

The prototype's limits are listed in `KNOWN_ISSUES.md`; next in the plan: lot 3 (the
fastgrpc3 side: `TransportContext`, backends), lot 4 (fastgrpc3's runtime on this
transport, client and bidirectional streams), lot 5 (cancellation and deadlines).
