# Architecture — the variant B bridge

How a unary call travels between tokio (Rust, no GIL) and asyncio (Python), as built in
`src/python.rs` (`start_queue`). The design reference is
`fastgrpc/private/docs/conception/11.1-transport-rust.md` (§5, §17).

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

## Not there yet

The prototype's limits are listed in `KNOWN_ISSUES.md`; the plan to remove them is lot 2
(bounded queue and admission, fair pump, terminal states, graceful stop), lot 1 (streams)
and lot 5 (cancellation and deadlines) of the design note.
