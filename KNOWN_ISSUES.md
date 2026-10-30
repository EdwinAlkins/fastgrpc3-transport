# Known issues

The prototype of spike S4 measures the bridge; it is not a transport yet. Lot numbers refer
to `fastgrpc/private/docs/conception/11.1-transport-rust.md` (§17).

| Issue | Effect | Lot |
|-------|--------|-----|
| A client's cancellation or deadline does not reach the Python task, except a server stream's next `send` (which returns `False`) | a unary handler, or a stream between two sends, keeps running after the RPC is gone | 5 |
| No response metadata (headers, trailers), no peer through the bridge | `Context.send_initial_metadata` / `set_trailing_metadata` raise `NotImplementedError` in fastgrpc3 | 6 |
| Plaintext only; no compression, keepalive or size limits other than tonic's defaults | fastgrpc3 refuses these settings with `transport="tonic"` (but does not enforce `max_send_bytes`) | 6 |
| The wake-up uses a Unix socket and `loop.add_reader` | not available on Windows (`ProactorEventLoop`) | 8 |

Fixed:

- Lot 4: client and bidirectional streams (`client_stream`, `bidi_stream` routes; requests
  read one message per credit, `Call.consumed()`), request metadata (`Call.metadata()`,
  converted only when asked), error details (`fail(code, message, details)`), any listening
  address (`start_queue(port, host)`). fastgrpc3 runs on it: `GrpcApp(transport="tonic")`
  (`fastgrpc/src/fastgrpc3/transport/tonic.py`).

- Lot 1: routes are declared before starting (`Server.add_route`), and any other path is
  `UNIMPLEMENTED` in Rust.
- Lot 2: one admission permit per RPC (`max_concurrent_rpcs`, `RESOURCE_EXHAUSTED` after
  `admission_timeout`) bounds the queue, the calls in Python and their tasks; the pump
  takes at most `max_batch` events per loop turn; a handler that returns without answering
  ends its RPC with `INTERNAL`, a cancelled one with `CANCELLED`; the stop is graceful and
  bounded by `grace` (`Server.shutdown` / `finished` / `join`, `await Bridge.stop`).

Fixed during the spike: with `serve_with_incoming`, tonic does not set `TCP_NODELAY` on
accepted connections (`serve(addr)` does); without it, Nagle and delayed ACKs added ~40 ms
per call at concurrency 1. Accepted streams now get `set_nodelay(true)`.
