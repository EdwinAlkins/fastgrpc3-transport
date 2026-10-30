# Known issues

What the transport does not do yet (it runs fastgrpc3 services since lot 4). Lot numbers refer
to the development plan of the transport (lots 0 to 9, not published).

| Issue | Effect | Lot |
|-------|--------|-----|
| No port sharing on Windows (no `SO_REUSEPORT`) | `fastgrpc3 run --workers N` fails at start on Windows with tonic | — |
| The Windows wake-up (loopback TCP, `sock_recv` task) has not been benchmarked against the Unix one | its throughput is unknown; tested for correctness only | 9 |
| protoruf, fastgrpc3's codec, has no Linux aarch64 wheel | installing fastgrpc3 on Linux ARM64 compiles protoruf (this package installs without compiling) | protoruf |

The differences left with `grpc.aio` on the common features are listed in
`docs/PARITY.md`.

Fixed:

- Lot 8: Windows (the loop reads the wake-up socket with `sock_recv` where it cannot watch
  it); wheels for Linux x86_64/aarch64, macOS arm64/x86_64 and Windows x86_64, CPython
  3.12–3.14, each installed without compiling and tested in CI (`release.yml`); the
  deadline margin that makes `DEADLINE_EXCEEDED` win over tonic's own timer every time.

- Lot 6: response headers and trailers, peer, TLS and mTLS (identities, `auth_context`),
  gzip/deflate, message and metadata size limits, keepalive, `SO_REUSEPORT`
  (`docs/PARITY.md`).

- Lot 5: a call tonic gives up before its end (client gone, deadline, closed connection,
  stop) is reported to Python (`take()`'s cancelled ids) and its task cancelled; the
  `grpc-timeout` deadline is read when tonic hands the request over (`Call.time_remaining()`),
  enforced by the bridge for unary and client stream calls (`DEADLINE_EXCEEDED`, not tonic's
  `CANCELLED: Timeout expired`) and for response streams (tonic bounds only the headers).

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
