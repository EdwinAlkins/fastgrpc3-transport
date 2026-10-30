# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: the API may change between minor
versions).

## [0.1.0] - Pending

First release: an experimental gRPC server transport for fastgrpc3 on tonic, selected with
`fastgrpc3 run --transport tonic`, `FASTGRPC_TRANSPORT=tonic` or
`GrpcApp(transport="tonic")`.

### Added

- The four kinds of call (unary, server, client and bidirectional streams), run by
  fastgrpc3's own runtime: dependencies, hooks, middlewares, exception handlers, cleanup.
- A tokio/asyncio bridge that hands calls to the loop by batches, woken through a socket
  only when the queue was empty (a Unix socket pair; on Windows, a loopback TCP pair read
  with `sock_recv`).
- Backpressure on response streams (bounded channel) and request streams (one message per
  credit); admission limit (`maximum_concurrent_rpcs`, `RESOURCE_EXHAUSTED`).
- Cancellation (client gone, deadline, closed connection, stop) cancels the handler's task;
  `grpc-timeout` deadlines, `Context.time_remaining()` and their propagation.
- Request metadata, response headers and trailers, peer address, error details
  (`google.rpc.Status`).
- TLS and mTLS (rustls), client identity and `auth_context` with `grpc`'s keys; gzip and
  deflate; message and metadata size limits; HTTP/2 keepalive; port sharing
  (`SO_REUSEPORT`, Linux and macOS) for `fastgrpc3 run --workers`.
- Graceful stop bounded by its grace period.
- `TRANSPORT_API_VERSION = 2`, checked by fastgrpc3.

### Packaging

- Wheels for CPython 3.12, 3.13 and 3.14 on Linux (x86_64, aarch64, manylinux), macOS
  (arm64, x86_64) and Windows (x86_64), tested by installing them without compiling.
- Linux wheels built with profile-guided optimization (`scripts/pgo-prepare.sh`, trained
  by a fastgrpc3 app under load): about −11 % CPU per call on the tokio side.

### Known limitations

- See `KNOWN_ISSUES.md` and the comparison with `grpc.aio` in `docs/PARITY.md`.
- Port sharing (`--workers`) is not available on Windows.
- fastgrpc3's codec, protoruf, has no Linux aarch64 wheel yet: installing fastgrpc3 there
  compiles it.

[0.1.0]: https://github.com/EdwinAlkins/fastgrpc3-transport/releases/tag/v0.1.0
