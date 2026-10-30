# fastgrpc3-transport

A gRPC server transport for [fastgrpc3](https://github.com/EdwinAlkins/fastgrpc3) on
[tonic](https://github.com/hyperium/tonic) (Rust, tokio), with the handlers still written in
Python. HTTP/2, gRPC framing, TLS, compression and flow control run in Rust; the asyncio
loop receives the calls by batches and runs them through fastgrpc3's own runtime, so
services, dependencies, hooks, middlewares and errors behave as on `grpc.aio`.

> **Status: experimental.** Wheels for Linux (x86_64, aarch64), macOS (arm64, x86_64) and
> Windows (x86_64), CPython 3.12 to 3.14, each tested in CI, on PyPI. See
> `KNOWN_ISSUES.md` and the parity matrix `docs/PARITY.md`.

## Use it

You do not import this package: fastgrpc3 selects it.

```bash
pip install "fastgrpc3[tonic]"
fastgrpc3 run myapp.main:app --transport tonic
```

or `FASTGRPC_TRANSPORT=tonic`, or `GrpcApp(..., transport="tonic")`.

On one core, fastgrpc3 serves a unary echo 6 to 10 times faster on tonic than on
`grpc.aio` under concurrency (16 to 64 clients), about 2 times faster with one client
(`bench/results/2026-10-30-lot8.md`).

## Layout

| Path | What |
|------|------|
| `src/python.rs` | the PyO3 extension `fastgrpc3_transport`: `Server`, the routes, the event queue shared by tokio and asyncio (`take`), `Call` (answers, streams, metadata, deadline, peer) |
| `src/peer.rs` | the mTLS client identity and `auth_context`, with `grpc`'s keys |
| `src/echo.rs`, `src/bin/echo.rs` | a pure-Rust server answering every unary call with its request bytes: the transport's ceiling |
| `fastgrpc3_transport.pyi` | the extension's types |
| `docs/ARCHITECTURE.md` | how a call travels between tokio and asyncio |
| `docs/PARITY.md` | each common feature with `grpc.aio`, its test, and the differences |
| `tests/` | the echo server through a tonic client (`cargo test`), the bridge through a `grpc.aio` client (`pytest`) |
| `bench/` | benchmarks against `grpc.aio`, and their results |

fastgrpc3's side is `src/fastgrpc3/transport/tonic.py` in its repository; its test suite
runs on both transports.

## Build and test

With a Rust toolchain and [maturin](https://www.maturin.rs/), next to a checkout of
fastgrpc3 (`../fastgrpc`):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --features python -- -D warnings
cargo test --all-targets

# This checkout into fastgrpc3's environment (`uv sync --extra tonic` there installs the
# version on PyPI instead); add `--features variant-a` for the variant A benchmark.
maturin build --release -i ../fastgrpc/.venv/bin/python --out dist
uv pip install --python ../fastgrpc/.venv --reinstall dist/fastgrpc3_transport-*.whl
../fastgrpc/.venv/bin/python -m pytest ../fastgrpc/tests   # fastgrpc3, on both transports
../fastgrpc/.venv/bin/python -m pytest tests/python
```

## Benchmark

`bench/s4_bench.py` reuses fastgrpc3's harness (`benches/e2e_bench.py`): each server is
pinned to CPUs, `ghz` loads it from others with pre-encoded requests.

```bash
cargo build --release --bin echo
../fastgrpc/.venv/bin/python bench/s4_bench.py --servers raw fastgrpc3 tonic-echo tonic-B
../fastgrpc/.venv/bin/python bench/stream_bench.py
```

fastgrpc3 itself on each transport: `FASTGRPC_TRANSPORT=tonic python benches/e2e_bench.py
--modes fastgrpc3` in its repository.

## License

MIT.
