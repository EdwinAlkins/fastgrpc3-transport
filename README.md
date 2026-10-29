# fastgrpc3-transport

A gRPC server transport for [fastgrpc3](../fastgrpc) on
[tonic](https://github.com/hyperium/tonic) (Rust, tokio), with the handlers still in
Python. Today it is the prototype of spike S4; the plan to turn it into a transport is
`fastgrpc/private/docs/conception/11.1-transport-rust.md`. Local only, nothing published.

**Spike S4 result** (`bench/results/2026-10-30.md`): on one core, medium message, the
variant B bridge serves **27,326 / 32,406 requests/s at concurrency 16 / 64**, against
4,163 / 4,096 for fastgrpc3 on `grpc.aio`: 6.6× / 7.9×, with p50 at c=64 down from 15.3 ms
to 1.6 ms. On two cores, one process does 5.4× / 6.2× `fastgrpc3 run --workers 2`.

## Layout

| Path | What |
|------|------|
| `src/echo.rs`, `src/bin/echo.rs` | a pure-Rust server answering every unary call with its request bytes: the transport's ceiling |
| `src/python.rs` | the PyO3 extension `fastgrpc3_transport`: `Server.start_queue` (variant B, see `docs/ARCHITECTURE.md`), `Call.reply` / `Call.fail`; `Server.start_direct` (variant A) behind the `variant-a` feature, for benchmarks |
| `tests/echo.rs` | the echo server through a real tonic client (`cargo test`) |
| `tests/python/` | the variant B bridge through a real `grpc.aio` client (`pytest`) |
| `bench/` | the S4 benchmark (`s4_bench.py`), its Python servers, and the results |
| `KNOWN_ISSUES.md` | what the prototype does not do yet, and the lot of the plan that fixes it |

## Build and test

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --features python -- -D warnings
cargo test --all-targets

# The extension, into fastgrpc3's environment (Python 3.12+); add `--features variant-a`
# for the benchmark of variant A.
../protoruf/.venv/bin/maturin build --release -i ../fastgrpc/.venv/bin/python --out dist
uv pip install --python ../fastgrpc/.venv --reinstall dist/fastgrpc3_transport-*.whl
../fastgrpc/.venv/bin/python -m pytest
```

## Benchmark

`bench/s4_bench.py` reuses fastgrpc3's harness (`fastgrpc/benches/e2e_bench.py`): each
server is pinned to CPUs with all its threads, `ghz` loads it from others with the
pre-encoded requests of `fastgrpc/benches/contract.py`.

```bash
cargo build --release --bin echo
../fastgrpc/.venv/bin/python bench/s4_bench.py --servers raw fastgrpc3 tonic-echo tonic-B
../fastgrpc/.venv/bin/python bench/s4_bench.py --servers fastgrpc3-2workers tonic-B \
    --server-cpus 0,2 --client-cpus 4-15
```

Servers: `raw` (`grpc.aio` + upb), `fastgrpc3` and `fastgrpc3-2workers` (on `grpc.aio`),
`tonic-echo` (no Python), `tonic-A` / `tonic-B` (`bench/python_server.py`: decode and
validate with `MessageCodec`, await the handler, encode, as fastgrpc3's fast path does),
`tonic-B-2threads`.
