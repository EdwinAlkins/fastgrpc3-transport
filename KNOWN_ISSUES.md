# Known issues

The prototype of spike S4 measures the bridge; it is not a transport yet. Lot numbers refer
to `fastgrpc/private/docs/conception/11.1-transport-rust.md` (§17).

| Issue | Effect | Lot |
|-------|--------|-----|
| The queue (`Mutex<Vec<Pending>>`) is unbounded, and so is the number of Python tasks the pump starts | a flood of calls grows memory without limit | 2 |
| No admission limit (`maximum_concurrent_rpcs`) | no `RESOURCE_EXHAUSTED` under overload | 2 |
| The pump drains everything at once | a large batch can starve other asyncio tasks | 2 |
| `Server.stop()` joins the tokio thread after a graceful shutdown that waits for open connections | `stop()` can block for as long as a client keeps its connection open | 2 |
| Unary calls and server streams only | no client or bidirectional streams (the Rust → Python direction has no bounded channel yet) | 4 |
| A client's cancellation or deadline does not reach the Python task, except a server stream's next `send` (which returns `False`) | a unary handler, or a stream between two sends, keeps running after the RPC is gone | 5 |
| No metadata, trailers or error details through the bridge; `fail()` takes a code and a message only | `Context` features, `google.rpc` details unavailable | 6 |
| Plaintext only, bound to `127.0.0.1` | no TLS, mTLS or other interfaces | 6 |
| The wake-up uses a Unix socket and `loop.add_reader` | not available on Windows (`ProactorEventLoop`) | 8 |
| `fastgrpc3` is not involved: the bench server calls `MessageCodec` and the handler itself | no DI, middlewares, hooks or exception handlers yet | 3, 4 |

Fixed: routes are declared before starting (`Server.add_route`), and any other path is
`UNIMPLEMENTED` in Rust (lot 1).

Fixed during the spike: with `serve_with_incoming`, tonic does not set `TCP_NODELAY` on
accepted connections (`serve(addr)` does); without it, Nagle and delayed ACKs added ~40 ms
per call at concurrency 1. Accepted streams now get `set_nodelay(true)`.
