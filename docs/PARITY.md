# Parity with `grpc.aio`

What fastgrpc3 offers on both transports, how the tonic transport provides it, and where it
differs. "Tested" names the fastgrpc3 tests (`fastgrpc/tests/`), which run on both
transports (`transport` fixture); the whole suite also passes with `FASTGRPC_TRANSPORT=tonic`.
Plan: §8 and lot 6 of the development plan (not published).

| Feature | tonic transport | Tested | Difference |
|---|---|---|---|
| Unary, server, client, bidirectional streams | routes declared in Rust, one `Invoker` | `test_runtime.py` | — |
| DI, hooks, middlewares, exception handlers, cleanup | the shared `Invoker` | `test_runtime.py`, `test_deps.py` | — |
| Status codes and messages | `Call.fail(code, message)` | `test_runtime.py` | — |
| Rich error details (`google.rpc.Status`) | `fail(..., details)` → `grpc-status-details-bin` | `test_runtime.py` | — |
| Request metadata, repeated keys, `-bin` | `Call.metadata()`, converted when read | `test_runtime.py` (hooks), `test_deps.py` | `grpc-*`, `te`, `content-type` left out |
| Response headers (`send_initial_metadata`) | streams: with the first message, the call or the end; unary: with the answer | `test_metadata.py` | unary headers leave with the answer, not at the call |
| Trailers (`set_trailing_metadata`) | merged into the status trailers (success: body trailers; failure: status metadata) | `test_metadata.py` | — |
| Peer (`ctx.peer()`) | `ipv4:a.b.c.d:port`, `ipv6:%5B…%5D:port` | `test_metadata.py` | — |
| Deadline (`time_remaining`, propagation) | `grpc-timeout` read when tonic hands the request over | `test_cancellation.py`, `test_client.py` | — |
| Cancellation (client, deadline, stop) | `Watch` guard → task cancelled, `Depends(yield)` cleaned | `test_cancellation.py` | — |
| TLS | rustls (`tls-ring`), PEM cert and key | `test_tls.py`, `test_client.py` | handshake timeout 120 s, as `grpc` |
| mTLS, peer identities | client CA; `peer_identities`: SANs, else CN | `test_tls.py` | — |
| `auth_context` | `transport_security_type`, `security_level`, `x509_common_name`, `x509_subject`, `x509_subject_alternative_name`, `peer_dns`, `peer_uri`, `peer_spiffe_id`, `x509_pem_cert` | `test_tls.py` | no session details (`ssl_tls_version`, `ssl_session_reused`, `ssl_requested_server_name`, `ssl_negotiated_key_exchange_group`): tonic does not expose them |
| Compression | gzip and deflate accepted; `compression=` compresses responses if the client accepts it | `test_limits.py` | — |
| `max_recv_bytes` | tonic's decoding limit, also after decompression | `test_limits.py` | tonic's `OUT_OF_RANGE` rewritten to `RESOURCE_EXHAUSTED`; message text differs |
| `max_send_bytes` | checked by the bridge before sending | `test_limits.py` | — |
| `max_metadata_bytes` | HTTP/2 `SETTINGS_MAX_HEADER_LIST_SIZE` (hyper) | by hand | stricter: with a 1 KiB limit, 4 KiB of request metadata passes on `grpc.aio` (soft limit) but gets the client's connection closed on tonic (`GOAWAY header_list_way_too_large`, `UNAVAILABLE`) |
| `maximum_concurrent_rpcs` | admission semaphore, `RESOURCE_EXHAUSTED` | `test_limits.py` | — |
| `Keepalive(time_ms, timeout_ms)` | HTTP/2 keepalive interval and timeout | — | `permit_without_calls`: no such setting in tonic, ignored |
| `run --workers` (port sharing) | `SO_REUSEPORT` (`("grpc.so_reuseport", 1)`) | `test_ops.py` | not on Windows |
| Health (`grpc.health.v1`) | the fastgrpc3 service, unchanged | `test_ops.py`; `grpc-health-probe` by hand | — |
| Reflection v1 and v1alpha | the fastgrpc3 services, unchanged | `test_ops.py`; `grpcurl list/describe` by hand | — |
| Interactive docs and JSON gateway | unchanged (HTTP side) | `test_docs.py` | — |
| Native `grpc.aio` interceptors | refused at `start()` | `test_transport.py` | structural: use hooks or middlewares |
| Other raw `grpc` options | refused at `start()` | `test_transport.py` | — |
