//! Steps 2 and 3: tonic serving unary calls whose handlers are Python coroutines.
//!
//! - `start_queue` (variant B, the transport's design): tokio threads push calls into a
//!   queue without the GIL and wake the loop through a socket only when the queue was empty;
//!   Python drains it by batches (`take`) and answers each `Call` (`reply` / `fail`).
//! - `start_direct` (variant A, feature `variant-a`, benchmarks only): each call takes the
//!   GIL on a tokio thread and schedules its coroutine on the asyncio loop
//!   (`pyo3-async-runtimes`), one wake-up per call.

use std::convert::Infallible;
use std::io::Write;
use std::os::fd::IntoRawFd;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use bytes::Bytes;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::oneshot;
use tonic::body::Body;
use tonic::server::{Grpc, UnaryService};
use tonic::{Code, Status};
use tower::ServiceExt;

use crate::echo::BytesCodec;

type Reply = oneshot::Sender<Result<Bytes, Status>>;

struct Pending {
    path: String,
    body: Bytes,
    reply: Reply,
}

/// Calls waiting for Python, and the socket that wakes the loop.
struct Queue {
    calls: Mutex<Vec<Pending>>,
    waker: UnixStream,
}

impl Queue {
    fn push(&self, call: Pending) {
        let was_empty = {
            let mut calls = self.calls.lock().expect("queue");
            calls.push(call);
            calls.len() == 1
        };
        if was_empty {
            // Full socket buffer: the loop has wake-ups pending anyway.
            let _ = (&self.waker).write(&[1]);
        }
    }
}

/// How a call reaches Python.
#[derive(Clone)]
enum Bridge {
    #[cfg(feature = "variant-a")]
    Direct {
        handler: Arc<Py<PyAny>>,
        locals: Arc<pyo3_async_runtimes::TaskLocals>,
    },
    Queue(Arc<Queue>),
}

#[derive(Clone)]
struct Unary {
    bridge: Bridge,
    path: String,
}

impl UnaryService<Bytes> for Unary {
    type Response = Bytes;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<tonic::Response<Bytes>, Status>> + Send>,
    >;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let (bridge, path) = (self.bridge.clone(), self.path.clone());
        let body = request.into_inner();
        Box::pin(async move {
            let reply = match bridge {
                Bridge::Queue(queue) => {
                    let (sender, receiver) = oneshot::channel();
                    queue.push(Pending {
                        path,
                        body,
                        reply: sender,
                    });
                    receiver
                        .await
                        .unwrap_or_else(|_| Err(Status::cancelled("call dropped")))
                }
                #[cfg(feature = "variant-a")]
                Bridge::Direct { handler, locals } => direct(&handler, &locals, path, body).await,
            };
            reply.map(tonic::Response::new)
        })
    }
}

#[cfg(feature = "variant-a")]
async fn direct(
    handler: &Py<PyAny>,
    locals: &pyo3_async_runtimes::TaskLocals,
    path: String,
    body: Bytes,
) -> Result<Bytes, Status> {
    let future = Python::attach(|py| {
        let coroutine = handler.bind(py).call1((path, PyBytes::new(py, &body)))?;
        pyo3_async_runtimes::into_future_with_locals(locals, coroutine)
    })
    .map_err(|e| Status::internal(e.to_string()))?;
    let result = future.await.map_err(|e| Status::internal(e.to_string()))?;
    Python::attach(|py| {
        let bytes = result
            .bind(py)
            .cast::<PyBytes>()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Bytes::copy_from_slice(bytes.as_bytes()))
    })
}

/// Any path, unary, through `bridge`.
#[derive(Clone)]
struct Router {
    bridge: Bridge,
}

impl tower::Service<http::Request<Body>> for Router {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Infallible>> + Send>,
    >;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let unary = Unary {
            bridge: self.bridge.clone(),
            path: request.uri().path().to_owned(),
        };
        Box::pin(async move { Ok(Grpc::new(BytesCodec).unary(unary, request).await) })
    }
}

/// One unary call waiting for its answer (variant B).
#[pyclass(module = "fastgrpc3_transport")]
struct Call {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    body: Py<PyBytes>,
    reply: Option<Reply>,
}

#[pymethods]
impl Call {
    /// Answer with the response bytes.
    fn reply(&mut self, data: &[u8]) {
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Ok(Bytes::copy_from_slice(data)));
        }
    }

    /// Fail with a gRPC status code and message.
    fn fail(&mut self, code: i32, message: &str) {
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(Status::new(Code::from_i32(code), message)));
        }
    }
}

/// A tonic server running on its own tokio thread(s).
#[pyclass(module = "fastgrpc3_transport")]
struct Server {
    threads: usize,
    queue: Option<Arc<Queue>>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

#[pymethods]
impl Server {
    #[new]
    #[pyo3(signature = (threads = 1))]
    fn new(threads: usize) -> Self {
        Self {
            threads,
            queue: None,
            shutdown: None,
            thread: None,
        }
    }

    #[cfg(feature = "variant-a")]
    /// Variant A (benchmarks only): `handler(path, body)` returns a coroutine run on `event_loop`. Returns
    /// the bound port.
    fn start_direct(
        &mut self,
        handler: Py<PyAny>,
        event_loop: Py<PyAny>,
        port: u16,
    ) -> PyResult<u16> {
        let locals = Python::attach(|py| {
            pyo3_async_runtimes::TaskLocals::new(event_loop.into_bound(py)).copy_context(py)
        })?;
        let bridge = Bridge::Direct {
            handler: Arc::new(handler),
            locals: Arc::new(locals),
        };
        self.start(bridge, port)
    }

    /// Variant B: returns `(port, fd)`; the loop reads `fd` when calls are waiting, then
    /// calls `take()`.
    fn start_queue(&mut self, port: u16) -> PyResult<(u16, i32)> {
        let (python_end, waker) = UnixStream::pair()?;
        python_end.set_nonblocking(true)?;
        waker.set_nonblocking(true)?;
        let queue = Arc::new(Queue {
            calls: Mutex::new(Vec::new()),
            waker,
        });
        self.queue = Some(queue.clone());
        let bound = self.start(Bridge::Queue(queue), port)?;
        Ok((bound, python_end.into_raw_fd()))
    }

    /// The calls waiting since the last `take`, oldest first.
    fn take(&self, py: Python<'_>) -> PyResult<Vec<Call>> {
        let queue = self
            .queue
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("not a queue server"))?;
        let pending = std::mem::take(&mut *queue.calls.lock().expect("queue"));
        Ok(pending
            .into_iter()
            .map(|call| Call {
                path: call.path,
                body: PyBytes::new(py, &call.body).unbind(),
                reply: Some(call.reply),
            })
            .collect())
    }

    /// Stop serving and join the server thread.
    fn stop(&mut self, py: Python<'_>) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            py.detach(|| {
                let _ = thread.join();
            });
        }
    }
}

impl Server {
    fn start(&mut self, bridge: Bridge, port: u16) -> PyResult<u16> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", port))?;
        listener.set_nonblocking(true)?;
        let bound = listener.local_addr()?.port();
        let (shutdown, stopped) = oneshot::channel::<()>();
        let threads = self.threads;
        let thread = std::thread::Builder::new()
            .name("tonic".into())
            .spawn(move || {
                let mut builder = if threads == 1 {
                    tokio::runtime::Builder::new_current_thread()
                } else {
                    let mut builder = tokio::runtime::Builder::new_multi_thread();
                    builder.worker_threads(threads);
                    builder
                };
                let runtime = builder.enable_all().build().expect("tokio runtime");
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).expect("listener");
                    // As `serve(addr)` does: without it, Nagle and delayed ACKs add ~40 ms.
                    let incoming = tokio_stream::StreamExt::map(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        |stream| {
                            stream.inspect(|stream| {
                                let _ = stream.set_nodelay(true);
                            })
                        },
                    );
                    let service = Router { bridge }.map_request(
                        |request: http::Request<axum::body::Body>| request.map(Body::new),
                    );
                    let routes =
                        tonic::service::Routes::from(axum::Router::new().fallback_service(service));
                    let _ = tonic::transport::Server::builder()
                        .add_routes(routes)
                        .serve_with_incoming_shutdown(incoming, async {
                            let _ = stopped.await;
                        })
                        .await;
                });
            })?;
        self.shutdown = Some(shutdown);
        self.thread = Some(thread);
        Ok(bound)
    }
}

#[pymodule]
fn fastgrpc3_transport(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Server>()?;
    module.add_class::<Call>()?;
    Ok(())
}
