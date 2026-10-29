//! tonic serving calls whose handlers are Python coroutines.
//!
//! - `start_queue` (variant B, the transport's design): tokio threads push events into a
//!   queue without the GIL and wake the loop through a socket only when the queue was empty;
//!   Python drains it by batches (`take`). Events are new calls, answered through their
//!   `Call` (`reply`, `try_send` / `finish`, `fail`), and "writable again" notices for
//!   server streams whose bounded channel was full (`when_writable`).
//! - `start_direct` (variant A, feature `variant-a`, benchmarks only, unary only): each call
//!   takes the GIL on a tokio thread and schedules its coroutine on the asyncio loop
//!   (`pyo3-async-runtimes`), one wake-up per call.
//!
//! Routes are declared before starting (`add_route`); any other path is `UNIMPLEMENTED`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;
use std::os::fd::IntoRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use bytes::Bytes;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body;
use tonic::server::{Grpc, ServerStreamingService, UnaryService};
use tonic::{Code, Status};
use tower::ServiceExt;

use crate::echo::BytesCodec;

type Answer = Result<Bytes, Status>;
type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// `try_send` results.
const SENT: u8 = 0;
const FULL: u8 = 1;
const CLOSED: u8 = 2;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Unary,
    ServerStream,
}

/// Where a call's answer goes.
enum Reply {
    Unary(oneshot::Sender<Answer>),
    Stream(mpsc::Sender<Answer>),
}

struct Pending {
    id: u64,
    path: String,
    body: Bytes,
    reply: Reply,
}

/// What tokio hands to Python.
enum Event {
    Call(Pending),
    /// The response channel of stream `id` has room again (or was closed).
    Writable(u64),
}

/// Events waiting for Python, and the socket that wakes the loop.
struct Queue {
    events: Mutex<Vec<Event>>,
    waker: UnixStream,
    next_id: AtomicU64,
    /// Messages a server stream may have queued for tonic before `try_send` says `FULL`.
    stream_capacity: usize,
}

impl Queue {
    fn push(&self, event: Event) {
        let was_empty = {
            let mut events = self.events.lock().expect("queue");
            events.push(event);
            events.len() == 1
        };
        if was_empty {
            // Full socket buffer: the loop has wake-ups pending anyway.
            let _ = (&self.waker).write(&[1]);
        }
    }

    fn call(&self, path: String, body: Bytes, reply: Reply) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.push(Event::Call(Pending {
            id,
            path,
            body,
            reply,
        }));
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
    type Future = BoxFuture<Result<tonic::Response<Bytes>, Status>>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let (bridge, path) = (self.bridge.clone(), self.path.clone());
        let body = request.into_inner();
        Box::pin(async move {
            let answer = match bridge {
                Bridge::Queue(queue) => {
                    let (sender, receiver) = oneshot::channel();
                    queue.call(path, body, Reply::Unary(sender));
                    receiver
                        .await
                        .unwrap_or_else(|_| Err(Status::cancelled("call dropped")))
                }
                #[cfg(feature = "variant-a")]
                Bridge::Direct { handler, locals } => direct(&handler, &locals, path, body).await,
            };
            answer.map(tonic::Response::new)
        })
    }
}

#[derive(Clone)]
struct ServerStream {
    queue: Arc<Queue>,
    path: String,
}

impl ServerStreamingService<Bytes> for ServerStream {
    type Response = Bytes;
    type ResponseStream = ReceiverStream<Answer>;
    type Future = std::future::Ready<Result<tonic::Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        // Bounded: tonic pulls a message only when HTTP/2 can send it, so a slow client
        // fills this channel and `try_send` tells the handler to wait.
        let (sender, receiver) = mpsc::channel(self.queue.stream_capacity);
        let path = self.path.clone();
        self.queue
            .call(path, request.into_inner(), Reply::Stream(sender));
        std::future::ready(Ok(tonic::Response::new(ReceiverStream::new(receiver))))
    }
}

#[cfg(feature = "variant-a")]
async fn direct(
    handler: &Py<PyAny>,
    locals: &pyo3_async_runtimes::TaskLocals,
    path: String,
    body: Bytes,
) -> Answer {
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

/// The declared routes, through `bridge`.
#[derive(Clone)]
struct Router {
    bridge: Bridge,
    routes: Arc<HashMap<String, Kind>>,
}

impl tower::Service<http::Request<Body>> for Router {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let path = request.uri().path().to_owned();
        let bridge = self.bridge.clone();
        match (self.routes.get(&path).copied(), bridge) {
            (Some(Kind::Unary), bridge) => {
                let unary = Unary { bridge, path };
                Box::pin(async move { Ok(Grpc::new(BytesCodec).unary(unary, request).await) })
            }
            (Some(Kind::ServerStream), Bridge::Queue(queue)) => {
                let stream = ServerStream { queue, path };
                Box::pin(async move {
                    Ok(Grpc::new(BytesCodec)
                        .server_streaming(stream, request)
                        .await)
                })
            }
            #[cfg(feature = "variant-a")]
            (Some(Kind::ServerStream), Bridge::Direct { .. }) => {
                let status = Status::unimplemented("variant A serves unary calls only");
                Box::pin(async move { Ok(status.into_http()) })
            }
            (None, _) => {
                let status = Status::unimplemented(format!("no method {path}"));
                Box::pin(async move { Ok(status.into_http()) })
            }
        }
    }
}

/// A call waiting for its answer (variant B): one `reply` or `fail` for a unary call;
/// `try_send`... then `finish` or `fail` for a server stream.
#[pyclass(module = "fastgrpc3_transport")]
struct Call {
    /// Unique per server, for matching `Server.take()`'s writable notices.
    #[pyo3(get)]
    id: u64,
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    body: Py<PyBytes>,
    reply: Option<Reply>,
    queue: Arc<Queue>,
    runtime: tokio::runtime::Handle,
}

#[pymethods]
impl Call {
    /// Whether the client expects a stream of responses.
    #[getter]
    fn streaming(&self) -> bool {
        matches!(self.reply, Some(Reply::Stream(_)))
    }

    /// Answer a unary call with the response bytes.
    fn reply(&mut self, data: &[u8]) -> PyResult<()> {
        match self.reply.take() {
            Some(Reply::Unary(sender)) => {
                let _ = sender.send(Ok(Bytes::copy_from_slice(data)));
                Ok(())
            }
            Some(stream @ Reply::Stream(_)) => {
                self.reply = Some(stream);
                Err(PyValueError::new_err(
                    "reply() is for unary calls: use try_send",
                ))
            }
            None => Ok(()), // already answered: the first answer stands
        }
    }

    /// Queue one message of a server stream: `0` sent, `1` full (call `when_writable`,
    /// wait for this call's id in `take()`, then try again), `2` closed (the client is
    /// gone: stop producing).
    fn try_send(&mut self, data: &[u8]) -> PyResult<u8> {
        let Some(Reply::Stream(sender)) = &self.reply else {
            return Ok(CLOSED);
        };
        Ok(match sender.try_send(Ok(Bytes::copy_from_slice(data))) {
            Ok(()) => SENT,
            Err(mpsc::error::TrySendError::Full(_)) => FULL,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.reply = None;
                CLOSED
            }
        })
    }

    /// Ask for this call's id in a later `take()` once its stream has room (or is closed).
    fn when_writable(&self) {
        let Some(Reply::Stream(sender)) = &self.reply else {
            self.queue.push(Event::Writable(self.id));
            return;
        };
        let (sender, queue, id) = (sender.clone(), self.queue.clone(), self.id);
        self.runtime.spawn(async move {
            // The permit is dropped at once: only this call's handler sends, so the room
            // stays until its next `try_send`.
            let _ = sender.reserve().await;
            queue.push(Event::Writable(id));
        });
    }

    /// End a server stream successfully.
    fn finish(&mut self) {
        self.reply = None;
    }

    /// Fail with a gRPC status code and message (a stream ends with it after the messages
    /// already queued).
    fn fail(&mut self, code: i32, message: &str) {
        let status = Status::new(Code::from_i32(code), message);
        match self.reply.take() {
            Some(Reply::Unary(sender)) => {
                let _ = sender.send(Err(status));
            }
            Some(Reply::Stream(sender)) => {
                self.runtime.spawn(async move {
                    let _ = sender.send(Err(status)).await;
                });
            }
            None => {}
        }
    }
}

/// A tonic server running on its own tokio thread(s).
#[pyclass(module = "fastgrpc3_transport")]
struct Server {
    threads: usize,
    stream_capacity: usize,
    routes: HashMap<String, Kind>,
    queue: Option<Arc<Queue>>,
    runtime: Option<tokio::runtime::Handle>,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

#[pymethods]
impl Server {
    #[new]
    #[pyo3(signature = (threads = 1, stream_capacity = 16))]
    fn new(threads: usize, stream_capacity: usize) -> PyResult<Self> {
        if stream_capacity == 0 {
            return Err(PyValueError::new_err("stream_capacity must be at least 1"));
        }
        Ok(Self {
            threads,
            stream_capacity,
            routes: HashMap::new(),
            queue: None,
            runtime: None,
            shutdown: None,
            thread: None,
        })
    }

    /// Serve `path` (`/package.Service/Method`) as `"unary"` or `"server_stream"`.
    fn add_route(&mut self, path: String, kind: &str) -> PyResult<()> {
        let kind = match kind {
            "unary" => Kind::Unary,
            "server_stream" => Kind::ServerStream,
            other => return Err(PyValueError::new_err(format!("unknown call kind {other}"))),
        };
        self.routes.insert(path, kind);
        Ok(())
    }

    #[cfg(feature = "variant-a")]
    /// Variant A (benchmarks only): `handler(path, body)` returns a coroutine run on
    /// `event_loop`. Returns the bound port.
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

    /// Variant B: returns `(port, fd)`; the loop reads `fd` when events are waiting, then
    /// calls `take()`.
    fn start_queue(&mut self, port: u16) -> PyResult<(u16, i32)> {
        let (python_end, waker) = UnixStream::pair()?;
        python_end.set_nonblocking(true)?;
        waker.set_nonblocking(true)?;
        let queue = Arc::new(Queue {
            events: Mutex::new(Vec::new()),
            waker,
            next_id: AtomicU64::new(1),
            stream_capacity: self.stream_capacity,
        });
        self.queue = Some(queue.clone());
        let bound = self.start(Bridge::Queue(queue), port)?;
        Ok((bound, python_end.into_raw_fd()))
    }

    /// The new calls, and the ids of the streams writable again, since the last `take`.
    fn take(&self, py: Python<'_>) -> PyResult<(Vec<Call>, Vec<u64>)> {
        let (Some(queue), Some(runtime)) = (&self.queue, &self.runtime) else {
            return Err(PyRuntimeError::new_err("not a started queue server"));
        };
        let events = std::mem::take(&mut *queue.events.lock().expect("queue"));
        let (mut calls, mut writable) = (Vec::new(), Vec::new());
        for event in events {
            match event {
                Event::Call(call) => calls.push(Call {
                    id: call.id,
                    path: call.path,
                    body: PyBytes::new(py, &call.body).unbind(),
                    reply: Some(call.reply),
                    queue: queue.clone(),
                    runtime: runtime.clone(),
                }),
                Event::Writable(id) => writable.push(id),
            }
        }
        Ok((calls, writable))
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
        let (handle_sender, handle_receiver) = std::sync::mpsc::channel();
        let threads = self.threads;
        let routes = Arc::new(self.routes.clone());
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
                let _ = handle_sender.send(runtime.handle().clone());
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
                    let service = Router { bridge, routes }.map_request(
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
        self.runtime = Some(
            handle_receiver
                .recv()
                .map_err(|_| PyRuntimeError::new_err("the tokio runtime did not start"))?,
        );
        self.shutdown = Some(shutdown);
        self.thread = Some(thread);
        Ok(bound)
    }
}

#[pymodule]
fn fastgrpc3_transport(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Server>()?;
    module.add_class::<Call>()?;
    module.add("SENT", SENT)?;
    module.add("FULL", FULL)?;
    module.add("CLOSED", CLOSED)?;
    Ok(())
}
