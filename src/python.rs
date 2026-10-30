//! tonic serving calls whose handlers are Python coroutines.
//!
//! - `start_queue` (variant B, the transport's design): tokio threads push events into a
//!   queue without the GIL and wake the loop through a socket only when the queue was empty;
//!   Python drains it by batches (`take`). Events are new calls, answered through their
//!   `Call` (`reply`, `try_send` / `finish`, `fail`), "writable again" notices for
//!   response streams whose bounded channel was full (`when_writable`), and the messages
//!   of request streams (one per credit, given back by `consumed`).
//! - `start_direct` (variant A, feature `variant-a`, benchmarks only, unary only): each call
//!   takes the GIL on a tokio thread and schedules its coroutine on the asyncio loop
//!   (`pyo3-async-runtimes`), one wake-up per call.
//!
//! Routes are declared before starting (`add_route`); any other path is `UNIMPLEMENTED`.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::io::Write;
use std::os::fd::IntoRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body;
use tonic::metadata::{KeyAndValueRef, MetadataMap};
use tonic::server::{
    ClientStreamingService, Grpc, ServerStreamingService, StreamingService, UnaryService,
};
use tonic::{Code, Status, Streaming};
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
    ClientStream,
    BidiStream,
}

/// Where a call's answer goes.
enum Reply {
    Unary(oneshot::Sender<Answer>),
    Stream(mpsc::Sender<Answer>),
}

struct Pending {
    id: u64,
    path: String,
    /// Converted for Python only if it asks (`Call.metadata()`).
    metadata: MetadataMap,
    body: Bytes,
    reply: Reply,
    requests: Option<Requests>,
}

/// The request stream of a call: its feeder task reads one message per credit, so a
/// handler that reads slowly slows the client down (HTTP/2 flow control), not memory.
struct Requests {
    credits: Arc<Semaphore>,
    feeder: tokio::task::AbortHandle,
}

/// What tokio hands to Python.
enum Event {
    Call(Pending),
    /// The response channel of stream `id` has room again (or was closed).
    Writable(u64),
    /// A request message of call `id`; `None` once the stream ends, cleanly (`true`) or
    /// not (`false`: reset by the client, malformed).
    Request(u64, Option<Bytes>, bool),
}

/// Events waiting for Python, and the socket that wakes the loop.
struct Queue {
    events: Mutex<VecDeque<Event>>,
    waker: UnixStream,
    next_id: AtomicU64,
    /// Messages a server stream may have queued for tonic before `try_send` says `FULL`.
    stream_capacity: usize,
    /// One permit per RPC alive on the tokio side: bounds the queue, the calls in Python
    /// and their tasks at once.
    admission: Arc<Semaphore>,
    /// How long a call waits for a permit before `RESOURCE_EXHAUSTED` (zero: not at all).
    admission_timeout: Duration,
}

impl Queue {
    /// A permit for one more RPC, or `RESOURCE_EXHAUSTED`. Never blocks a thread.
    async fn admit(&self) -> Result<OwnedSemaphorePermit, Status> {
        let exhausted = || Status::resource_exhausted("too many concurrent calls");
        if let Ok(permit) = self.admission.clone().try_acquire_owned() {
            return Ok(permit);
        }
        if self.admission_timeout.is_zero() {
            return Err(exhausted());
        }
        match tokio::time::timeout(
            self.admission_timeout,
            self.admission.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(exhausted()),
        }
    }

    fn push(&self, event: Event) {
        let was_empty = {
            let mut events = self.events.lock().expect("queue");
            events.push_back(event);
            events.len() == 1
        };
        if was_empty {
            // Full socket buffer: the loop has wake-ups pending anyway.
            let _ = (&self.waker).write(&[1]);
        }
    }

    fn call(&self, path: String, metadata: MetadataMap, body: Bytes, reply: Reply) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.push(Event::Call(Pending {
            id,
            path,
            metadata,
            body,
            reply,
            requests: None,
        }));
    }
}

/// A call whose requests are a stream: the call first, then its messages as Python
/// consumes them.
fn call_streaming(
    queue: &Arc<Queue>,
    path: String,
    request: tonic::Request<Streaming<Bytes>>,
    reply: Reply,
) {
    let (metadata, _, requests) = request.into_parts();
    let id = queue.next_id.fetch_add(1, Ordering::Relaxed);
    // No credit until the call is queued: its messages must not overtake it.
    let credits = Arc::new(Semaphore::new(0));
    let feeder = tokio::spawn(feed(queue.clone(), id, requests, credits.clone())).abort_handle();
    queue.push(Event::Call(Pending {
        id,
        path,
        metadata,
        body: Bytes::new(),
        reply,
        requests: Some(Requests {
            credits: credits.clone(),
            feeder,
        }),
    }));
    credits.add_permits(queue.stream_capacity);
}

async fn feed(queue: Arc<Queue>, id: u64, mut requests: Streaming<Bytes>, credits: Arc<Semaphore>) {
    loop {
        match credits.acquire().await {
            Ok(permit) => permit.forget(),
            Err(_) => return,
        }
        let (message, ok) = match requests.message().await {
            Ok(Some(data)) => (Some(data), true),
            Ok(None) => (None, true),
            Err(_) => (None, false),
        };
        let end = message.is_none();
        queue.push(Event::Request(id, message, ok));
        if end {
            return;
        }
    }
}

async fn answer(receiver: oneshot::Receiver<Answer>) -> Answer {
    receiver
        .await
        // The `Call` was freed unanswered: a handler bug, not the client.
        .unwrap_or_else(|_| Err(Status::internal("the handler did not answer")))
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
        let (metadata, _, body) = request.into_parts();
        Box::pin(async move {
            let answer = match bridge {
                Bridge::Queue(queue) => {
                    let (sender, receiver) = oneshot::channel();
                    queue.call(path, metadata, body, Reply::Unary(sender));
                    answer(receiver).await
                }
                #[cfg(feature = "variant-a")]
                Bridge::Direct { handler, locals } => direct(&handler, &locals, path, body).await,
            };
            answer.map(tonic::Response::new)
        })
    }
}

#[derive(Clone)]
struct ClientStream {
    queue: Arc<Queue>,
    path: String,
}

impl ClientStreamingService<Bytes> for ClientStream {
    type Response = Bytes;
    type Future = BoxFuture<Result<tonic::Response<Bytes>, Status>>;

    fn call(&mut self, request: tonic::Request<Streaming<Bytes>>) -> Self::Future {
        let (sender, receiver) = oneshot::channel();
        call_streaming(
            &self.queue,
            self.path.clone(),
            request,
            Reply::Unary(sender),
        );
        Box::pin(async move { answer(receiver).await.map(tonic::Response::new) })
    }
}

/// A server or bidirectional stream.
struct ResponseStream {
    queue: Arc<Queue>,
    path: String,
    permit: Option<OwnedSemaphorePermit>,
}

type StreamFuture = std::future::Ready<Result<tonic::Response<Responses>, Status>>;

impl ResponseStream {
    fn respond(&mut self, start: impl FnOnce(Reply)) -> StreamFuture {
        // Bounded: tonic pulls a message only when HTTP/2 can send it, so a slow client
        // fills this channel and `try_send` tells the handler to wait.
        let (sender, receiver) = mpsc::channel(self.queue.stream_capacity);
        start(Reply::Stream(sender));
        let responses = Responses {
            inner: ReceiverStream::new(receiver),
            _permit: self.permit.take(),
        };
        std::future::ready(Ok(tonic::Response::new(responses)))
    }
}

impl ServerStreamingService<Bytes> for ResponseStream {
    type Response = Bytes;
    type ResponseStream = Responses;
    type Future = StreamFuture;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let (queue, path) = (self.queue.clone(), self.path.clone());
        let (metadata, _, body) = request.into_parts();
        self.respond(|reply| queue.call(path, metadata, body, reply))
    }
}

impl StreamingService<Bytes> for ResponseStream {
    type Response = Bytes;
    type ResponseStream = Responses;
    type Future = StreamFuture;

    fn call(&mut self, request: tonic::Request<Streaming<Bytes>>) -> Self::Future {
        let (queue, path) = (self.queue.clone(), self.path.clone());
        self.respond(|reply| call_streaming(&queue, path, request, reply))
    }
}

/// A response stream; the RPC's admission permit lives as long as it does.
struct Responses {
    inner: ReceiverStream<Answer>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl tokio_stream::Stream for Responses {
    type Item = Answer;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Answer>> {
        std::pin::Pin::new(&mut self.inner).poll_next(context)
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
            (Some(kind), Bridge::Queue(queue)) => Box::pin(async move {
                // Held until the answer (unary, client stream) or by the responses.
                let permit = match queue.admit().await {
                    Ok(permit) => permit,
                    Err(status) => return Ok(status.into_http()),
                };
                let mut grpc = Grpc::new(BytesCodec);
                Ok(match kind {
                    Kind::Unary => {
                        let _permit = permit;
                        let bridge = Bridge::Queue(queue);
                        grpc.unary(Unary { bridge, path }, request).await
                    }
                    Kind::ClientStream => {
                        let _permit = permit;
                        grpc.client_streaming(ClientStream { queue, path }, request)
                            .await
                    }
                    Kind::ServerStream | Kind::BidiStream => {
                        let stream = ResponseStream {
                            queue,
                            path,
                            permit: Some(permit),
                        };
                        if kind == Kind::ServerStream {
                            grpc.server_streaming(stream, request).await
                        } else {
                            grpc.streaming(stream, request).await
                        }
                    }
                })
            }),
            #[cfg(feature = "variant-a")]
            (Some(Kind::Unary), bridge @ Bridge::Direct { .. }) => {
                let unary = Unary { bridge, path };
                Box::pin(async move { Ok(Grpc::new(BytesCodec).unary(unary, request).await) })
            }
            #[cfg(feature = "variant-a")]
            (Some(_), Bridge::Direct { .. }) => {
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

/// A call waiting for its answer (variant B): one `reply` or `fail` for a unary or client
/// stream call; `try_send`... then `finish` or `fail` for a server or bidirectional stream.
/// A request stream's messages come through `Server.take()`; call `consumed` for each.
#[pyclass(module = "fastgrpc3_transport")]
struct Call {
    /// Unique per server, for matching `Server.take()`'s writable notices.
    #[pyo3(get)]
    id: u64,
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    body: Py<PyBytes>,
    metadata: MetadataMap,
    reply: Option<Reply>,
    requests: Option<Requests>,
    queue: Arc<Queue>,
    runtime: tokio::runtime::Handle,
}

impl Drop for Call {
    /// A stream left open by its handler must not end as a success: dropping the sender
    /// alone would end it with `OK`. The request stream is no longer read.
    fn drop(&mut self) {
        if let Some(requests) = &self.requests {
            requests.feeder.abort();
        }
        if let Some(Reply::Stream(sender)) = self.reply.take() {
            let status = Status::internal("the handler did not finish the stream");
            self.runtime.spawn(async move {
                let _ = sender.send(Err(status)).await;
            });
        }
    }
}

#[pymethods]
impl Call {
    /// Whether the client expects a stream of responses.
    #[getter]
    fn streaming(&self) -> bool {
        matches!(self.reply, Some(Reply::Stream(_)))
    }

    /// The request metadata, as `(key, str | bytes)` pairs (`-bin` keys: bytes), without
    /// the protocol's own headers (`te`, `content-type`, `grpc-*`).
    fn metadata<'py>(&self, py: Python<'py>) -> Vec<(String, Bound<'py, PyAny>)> {
        self.metadata
            .iter()
            .filter_map(|entry| match entry {
                KeyAndValueRef::Ascii(key, value) => {
                    let key = key.as_str();
                    let reserved = key == "te" || key == "content-type" || key.starts_with("grpc-");
                    let value = value.to_str().ok().filter(|_| !reserved)?;
                    Some((key.to_owned(), PyString::new(py, value).into_any()))
                }
                KeyAndValueRef::Binary(key, value) => {
                    let value = value.to_bytes().ok()?;
                    Some((key.as_str().to_owned(), PyBytes::new(py, &value).into_any()))
                }
            })
            .collect()
    }

    /// One request message handled: the feeder may read one more.
    fn consumed(&self) {
        if let Some(requests) = &self.requests {
            requests.credits.add_permits(1);
        }
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

    /// Fail with a gRPC status code, message and optional `google.rpc.Status` details (a
    /// stream ends with it after the messages already queued).
    #[pyo3(signature = (code, message, details = None))]
    fn fail(&mut self, code: i32, message: &str, details: Option<&[u8]>) {
        let code = Code::from_i32(code);
        let status = match details {
            Some(details) => Status::with_details(code, message, Bytes::copy_from_slice(details)),
            None => Status::new(code, message),
        };
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
    max_concurrent_rpcs: usize,
    admission_timeout: Duration,
    routes: HashMap<String, Kind>,
    queue: Option<Arc<Queue>>,
    runtime: Option<tokio::runtime::Handle>,
    /// Starts the graceful stop, with its grace period.
    shutdown: Option<oneshot::Sender<Duration>>,
    thread: Option<JoinHandle<()>>,
}

#[pymethods]
impl Server {
    #[new]
    #[pyo3(signature = (
        threads = 1,
        stream_capacity = 16,
        max_concurrent_rpcs = 1024,
        admission_timeout = 0.0,
    ))]
    fn new(
        threads: usize,
        stream_capacity: usize,
        max_concurrent_rpcs: usize,
        admission_timeout: f64,
    ) -> PyResult<Self> {
        if stream_capacity == 0 || max_concurrent_rpcs == 0 {
            return Err(PyValueError::new_err(
                "stream_capacity and max_concurrent_rpcs must be at least 1",
            ));
        }
        let admission_timeout = Duration::try_from_secs_f64(admission_timeout)
            .map_err(|_| PyValueError::new_err("admission_timeout must be a duration >= 0"))?;
        Ok(Self {
            threads,
            stream_capacity,
            max_concurrent_rpcs,
            admission_timeout,
            routes: HashMap::new(),
            queue: None,
            runtime: None,
            shutdown: None,
            thread: None,
        })
    }

    /// Serve `path` (`/package.Service/Method`) as `"unary"`, `"server_stream"`,
    /// `"client_stream"` or `"bidi_stream"`.
    fn add_route(&mut self, path: String, kind: &str) -> PyResult<()> {
        let kind = match kind {
            "unary" => Kind::Unary,
            "server_stream" => Kind::ServerStream,
            "client_stream" => Kind::ClientStream,
            "bidi_stream" => Kind::BidiStream,
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
        self.start(bridge, "127.0.0.1", port)
    }

    /// Variant B: listens on `host:port`, returns `(port, fd)`; the loop reads `fd` when
    /// events are waiting, then calls `take()`.
    #[pyo3(signature = (port, host = "127.0.0.1"))]
    fn start_queue(&mut self, port: u16, host: &str) -> PyResult<(u16, i32)> {
        let (python_end, waker) = UnixStream::pair()?;
        python_end.set_nonblocking(true)?;
        waker.set_nonblocking(true)?;
        let queue = Arc::new(Queue {
            events: Mutex::new(VecDeque::new()),
            waker,
            next_id: AtomicU64::new(1),
            stream_capacity: self.stream_capacity,
            admission: Arc::new(Semaphore::new(self.max_concurrent_rpcs)),
            admission_timeout: self.admission_timeout,
        });
        self.queue = Some(queue.clone());
        let bound = self.start(Bridge::Queue(queue), host, port)?;
        Ok((bound, python_end.into_raw_fd()))
    }

    /// At most `max_events` of the waiting events, oldest first: the new calls, the ids of
    /// the streams writable again, the request messages `(id, bytes, True)` and request
    /// stream ends `(id, None, clean)`, and whether events remain. When some remain, call
    /// `take` again without waiting for the socket: it is written only when the queue goes
    /// from empty to non-empty. A call's messages always come after the call.
    #[pyo3(signature = (max_events = usize::MAX))]
    fn take(&self, py: Python<'_>, max_events: usize) -> PyResult<Taken> {
        let (Some(queue), Some(runtime)) = (&self.queue, &self.runtime) else {
            return Err(PyRuntimeError::new_err("not a started queue server"));
        };
        let (events, more): (Vec<Event>, bool) = {
            let mut waiting = queue.events.lock().expect("queue");
            let count = waiting.len().min(max_events.max(1));
            (waiting.drain(..count).collect(), !waiting.is_empty())
        };
        let (mut calls, mut writable, mut requests) = (Vec::new(), Vec::new(), Vec::new());
        for event in events {
            match event {
                Event::Call(call) => calls.push(Call {
                    id: call.id,
                    path: call.path,
                    body: PyBytes::new(py, &call.body).unbind(),
                    metadata: call.metadata,
                    reply: Some(call.reply),
                    requests: call.requests,
                    queue: queue.clone(),
                    runtime: runtime.clone(),
                }),
                Event::Writable(id) => writable.push(id),
                Event::Request(id, message, ok) => {
                    let message = message.map(|data| PyBytes::new(py, &data).unbind());
                    requests.push((id, message, ok));
                }
            }
        }
        Ok((calls, writable, requests, more))
    }

    /// Start stopping, without blocking: refuse new calls, let the calls in flight finish
    /// for `grace` seconds, then close the connections left and stop tokio. Keep the
    /// asyncio loop running meanwhile (handlers may still be answering), poll `finished()`,
    /// then `join()`.
    #[pyo3(signature = (grace = 0.0))]
    fn shutdown(&mut self, grace: f64) -> PyResult<()> {
        let grace = Duration::try_from_secs_f64(grace)
            .map_err(|_| PyValueError::new_err("grace must be a duration >= 0"))?;
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(grace);
        }
        Ok(())
    }

    /// Whether the server thread has ended (after `shutdown`).
    fn finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Wait for the server thread (bounded once `shutdown` was called).
    fn join(&mut self, py: Python<'_>) {
        if let Some(thread) = self.thread.take() {
            py.detach(|| {
                let _ = thread.join();
            });
        }
    }

    /// `shutdown(grace)` then `join()`, blocking the calling thread: for scripts and
    /// variant A. An asyncio application calls `shutdown` and waits for `finished()`.
    #[pyo3(signature = (grace = 0.0))]
    fn stop(&mut self, py: Python<'_>, grace: f64) -> PyResult<()> {
        self.shutdown(grace)?;
        self.join(py);
        Ok(())
    }
}

type Taken = (
    Vec<Call>,
    Vec<u64>,
    Vec<(u64, Option<Py<PyBytes>>, bool)>,
    bool,
);

impl Server {
    fn start(&mut self, bridge: Bridge, host: &str, port: u16) -> PyResult<u16> {
        let listener = std::net::TcpListener::bind((host, port))?;
        listener.set_nonblocking(true)?;
        let bound = listener.local_addr()?.port();
        let (shutdown, stopped) = oneshot::channel::<Duration>();
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
                    let (graceful, drain) = oneshot::channel::<()>();
                    let serve = tonic::transport::Server::builder()
                        .add_routes(routes)
                        .serve_with_incoming_shutdown(incoming, async {
                            let _ = drain.await;
                        });
                    tokio::pin!(serve);
                    let grace = tokio::select! {
                        _ = &mut serve => return,
                        grace = stopped => grace.unwrap_or(Duration::ZERO),
                    };
                    // tonic stops accepting and lets the calls in flight finish, for at most
                    // `grace`; dropping `serve` then closes the connections left.
                    let _ = graceful.send(());
                    let _ = tokio::time::timeout(grace, &mut serve).await;
                });
                // Tasks still running (a stream's `when_writable`, `fail`) get a moment.
                runtime.shutdown_timeout(Duration::from_secs(1));
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
