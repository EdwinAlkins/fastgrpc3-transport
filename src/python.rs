//! tonic serving calls whose handlers are Python coroutines.
//!
//! - `start_queue` (variant B, the transport's design): tokio threads push events into a
//!   queue without the GIL and wake the loop through a socket only when the queue was empty;
//!   Python drains it by batches (`take`). Events are new calls, answered through their
//!   `Call` (`reply`, `try_send` / `finish`, `fail`), "writable again" notices for
//!   response streams whose bounded channel was full (`when_writable`), the messages of
//!   request streams (one per credit, given back by `consumed`), and the calls tonic gave up
//!   before their end (client gone, deadline, closed connection, stop): Python cancels
//!   their task.
//! - `start_direct` (variant A, feature `variant-a`, benchmarks only, unary only): each call
//!   takes the GIL on a tokio thread and schedules its coroutine on the asyncio loop
//!   (`pyo3-async-runtimes`), one wake-up per call.
//!
//! Routes are declared before starting (`add_route`); any other path is `UNIMPLEMENTED`.
//! Response headers go out with the answer of a unary call, and with the first message,
//! `send_initial_metadata` or the end of a stream, as gRPC does; trailing metadata is added
//! to the status trailers.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body;
use tonic::codec::CompressionEncoding;
use tonic::metadata::{
    AsciiMetadataKey, BinaryMetadataKey, KeyAndValueRef, MetadataMap, MetadataValue,
};
use tonic::server::{
    ClientStreamingService, Grpc, ServerStreamingService, StreamingService, UnaryService,
};
use tonic::transport::{CertificateDer, ServerTlsConfig};
use tonic::{Code, Status, Streaming};
use tower::ServiceExt;

use crate::echo::BytesCodec;
use crate::{peer, wake};

type Answer = Result<Bytes, Status>;
/// A unary answer: the response and its headers (and `Trailers` in its extensions).
type UnaryAnswer = Result<tonic::Response<Bytes>, Status>;
type StreamFuture = BoxFuture<Result<tonic::Response<Responses>, Status>>;
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
    Unary(oneshot::Sender<UnaryAnswer>),
    Stream(mpsc::Sender<Answer>, StreamEnds),
}

/// When a response stream sends its headers, and the trailers of its successful end.
struct StreamEnds {
    headers: Option<oneshot::Sender<MetadataMap>>,
    trailers: Arc<Mutex<Option<MetadataMap>>>,
}

/// The trailing metadata of a successful unary answer, carried in its extensions.
#[derive(Clone)]
struct Trailers(MetadataMap);

/// Who called: the address, and the certificates of an mTLS client.
#[derive(Default)]
struct Peer {
    address: Option<SocketAddr>,
    certificates: Option<Arc<Vec<CertificateDer<'static>>>>,
}

impl Peer {
    fn of<T>(request: &tonic::Request<T>) -> Self {
        Self {
            address: request.remote_addr(),
            certificates: request.peer_certs(),
        }
    }

    /// The client's leaf certificate, if it presented one.
    fn certificate(&self) -> Option<&[u8]> {
        self.certificates.as_ref()?.first().map(|der| der.as_ref())
    }
}

struct Pending {
    id: u64,
    path: String,
    peer: Peer,
    /// Converted for Python only if it asks (`Call.metadata()`).
    metadata: MetadataMap,
    deadline: Option<Instant>,
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
// Not boxed: that would cost an allocation per call; the queue holds few events at once.
#[allow(clippy::large_enum_variant)]
enum Event {
    Call(Pending),
    /// The response channel of stream `id` has room again (or was closed).
    Writable(u64),
    /// A request message of call `id`; `None` once the stream ends: cleanly (`0`), reset
    /// by the client or malformed (`CANCELLED`), or with a message over the size limit
    /// (`RESOURCE_EXHAUSTED`).
    Request(u64, Option<Bytes>, i32),
    /// tonic dropped call `id` before its end: cancel its handler.
    Cancelled(u64),
}

/// Events waiting for Python, and the socket that wakes the loop.
struct Queue {
    events: Mutex<VecDeque<Event>>,
    waker: wake::Waker,
    next_id: AtomicU64,
    /// Messages a server stream may have queued for tonic before `try_send` says `FULL`.
    stream_capacity: usize,
    /// One permit per RPC alive on the tokio side: bounds the queue, the calls in Python
    /// and their tasks at once.
    admission: Arc<Semaphore>,
    /// How long a call waits for a permit before `RESOURCE_EXHAUSTED` (zero: not at all).
    admission_timeout: Duration,
    settings: Settings,
}

/// What fastgrpc3 configures (`Limits`, `Compression`, `Keepalive`, port sharing).
#[derive(Clone, Copy)]
struct Settings {
    max_recv_bytes: Option<usize>,
    max_send_bytes: Option<usize>,
    max_metadata_bytes: Option<u32>,
    compression: Option<CompressionEncoding>,
    keepalive: Option<(Duration, Duration)>,
    reuse_port: bool,
    tls: bool,
}

impl Settings {
    /// The codec of one call: requests in gzip or deflate are accepted, as `grpc` does.
    fn grpc(&self) -> Grpc<BytesCodec> {
        let mut grpc = Grpc::new(BytesCodec)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Deflate);
        if let Some(encoding) = self.compression {
            grpc = grpc.send_compressed(encoding); // if the client accepts it
        }
        if let Some(max) = self.max_recv_bytes {
            grpc = grpc.max_decoding_message_size(max);
        }
        grpc
    }

    /// `grpc`'s status for a message over `max_send_bytes` (tonic checks it too late, with
    /// `OUT_OF_RANGE`).
    fn check_send(&self, size: usize) -> Result<(), Status> {
        match self.max_send_bytes {
            Some(max) if size > max => Err(Status::resource_exhausted(format!(
                "Sent message larger than max ({size} vs. {max})"
            ))),
            _ => Ok(()),
        }
    }
}

/// tonic refuses a request over `max_recv_bytes` with `OUT_OF_RANGE`; `grpc` says
/// `RESOURCE_EXHAUSTED`.
fn too_large(status: &Status) -> bool {
    status.code() == Code::OutOfRange && status.message().contains("message length too large")
}

/// The same, on a response tonic built from such a status (trailers only).
fn too_large_response(mut response: http::Response<Body>) -> http::Response<Body> {
    let headers = response.headers_mut();
    let refused = headers.get("grpc-status").is_some_and(|code| code == "11")
        && headers
            .get("grpc-message")
            .and_then(|message| message.to_str().ok())
            .is_some_and(|message| message.contains("message%20length%20too%20large"));
    if refused {
        headers.insert("grpc-status", http::HeaderValue::from_static("8"));
    }
    response
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

    /// Queue a call; returns its id.
    fn call(
        &self,
        path: String,
        peer: Peer,
        metadata: MetadataMap,
        deadline: Option<Instant>,
        body: Bytes,
        reply: Reply,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.push(Event::Call(Pending {
            id,
            path,
            peer,
            metadata,
            deadline,
            body,
            reply,
            requests: None,
        }));
        id
    }
}

/// The local, monotonic deadline of a call from its `grpc-timeout` header. Read when tonic
/// hands the request over, just before it arms its own timer for the same header: ours
/// never ends later, and tonic polls the call before its timer, so the client gets our
/// `DEADLINE_EXCEEDED`, not tonic's `CANCELLED`.
fn deadline(headers: &http::HeaderMap) -> Option<Instant> {
    let value = headers.get("grpc-timeout")?.to_str().ok()?;
    let (amount, unit) = value.split_at(value.len().checked_sub(1)?);
    let amount: u64 = amount.parse().ok()?;
    let timeout = match unit {
        "H" => Duration::from_secs(amount.checked_mul(3600)?),
        "M" => Duration::from_secs(amount.checked_mul(60)?),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return None,
    };
    Instant::now().checked_add(timeout)
}

/// Tells Python when tonic drops a call before its end (client gone, deadline, closed
/// connection, stop), so that its handler is cancelled. Disarmed once the call has ended.
struct Watch {
    queue: Arc<Queue>,
    id: u64,
    armed: bool,
}

impl Watch {
    fn new(queue: Arc<Queue>, id: u64) -> Self {
        Self {
            queue,
            id,
            armed: true,
        }
    }

    /// A method, not a field write: an `async move` block then captures the whole guard.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        if self.armed {
            self.queue.push(Event::Cancelled(self.id));
        }
    }
}

/// A call whose requests are a stream: the call first, then its messages as Python
/// consumes them.
fn call_streaming(
    queue: &Arc<Queue>,
    path: String,
    deadline: Option<Instant>,
    request: tonic::Request<Streaming<Bytes>>,
    reply: Reply,
) -> u64 {
    let peer = Peer::of(&request);
    let (metadata, _, requests) = request.into_parts();
    let id = queue.next_id.fetch_add(1, Ordering::Relaxed);
    // No credit until the call is queued: its messages must not overtake it.
    let credits = Arc::new(Semaphore::new(0));
    let feeder = tokio::spawn(feed(queue.clone(), id, requests, credits.clone())).abort_handle();
    queue.push(Event::Call(Pending {
        id,
        path,
        peer,
        metadata,
        deadline,
        body: Bytes::new(),
        reply,
        requests: Some(Requests {
            credits: credits.clone(),
            feeder,
        }),
    }));
    credits.add_permits(queue.stream_capacity);
    id
}

async fn feed(queue: Arc<Queue>, id: u64, mut requests: Streaming<Bytes>, credits: Arc<Semaphore>) {
    loop {
        match credits.acquire().await {
            Ok(permit) => permit.forget(),
            Err(_) => return,
        }
        let (message, code) = match requests.message().await {
            Ok(Some(data)) => (Some(data), 0),
            Ok(None) => (None, 0),
            Err(status) if too_large(&status) => (None, Code::ResourceExhausted as i32),
            Err(_) => (None, Code::Cancelled as i32),
        };
        let end = message.is_none();
        queue.push(Event::Request(id, message, code));
        if end {
            return;
        }
    }
}

/// The handler's answer, or `DEADLINE_EXCEEDED`. The watch stays armed when the answer
/// does not come (deadline, or tonic dropping this future): the handler is cancelled.
async fn answer(
    receiver: oneshot::Receiver<UnaryAnswer>,
    deadline: Option<Instant>,
    mut watch: Watch,
) -> UnaryAnswer {
    let received = match deadline {
        Some(deadline) => match tokio::time::timeout_at(deadline.into(), receiver).await {
            Ok(received) => received,
            Err(_) => return Err(Status::deadline_exceeded("deadline exceeded")),
        },
        None => receiver.await,
    };
    watch.disarm();
    // The `Call` was freed unanswered: a handler bug, not the client.
    received.unwrap_or_else(|_| Err(Status::internal("the handler did not answer")))
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
    deadline: Option<Instant>,
}

impl UnaryService<Bytes> for Unary {
    type Response = Bytes;
    type Future = BoxFuture<Result<tonic::Response<Bytes>, Status>>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let (bridge, path, deadline) = (self.bridge.clone(), self.path.clone(), self.deadline);
        let peer = Peer::of(&request);
        let (metadata, _, body) = request.into_parts();
        Box::pin(async move {
            match bridge {
                Bridge::Queue(queue) => {
                    let (sender, receiver) = oneshot::channel();
                    let reply = Reply::Unary(sender);
                    let id = queue.call(path, peer, metadata, deadline, body, reply);
                    answer(receiver, deadline, Watch::new(queue, id)).await
                }
                #[cfg(feature = "variant-a")]
                Bridge::Direct { handler, locals } => direct(&handler, &locals, path, body)
                    .await
                    .map(tonic::Response::new),
            }
        })
    }
}

#[derive(Clone)]
struct ClientStream {
    queue: Arc<Queue>,
    path: String,
    deadline: Option<Instant>,
}

impl ClientStreamingService<Bytes> for ClientStream {
    type Response = Bytes;
    type Future = BoxFuture<Result<tonic::Response<Bytes>, Status>>;

    fn call(&mut self, request: tonic::Request<Streaming<Bytes>>) -> Self::Future {
        let (sender, receiver) = oneshot::channel();
        let (path, deadline) = (self.path.clone(), self.deadline);
        let id = call_streaming(&self.queue, path, deadline, request, Reply::Unary(sender));
        let watch = Watch::new(self.queue.clone(), id);
        Box::pin(answer(receiver, deadline, watch))
    }
}

/// A server or bidirectional stream.
struct ResponseStream {
    queue: Arc<Queue>,
    path: String,
    deadline: Option<Instant>,
    permit: Option<OwnedSemaphorePermit>,
    trailers: Arc<Mutex<Option<MetadataMap>>>,
}

impl ResponseStream {
    fn respond(&mut self, start: impl FnOnce(Reply) -> u64) -> StreamFuture {
        // Bounded: tonic pulls a message only when HTTP/2 can send it, so a slow client
        // fills this channel and `try_send` tells the handler to wait.
        let (sender, receiver) = mpsc::channel(self.queue.stream_capacity);
        let (headers, headers_received) = oneshot::channel();
        let ends = StreamEnds {
            headers: Some(headers),
            trailers: self.trailers.clone(),
        };
        let id = start(Reply::Stream(sender, ends));
        let responses = Responses {
            inner: ReceiverStream::new(receiver),
            deadline: self
                .deadline
                .map(|deadline| Box::pin(tokio::time::sleep_until(deadline.into()))),
            ended: false,
            watch: Watch::new(self.queue.clone(), id),
            _permit: self.permit.take(),
        };
        let deadline = self.deadline;
        Box::pin(async move {
            // Dropped meanwhile (client gone, deadline), `responses` cancels the handler.
            let headers = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline.into(), headers_received)
                    .await
                    .map_err(|_| Status::deadline_exceeded("deadline exceeded"))?,
                None => headers_received.await,
            };
            let headers = headers.unwrap_or_default();
            Ok(tonic::Response::from_parts(
                headers,
                responses,
                http::Extensions::new(),
            ))
        })
    }
}

impl ServerStreamingService<Bytes> for ResponseStream {
    type Response = Bytes;
    type ResponseStream = Responses;
    type Future = StreamFuture;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let (queue, path, deadline) = (self.queue.clone(), self.path.clone(), self.deadline);
        let peer = Peer::of(&request);
        let (metadata, _, body) = request.into_parts();
        self.respond(|reply| queue.call(path, peer, metadata, deadline, body, reply))
    }
}

impl StreamingService<Bytes> for ResponseStream {
    type Response = Bytes;
    type ResponseStream = Responses;
    type Future = StreamFuture;

    fn call(&mut self, request: tonic::Request<Streaming<Bytes>>) -> Self::Future {
        let (queue, path, deadline) = (self.queue.clone(), self.path.clone(), self.deadline);
        self.respond(|reply| call_streaming(&queue, path, deadline, request, reply))
    }
}

/// A response stream; the RPC's admission permit lives as long as it does.
struct Responses {
    inner: ReceiverStream<Answer>,
    /// Enforced here: tonic bounds the response headers by the deadline, not a stream.
    deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
    ended: bool,
    watch: Watch,
    _permit: Option<OwnedSemaphorePermit>,
}

impl tokio_stream::Stream for Responses {
    type Item = Answer;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Answer>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if let Some(deadline) = &mut self.deadline
            && deadline.as_mut().poll(context).is_ready()
        {
            // The watch stays armed: the handler is cancelled when tonic drops the stream.
            self.ended = true;
            return Poll::Ready(Some(Err(Status::deadline_exceeded("deadline exceeded"))));
        }
        let item = std::task::ready!(std::pin::Pin::new(&mut self.inner).poll_next(context));
        if !matches!(item, Some(Ok(_))) {
            self.ended = true;
            self.watch.armed = false;
        }
        Poll::Ready(item)
    }
}

/// A response body whose trailers also carry the handler's trailing metadata.
struct WithTrailers {
    inner: Body,
    extra: Arc<Mutex<Option<MetadataMap>>>,
}

impl http_body::Body for WithTrailers {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        let frame = std::task::ready!(std::pin::Pin::new(&mut self.inner).poll_frame(context));
        Poll::Ready(frame.map(|frame| {
            frame.map(|frame| match frame.into_trailers() {
                Ok(mut trailers) => {
                    if let Some(extra) = self.extra.lock().expect("trailers").take() {
                        trailers.extend(extra.into_headers());
                    }
                    http_body::Frame::trailers(trailers)
                }
                Err(frame) => frame,
            })
        }))
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn with_trailers(
    response: http::Response<Body>,
    extra: Arc<Mutex<Option<MetadataMap>>>,
) -> http::Response<Body> {
    response.map(|inner| Body::new(WithTrailers { inner, extra }))
}

/// A unary answer's `Trailers`, if any, added to its body's trailers.
fn unary_trailers(mut response: http::Response<Body>) -> http::Response<Body> {
    match response.extensions_mut().remove::<Trailers>() {
        Some(Trailers(trailers)) => with_trailers(response, Arc::new(Mutex::new(Some(trailers)))),
        None => response,
    }
}

/// Response metadata from Python's `(key, str | bytes)` pairs (`-bin` keys take bytes).
fn to_metadata(pairs: Vec<(String, Bound<'_, PyAny>)>) -> PyResult<MetadataMap> {
    let invalid = |key: &str| PyValueError::new_err(format!("invalid metadata {key:?}"));
    let mut metadata = MetadataMap::new();
    for (key, value) in pairs {
        if key.ends_with("-bin") {
            let name = BinaryMetadataKey::from_bytes(key.as_bytes()).map_err(|_| invalid(&key))?;
            let value = MetadataValue::from_bytes(value.cast::<PyBytes>()?.as_bytes());
            metadata.append_bin(name, value);
        } else {
            let name = AsciiMetadataKey::from_bytes(key.as_bytes()).map_err(|_| invalid(&key))?;
            let value = value.cast::<PyString>()?.to_str()?.parse();
            metadata.append(name, value.map_err(|_| invalid(&key))?);
        }
    }
    Ok(metadata)
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
    settings: Settings,
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
        let deadline = deadline(request.headers());
        let (bridge, settings) = (self.bridge.clone(), self.settings);
        match (self.routes.get(&path).copied(), bridge) {
            (Some(kind), Bridge::Queue(queue)) => Box::pin(async move {
                // Held until the answer (unary, client stream) or by the responses.
                let permit = match queue.admit().await {
                    Ok(permit) => permit,
                    Err(status) => return Ok(status.into_http()),
                };
                let mut grpc = settings.grpc();
                Ok(too_large_response(match kind {
                    Kind::Unary => {
                        let _permit = permit;
                        let bridge = Bridge::Queue(queue);
                        let unary = Unary {
                            bridge,
                            path,
                            deadline,
                        };
                        unary_trailers(grpc.unary(unary, request).await)
                    }
                    Kind::ClientStream => {
                        let _permit = permit;
                        let stream = ClientStream {
                            queue,
                            path,
                            deadline,
                        };
                        unary_trailers(grpc.client_streaming(stream, request).await)
                    }
                    Kind::ServerStream | Kind::BidiStream => {
                        let trailers = Arc::new(Mutex::new(None));
                        let stream = ResponseStream {
                            queue,
                            path,
                            deadline,
                            permit: Some(permit),
                            trailers: trailers.clone(),
                        };
                        let response = if kind == Kind::ServerStream {
                            grpc.server_streaming(stream, request).await
                        } else {
                            grpc.streaming(stream, request).await
                        };
                        with_trailers(response, trailers)
                    }
                }))
            }),
            #[cfg(feature = "variant-a")]
            (Some(Kind::Unary), bridge @ Bridge::Direct { .. }) => {
                let unary = Unary {
                    bridge,
                    path,
                    deadline,
                };
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
    peer: Peer,
    metadata: MetadataMap,
    deadline: Option<Instant>,
    /// Response headers and trailers set by the handler.
    initial: MetadataMap,
    trailers: MetadataMap,
    headers_sent: bool,
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
        if let Some(Reply::Stream(sender, _)) = self.reply.take() {
            let status = Status::internal("the handler did not finish the stream");
            self.runtime.spawn(async move {
                let _ = sender.send(Err(status)).await;
            });
        }
    }
}

impl Call {
    /// Let a response stream send its headers (the initial metadata, if any).
    fn release_headers(&mut self) {
        self.headers_sent = true;
        if let Some(Reply::Stream(_, ends)) = &mut self.reply
            && let Some(headers) = ends.headers.take()
        {
            let _ = headers.send(std::mem::take(&mut self.initial));
        }
    }
}

#[pymethods]
impl Call {
    /// Whether the client expects a stream of responses.
    #[getter]
    fn streaming(&self) -> bool {
        matches!(self.reply, Some(Reply::Stream(..)))
    }

    /// The client's address, as `grpc` writes it: `ipv4:127.0.0.1:5000`, `ipv6:[::1]:5000`.
    #[getter]
    fn peer(&self) -> String {
        match self.peer.address {
            Some(SocketAddr::V4(address)) => format!("ipv4:{address}"),
            // `grpc` escapes the brackets.
            Some(SocketAddr::V6(address)) => {
                format!("ipv6:%5B{}%5D:{}", address.ip(), address.port())
            }
            None => String::new(),
        }
    }

    /// The identities of an mTLS client (`grpc`'s: the certificate's subject alternative
    /// names, or its common name), `None` without a client certificate.
    fn peer_identities<'py>(&self, py: Python<'py>) -> Option<Vec<Bound<'py, PyBytes>>> {
        let identities = peer::identities(self.peer.certificate()?);
        Some(
            identities
                .iter()
                .map(|identity| PyBytes::new(py, identity))
                .collect(),
        )
    }

    /// `grpc`'s authentication context: empty in plaintext.
    fn auth_context<'py>(
        &self,
        py: Python<'py>,
    ) -> HashMap<&'static str, Vec<Bound<'py, PyBytes>>> {
        if !self.queue.settings.tls {
            return HashMap::new();
        }
        let context = peer::auth_context(self.peer.certificate());
        let bytes = |values: Vec<Vec<u8>>| values.iter().map(|v| PyBytes::new(py, v)).collect();
        context
            .into_iter()
            .map(|(key, values)| (key, bytes(values)))
            .collect()
    }

    /// Response headers: sent now by a stream, with the answer by a unary call.
    fn send_initial_metadata(&mut self, metadata: Vec<(String, Bound<'_, PyAny>)>) -> PyResult<()> {
        if self.headers_sent {
            return Err(PyValueError::new_err(
                "the initial metadata was already sent",
            ));
        }
        self.initial = to_metadata(metadata)?;
        self.release_headers();
        Ok(())
    }

    /// Response trailers, sent with the status (replaces those set before).
    fn set_trailing_metadata(&mut self, metadata: Vec<(String, Bound<'_, PyAny>)>) -> PyResult<()> {
        self.trailers = to_metadata(metadata)?;
        Ok(())
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

    /// Seconds left before the call's deadline, `None` without one.
    fn time_remaining(&self) -> Option<f64> {
        self.deadline.map(|deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .as_secs_f64()
        })
    }

    /// One request message handled: the feeder may read one more.
    fn consumed(&self) {
        if let Some(requests) = &self.requests {
            requests.credits.add_permits(1);
        }
    }

    /// Answer a unary call with the response bytes.
    fn reply(&mut self, data: &[u8]) -> PyResult<()> {
        if let Err(status) = self.queue.settings.check_send(data.len()) {
            self.fail(status.code() as i32, status.message(), None);
            return Ok(());
        }
        match self.reply.take() {
            Some(Reply::Unary(sender)) => {
                let mut extensions = http::Extensions::new();
                if !self.trailers.is_empty() {
                    extensions.insert(Trailers(std::mem::take(&mut self.trailers)));
                }
                let initial = std::mem::take(&mut self.initial);
                let data = Bytes::copy_from_slice(data);
                let _ = sender.send(Ok(tonic::Response::from_parts(initial, data, extensions)));
                Ok(())
            }
            Some(stream @ Reply::Stream(..)) => {
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
        if !self.headers_sent {
            self.release_headers();
        }
        if let Err(status) = self.queue.settings.check_send(data.len()) {
            self.fail(status.code() as i32, status.message(), None);
            return Ok(CLOSED);
        }
        let Some(Reply::Stream(sender, _)) = &self.reply else {
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
        let Some(Reply::Stream(sender, _)) = &self.reply else {
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
        self.release_headers();
        if let Some(Reply::Stream(_, ends)) = &self.reply
            && !self.trailers.is_empty()
        {
            *ends.trailers.lock().expect("trailers") = Some(std::mem::take(&mut self.trailers));
        }
        self.reply = None;
    }

    /// Fail with a gRPC status code, message and optional `google.rpc.Status` details (a
    /// stream ends with it after the messages already queued).
    #[pyo3(signature = (code, message, details = None))]
    fn fail(&mut self, code: i32, message: &str, details: Option<&[u8]>) {
        let code = Code::from_i32(code);
        let mut metadata = std::mem::take(&mut self.trailers).into_headers();
        if matches!(self.reply, Some(Reply::Unary(_))) {
            // A failed unary call answers with trailers only: its headers go there too.
            metadata.extend(std::mem::take(&mut self.initial).into_headers());
        } else {
            self.release_headers();
        }
        let metadata = MetadataMap::from_headers(metadata);
        let status = match details {
            Some(details) => Status::with_details_and_metadata(
                code,
                message,
                Bytes::copy_from_slice(details),
                metadata,
            ),
            None => Status::with_metadata(code, message, metadata),
        };
        match self.reply.take() {
            Some(Reply::Unary(sender)) => {
                let _ = sender.send(Err(status));
            }
            Some(Reply::Stream(sender, _)) => {
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
    settings: Settings,
    tls: Option<ServerTlsConfig>,
    routes: HashMap<String, Kind>,
    queue: Option<Arc<Queue>>,
    runtime: Option<tokio::runtime::Handle>,
    /// Starts the graceful stop, with its grace period.
    shutdown: Option<oneshot::Sender<Duration>>,
    thread: Option<JoinHandle<()>>,
}

#[pymethods]
impl Server {
    /// `max_*_bytes`: `None` keeps tonic's defaults (4 MiB received, no send limit, 16 KiB
    /// of headers). `compression`: `"gzip"` or `"deflate"` for the responses, when the
    /// client accepts it. `keepalive`: `(interval, timeout)` of the HTTP/2 pings, in seconds.
    /// `reuse_port`: share the port with other processes (`SO_REUSEPORT`). `tls`:
    /// `(cert, key, client_ca, client_required)` in PEM; with `client_ca`, client certificates
    /// are verified (required if `client_required`).
    #[new]
    #[pyo3(signature = (
        threads = 1,
        stream_capacity = 16,
        max_concurrent_rpcs = 1024,
        admission_timeout = 0.0,
        *,
        max_recv_bytes = None,
        max_send_bytes = None,
        max_metadata_bytes = None,
        compression = None,
        keepalive = None,
        reuse_port = false,
        tls = None,
    ))]
    #[allow(clippy::too_many_arguments)] // Python keyword arguments
    fn new(
        threads: usize,
        stream_capacity: usize,
        max_concurrent_rpcs: usize,
        admission_timeout: f64,
        max_recv_bytes: Option<usize>,
        max_send_bytes: Option<usize>,
        max_metadata_bytes: Option<u32>,
        compression: Option<&str>,
        keepalive: Option<(f64, f64)>,
        reuse_port: bool,
        tls: Option<TlsFiles>,
    ) -> PyResult<Self> {
        if stream_capacity == 0 || max_concurrent_rpcs == 0 {
            return Err(PyValueError::new_err(
                "stream_capacity and max_concurrent_rpcs must be at least 1",
            ));
        }
        let duration = |seconds: f64, name: &str| {
            Duration::try_from_secs_f64(seconds)
                .map_err(|_| PyValueError::new_err(format!("{name} must be a duration >= 0")))
        };
        let admission_timeout = duration(admission_timeout, "admission_timeout")?;
        let compression = match compression {
            None => None,
            Some("gzip") => Some(CompressionEncoding::Gzip),
            Some("deflate") => Some(CompressionEncoding::Deflate),
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "unknown compression {other}"
                )));
            }
        };
        let keepalive = match keepalive {
            Some((interval, timeout)) => Some((
                duration(interval, "keepalive interval")?,
                duration(timeout, "keepalive timeout")?,
            )),
            None => None,
        };
        let tls = tls.map(|(cert, key, client_ca, client_required)| {
            let config = ServerTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(cert, key))
                // `grpc`'s default handshake timeout: a client that stalls is let go.
                .timeout(Duration::from_secs(120));
            match client_ca {
                Some(ca) => config
                    .client_ca_root(tonic::transport::Certificate::from_pem(ca))
                    .client_auth_optional(!client_required),
                None => config,
            }
        });
        if let Some(config) = &tls {
            // Invalid certificates or keys fail here, not on the server thread.
            tonic::transport::Server::builder()
                .tls_config(config.clone())
                .map_err(|e| PyValueError::new_err(format!("invalid TLS configuration: {e}")))?;
        }
        Ok(Self {
            threads,
            stream_capacity,
            max_concurrent_rpcs,
            admission_timeout,
            settings: Settings {
                max_recv_bytes,
                max_send_bytes,
                max_metadata_bytes,
                compression,
                keepalive,
                reuse_port,
                tls: tls.is_some(),
            },
            tls,
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

    /// Variant B: listens on `host:port`, returns `(port, socket)`; the loop reads
    /// `socket` (a descriptor, or a `SOCKET` on Windows) when events are waiting, then
    /// calls `take()`.
    #[pyo3(signature = (port, host = "127.0.0.1"))]
    fn start_queue(&mut self, port: u16, host: &str) -> PyResult<(u16, i64)> {
        let (waker, python_end) = wake::pair()?;
        let queue = Arc::new(Queue {
            events: Mutex::new(VecDeque::new()),
            waker,
            next_id: AtomicU64::new(1),
            stream_capacity: self.stream_capacity,
            admission: Arc::new(Semaphore::new(self.max_concurrent_rpcs)),
            admission_timeout: self.admission_timeout,
            settings: self.settings,
        });
        self.queue = Some(queue.clone());
        let bound = self.start(Bridge::Queue(queue), host, port)?;
        Ok((bound, python_end))
    }

    /// At most `max_events` of the waiting events, oldest first: the new calls, the ids of
    /// the streams writable again, the request messages `(id, bytes, 0)` and request
    /// stream ends `(id, None, code)` (`0`: clean), the ids of the calls tonic gave up (cancel their
    /// handler), and whether events remain. When some remain, call
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
        let mut cancelled = Vec::new();
        for event in events {
            match event {
                Event::Call(call) => calls.push(Call {
                    id: call.id,
                    path: call.path,
                    body: PyBytes::new(py, &call.body).unbind(),
                    peer: call.peer,
                    metadata: call.metadata,
                    deadline: call.deadline,
                    initial: MetadataMap::new(),
                    trailers: MetadataMap::new(),
                    headers_sent: false,
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
                Event::Cancelled(id) => cancelled.push(id),
            }
        }
        Ok((calls, writable, requests, cancelled, more))
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

/// `(cert, key, client_ca, client_required)`, in PEM.
type TlsFiles = (Vec<u8>, Vec<u8>, Option<Vec<u8>>, bool);

type Taken = (
    Vec<Call>,
    Vec<u64>,
    Vec<(u64, Option<Py<PyBytes>>, i32)>,
    Vec<u64>,
    bool,
);

impl Server {
    fn start(&mut self, bridge: Bridge, host: &str, port: u16) -> PyResult<u16> {
        let (settings, tls) = (self.settings, self.tls.clone());
        let listener = listen(host, port, settings.reuse_port)?;
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
                    let service = Router {
                        bridge,
                        routes,
                        settings,
                    }
                    .map_request(|request: http::Request<axum::body::Body>| request.map(Body::new));
                    let routes =
                        tonic::service::Routes::from(axum::Router::new().fallback_service(service));
                    let (graceful, drain) = oneshot::channel::<()>();
                    let mut server = tonic::transport::Server::builder()
                        .http2_max_header_list_size(settings.max_metadata_bytes);
                    if let Some(tls) = tls {
                        server = server.tls_config(tls).expect("checked by Server()");
                    }
                    if let Some((interval, timeout)) = settings.keepalive {
                        server = server
                            .http2_keepalive_interval(Some(interval))
                            .http2_keepalive_timeout(Some(timeout));
                    }
                    let serve =
                        server
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

/// A listening socket, shared with other processes on the same port if `reuse_port`.
fn listen(host: &str, port: u16, reuse_port: bool) -> std::io::Result<std::net::TcpListener> {
    if !reuse_port {
        return std::net::TcpListener::bind((host, port));
    }
    let address = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))?
        .next()
        .ok_or_else(|| std::io::Error::other(format!("no address for {host}")))?;
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    #[cfg(not(unix))]
    return Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "port sharing (SO_REUSEPORT) is not available on this platform",
    ));
    socket.bind(&address.into())?;
    socket.listen(1024)?;
    Ok(socket.into())
}

#[pymodule]
fn fastgrpc3_transport(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Server>()?;
    module.add_class::<Call>()?;
    module.add("SENT", SENT)?;
    module.add("FULL", FULL)?;
    module.add("CLOSED", CLOSED)?;
    // The integration contract with fastgrpc3: bump it when this module's API changes.
    module.add("TRANSPORT_API_VERSION", 1)?;
    Ok(())
}
