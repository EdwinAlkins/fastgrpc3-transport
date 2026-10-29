//! Step 1: a pure-Rust gRPC server that answers every unary call with its own request
//! bytes, on any path. The ceiling of the transport, with no Python involved.

use std::convert::Infallible;
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes};
use tonic::Status;
use tonic::body::Body;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::{Grpc, UnaryService};

/// Passes message bodies through as `Bytes`: decoding stays with the caller.
#[derive(Clone, Copy, Debug, Default)]
pub struct BytesCodec;

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesCodec;
    type Decoder = BytesCodec;

    fn encoder(&mut self) -> Self::Encoder {
        BytesCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        BytesCodec
    }
}

impl Encoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

#[derive(Clone, Copy)]
struct Echo;

impl UnaryService<Bytes> for Echo {
    type Response = Bytes;
    type Future = std::future::Ready<Result<tonic::Response<Bytes>, Status>>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        std::future::ready(Ok(tonic::Response::new(request.into_inner())))
    }
}

/// Any path, unary echo.
#[derive(Clone, Copy, Default)]
pub struct EchoService;

impl tower::Service<http::Request<Body>> for EchoService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Infallible>> + Send>,
    >;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        Box::pin(async move { Ok(Grpc::new(BytesCodec).unary(Echo, request).await) })
    }
}
