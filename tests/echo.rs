//! The step 1 echo server answers any path with the request bytes, through a real tonic
//! client.

use bytes::Bytes;
use fastgrpc3_transport::echo::{BytesCodec, EchoService};
use tower::ServiceExt;

async fn serve() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = EchoService.map_request(|request: http::Request<axum::body::Body>| {
        request.map(tonic::body::Body::new)
    });
    let routes = tonic::service::Routes::from(axum::Router::new().fallback_service(service));
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_routes(routes)
            .serve_with_incoming(incoming),
    );
    address
}

#[tokio::test]
async fn echoes_on_any_path() {
    let address = serve().await;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = tonic::client::Grpc::new(channel);
    for (path, size) in [("/a.v1.A/Get", 0), ("/b.B/Put", 9), ("/c.C/Big", 1 << 20)] {
        client.ready().await.unwrap();
        let body = Bytes::from(vec![7u8; size]);
        let reply = client
            .unary(
                tonic::Request::new(body.clone()),
                path.parse().unwrap(),
                BytesCodec,
            )
            .await
            .unwrap();
        assert_eq!(reply.into_inner(), body, "{path}");
    }
}
