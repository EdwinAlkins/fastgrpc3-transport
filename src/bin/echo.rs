//! `echo PORT [THREADS]`: the step 1 server (tokio current-thread when THREADS is 1).

use fastgrpc3_transport::echo::EchoService;
use tower::ServiceExt;

fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().expect("PORT").parse().expect("PORT");
    let threads: usize = args.next().map_or(1, |t| t.parse().expect("THREADS"));
    let runtime = if threads == 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(threads);
        builder
    }
    .enable_all()
    .build()
    .expect("runtime");
    runtime.block_on(async move {
        let address = format!("127.0.0.1:{port}").parse().expect("address");
        // Every path: the axum router hands over its own body type, tonic wants its own.
        let service = EchoService.map_request(|request: http::Request<axum::body::Body>| {
            request.map(tonic::body::Body::new)
        });
        let routes = tonic::service::Routes::from(axum::Router::new().fallback_service(service));
        tonic::transport::Server::builder()
            .add_routes(routes)
            .serve(address)
            .await
            .expect("serve");
    });
}
