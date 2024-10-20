mod routes;

use actix_web::{web, App, HttpServer};
use routes::{chunks, index, proxy::proxy, tx};


pub async fn run_server() -> std::io::Result<()> {
    HttpServer::new(|| {
        let awc_client = awc::Client::new();

        App::new()
            .app_data(web::Data::new(awc_client))
            .route("/info", web::get().to(index::info_route))
            .route("/chunk", web::post().to(chunks::post_chunk))
            .route("/tx", web::post().to(tx::post_tx))
            .route("/", web::post().to(proxy))
            .route("/", web::get().to(proxy))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}