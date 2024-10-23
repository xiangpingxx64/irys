mod routes;

use actix_web::{web, App, HttpServer};
use routes::{chunks, index, price, proxy::proxy, tx};

pub async fn run_server() {
    HttpServer::new(|| {
        let awc_client = awc::Client::new();

        App::new()
            .app_data(web::Data::new(awc_client))
            .service(
                web::scope("v1")
                    .route("/info", web::get().to(index::info_route))
                    .route("/chunk", web::post().to(chunks::post_chunk))
                    .route("/tx", web::post().to(tx::post_tx))
                    .route("/price/{size}", web::get().to(price::get_price)),
            )
            .route("/", web::post().to(proxy))
            .route("/", web::get().to(proxy))
    })
    .bind(("127.0.0.1", 8080))
    .unwrap()
    .run()
    .await
    .unwrap();
}
