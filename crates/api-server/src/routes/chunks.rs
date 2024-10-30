use actix_web::{web::Json, HttpResponse};
use irys_types::{Base64, Chunk};
use serde::Deserialize;

pub async fn post_chunk(chunk: Json<Chunk>) -> actix_web::Result<HttpResponse> {
    // TODO: Implement logic

    Ok(HttpResponse::Ok().finish())
}
