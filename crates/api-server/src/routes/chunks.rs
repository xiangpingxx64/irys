use actix_web::{web::Json, HttpResponse};
use irys_types::Base64;
use serde::Deserialize;

// TODO: Implement serde decoding for Base64url
#[derive(Deserialize)]
pub struct PostChunkBody {
    chunk: Base64,
    data_path: Base64,
}

pub async fn post_chunk(chunk: Json<PostChunkBody>) -> actix_web::Result<HttpResponse> {
    // TODO: Implement logic

    Ok(HttpResponse::Ok().finish())
}
