use actix_web::{web::Json, HttpResponse};
use serde::Deserialize;

// TODO: Implement serde decoding for Base64url
#[derive(Deserialize)]
pub struct PostChunkBody {
    chunk: String,
    data_path: String
}

pub async fn post_chunk(chunk: Json<PostChunkBody>) -> actix_web::Result<HttpResponse> {
    // TODO: Implement logic
    
    Ok(HttpResponse::Ok().finish())
}