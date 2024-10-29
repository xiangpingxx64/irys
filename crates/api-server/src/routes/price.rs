use std::cmp::max;

use actix_web::{web::Path, HttpResponse};

const MIN_FEE: u128 = 100;

pub async fn get_price(path: Path<u128>) -> actix_web::Result<HttpResponse> {
    let size = path.into_inner();

    let price_per_byte = 0;

    let final_price = max(MIN_FEE, price_per_byte * size);

    Ok(HttpResponse::Ok().body(final_price.to_string()))
}
