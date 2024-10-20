use actix_web::HttpResponse;

pub async fn info_route() -> HttpResponse {
    HttpResponse::Ok().body("Hello world!")
}
