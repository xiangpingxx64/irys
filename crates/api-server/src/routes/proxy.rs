use actix_web::{web::Data, HttpResponse};
use awc::Client;
use actix_proxy::{IntoHttpResponse, SendRequestError};

pub async fn proxy(client: Data<Client>) -> Result<HttpResponse, SendRequestError> {
    Ok(client.get("http://localhost:4567").send().await?.into_http_response())
}