use actix_web::{web::{Data, Payload}, HttpRequest, HttpResponse};
use awc::{http::Method, Client};
use actix_proxy::{IntoHttpResponse, SendRequestError};

pub async fn proxy(req: HttpRequest, payload: Payload, client: Data<Client>) -> Result<HttpResponse, SendRequestError> {
    match *req.method() {
        Method::GET => Ok(client.get("http://localhost:8545").send().await?.into_http_response()),
        Method::POST => Ok(client.post("http://localhost:8545").send_stream(payload).await?.into_http_response()),
        _ => Ok(HttpResponse::MethodNotAllowed().finish())
    }
}