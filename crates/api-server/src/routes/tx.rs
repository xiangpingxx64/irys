use actix_web::{
    web::{Data, Json},
    FromRequest, HttpResponse,
};
use irys_types::IrysTransactionHeader;

pub async fn post_tx(body: Json<IrysTransactionHeader>) -> actix_web::Result<HttpResponse> {
    let tx = body.into_inner();

    // TODO: Validate transaction is valid. Check balances etc etc.

    // TODO: Commit to mempool.

    Ok(HttpResponse::Ok().finish())
}
