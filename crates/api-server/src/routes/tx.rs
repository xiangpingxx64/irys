use actix_web::{
    web::{self, Data, Json},
    FromRequest, HttpResponse,
};
use actors::{
    mempool::{TxIngressError, TxIngressMessage},
    ActorAddresses,
};
use awc::http::StatusCode;
use irys_types::IrysTransactionHeader;

pub async fn post_tx(
    state: web::Data<ActorAddresses>,
    body: Json<IrysTransactionHeader>,
) -> actix_web::Result<HttpResponse> {
    let tx = body.into_inner();

    // println!("{:?}", tx);
    // println!("{}", serde_json::to_string_pretty(&tx).unwrap());

    // Validate transaction is valid. Check balances etc etc.
    let tx_ingress_msg = TxIngressMessage { 0: tx };
    let result = state.mempool.send(tx_ingress_msg).await;

    // println!("{:?}", result);

    // Provide the appropriate error / feedback to the c
    match result {
        Ok(response) => match response {
            Ok(()) => Ok(HttpResponse::Ok().finish()),
            Err(err) => match err {
                TxIngressError::InvalidSignature => {
                    Ok(HttpResponse::build(StatusCode::BAD_REQUEST)
                        .body(format!("Invalid Signature: {:?}", err)))
                }
                TxIngressError::Unfunded => Ok(HttpResponse::build(StatusCode::PAYMENT_REQUIRED)
                    .body(format!("Unfunded: {:?}", err))),
                TxIngressError::Skipped => Ok(HttpResponse::Ok()
                    .body("Already processed: the transaction was previously handled")),
            },
        },
        Err(err) => {
            println!("Error sending message to actor: {:?}", err);
            Ok(HttpResponse::build(StatusCode::INTERNAL_SERVER_ERROR)
                .body(format!("Failed to process transaction: {:?}", err)))
        }
    }
}
