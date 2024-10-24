use actix::{Actor, Context, Handler};
use irys_types::block_production::SolutionContext;

pub struct BlockProducerActor {}

impl Actor for BlockProducerActor {
    type Context = Context<Self>;
}

impl Handler<SolutionContext> for BlockProducerActor {
    type Result = ();

    fn handle(&mut self, msg: SolutionContext, ctx: &mut Self::Context) -> Self::Result {
        ()
    }
}