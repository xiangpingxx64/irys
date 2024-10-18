pub mod address;
pub mod chain;
pub mod custom_pipeline;
pub mod custom_rpc;
mod mdbx;
pub mod node_block_test;
pub mod node_launcher;
pub mod p2p;
pub mod payload;
pub mod test_node;
pub mod tests;
// #[cfg(test)]
// mod tests {

//     use crate::chain::get_chain_spec;

//     #[test]
//     fn test() {
//         dbg!("rust tests");
//         let chain_spec = get_chain_spec();
//         dbg!(chain_spec);
//         ()
//     }
// }
