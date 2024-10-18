flow:
init: payloadService builder - defines payload generator and the payload "job" configuration as part of the node configuration
1. fork choice updated
2. rpc-engine-api
3. validate_version_specific_fields (we can hook this)
4. rpc-engine-api/new_payload_v(1/2/3/4) [validate_version_specific_fields](../reth/crates/rpc/rpc-engine-api/src/engine_api.rs#L176)
5. concensus: new payload
6. ?? -> payloadService builder (can also hook this)
7. payload generator (can hook this)
8.1 ?? -> blockchain tree validate_and_execute (for canonical chain) eth block executor [AppendableChain](../reth/crates/blockchain-tree/src/chain.rs#L170)



need to figure out how the AccountInfo works