extern "C" {
    pub fn compute_entropy_chunks_cuda(
        mining_addr: *const cty::c_uchar,
        mining_addr_size: cty::size_t,
        chunk_offset_start: cty::c_ulong,
        chunks_count: cty::c_long,
        partition_hash: *const cty::c_uchar,
        partition_hash_size: cty::size_t,
        chunks: *const cty::c_uchar,
        packing_sha_1_5_s: cty::c_uint
    );
}


pub fn pack() {
   
}

#[cfg(test)]
mod tests {
    use irys_types::consensus::CHUNK_SIZE;

    use super::*;

    #[test]
    fn it_works() {
        let v = Vec::new();
        let num_chunks = 10;
        let buffer_size = CHUNK_SIZE * num_chunks;
        let num_hashes = 1000;
        unsafe { compute_entropy_chunks_cuda(v.as_ptr(), 32usize, 32, num_chunks as i64, Vec::new().as_ptr(), 32, Vec::with_capacity(buffer_size as usize).as_ptr(), num_hashes); }
    }
}
