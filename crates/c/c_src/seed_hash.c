// Compiles on OSX with:
// clang seed_hash.c -o seed_hash -I /opt/local/include -I/opt/homebrew/Cellar/openssl@1.1/1.1.1v/include /opt/homebrew/Cellar/openssl@1.1/1.1.1v/lib/libcrypto.a
#include "capacity_single.c"

unsigned char mining_addr[] = "Hello";
unsigned char partition_hash[] = "P";
long int chunk_offset = 1;


int main() {
	size_t hash_size = EVP_MD_size(EVP_sha256());
	unsigned char seed_hash[hash_size];

	compute_seed_hash(
			mining_addr, sizeof(mining_addr),
			chunk_offset,
			partition_hash, sizeof(partition_hash),
			seed_hash);

	for(int i = 0; i < hash_size; i++)
        printf("%.2x", seed_hash[i]);
    return 0;

	//printf("Result: %s", seed_hash);
	return 0;
}

