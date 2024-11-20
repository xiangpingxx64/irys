use eyre::Result;
use irys_types::CHUNK_SIZE;
use nodit::{interval::ie, InclusiveInterval, Interval, NoditMap, NoditSet};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    ops::Range,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

type ChunkOffset = u32;
type ChunkBytes = Vec<u8>;

// In-memory chunk data indexed by offset within partition
type ChunkMap = BTreeMap<ChunkOffset, (ChunkBytes, ChunkType)>;

/// Storage submodules mapped to their chunk ranges
type SubmoduleMap = NoditMap<u32, Interval<u32>, StorageSubmodule>;

/// Tracks storage state of chunk ranges across all submodules
type StorageIntervals = NoditMap<u32, Interval<u32>, ChunkType>;

/// Maps a logical partition (fixed size) to physical storage across multiple drives
#[derive(Debug)]
pub struct StorageModule {
    /// In-memory chunk buffer awaiting disk write
    pending_writes: Arc<RwLock<ChunkMap>>,
    /// Tracks the storage state of each chunk across all submodules
    intervals: Arc<RwLock<StorageIntervals>>,
    /// Physical storage locations indexed by chunk ranges
    submodules: SubmoduleMap,
    /// Runtime configuration parameters
    config: StorageModuleConfig,
}

/// Per-module configuration for tuning storage behavior
#[derive(Debug)]
pub struct StorageModuleConfig {
    /// Minimum chunks to accumulate before disk sync
    pub min_writes_before_sync: usize,
    /// Size of each chunk in bytes
    pub chunk_size: u64,
}

impl Default for StorageModuleConfig {
    fn default() -> Self {
        Self {
            min_writes_before_sync: 1,
            chunk_size: CHUNK_SIZE,
        }
    }
}

/// Manages chunk storage on a single physical drive
#[derive(Debug)]
struct StorageSubmodule {
    /// Persistent storage handle
    file: Arc<Mutex<File>>,
}

/// Defines how chunk data is processed and stored
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ChunkType {
    /// Chunk containing matrix-packed entropy only
    Entropy,
    /// Chunk containing packed blockchain transaction data
    Data,
    /// Chunk has not been initialized
    Uninitialized,
}

impl StorageModule {
    /// Initializes a new StorageModule
    pub fn new(
        base_path: &str,
        ranges: Vec<(Range<u32>, &str)>,
        config: Option<StorageModuleConfig>,
    ) -> Self {
        // Use the provided config or Default
        let config = match config {
            Some(cfg) => cfg,
            None => StorageModuleConfig::default(),
        };

        let mut map = NoditMap::new();
        let mut intervals = StorageIntervals::new();

        for (range, dir) in ranges {
            let path = Path::new(base_path).join(dir).join("data.dat");
            println!("{:?}", path);
            let file = Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true) // Optional: creates file if it doesn't exist
                    .open(&path)
                    .unwrap_or_else(|_| panic!("Failed to open: {}", path.display())),
            ));
            // Convert Range to an inclusive-exclusive interval and insert into the map
            let ie = ie(range.start, range.end);
            map.insert_strict(
                ie.clone(), // Inclusive-exclusive interval
                StorageSubmodule { file },
            )
            .unwrap_or_else(|_| panic!("Failed to insert interval: {}-{}", range.start, range.end));

            let _ = intervals.insert_merge_touching_if_values_equal(ie, ChunkType::Uninitialized);
        }

        println!("intervals:{:?}", intervals);

        StorageModule {
            pending_writes: Arc::new(RwLock::new(ChunkMap::new())),
            intervals: Arc::new(RwLock::new(intervals)),
            submodules: map,
            config,
        }
    }

    /// Synchronizes chunks to disk when sufficient writes have accumulated
    ///
    /// Process:
    /// 1. Collects pending writes that meet threshold for each submodule
    /// 2. Acquires write lock only if batched writes exist
    /// 3. Writes chunks to disk and removes them from pending queue
    ///
    /// The sync threshold is configured via `min_writes_before_sync` to optimize
    /// disk writes and minimize fragmentation.
    pub fn sync_pending_chunks(&mut self) -> eyre::Result<()> {
        let threshold = self.config.min_writes_before_sync;
        let arc = self.pending_writes.clone();

        // First use read lock to check if we have work to do
        let write_batch = {
            let pending = arc.read().unwrap();
            self.submodules
                .iter()
                .flat_map(|(interval, _)| {
                    let submodule_writes: Vec<_> = pending
                        .iter()
                        .filter(|(offset, _)| interval.contains_point(**offset))
                        .map(|(offset, state)| (*offset, state.clone()))
                        .collect();

                    if submodule_writes.len() >= threshold {
                        submodule_writes
                    } else {
                        Vec::new()
                    }
                })
                .collect::<Vec<_>>()
        }; // Read lock released here

        // Only acquire write lock if we have work to do
        if !write_batch.is_empty() {
            let mut pending = arc.write().unwrap();
            for (chunk_offset, (bytes, chunk_type)) in write_batch {
                self.write_chunk_internal(chunk_offset, bytes, chunk_type.clone())?;
                pending.remove(&chunk_offset); // Clean up written chunks

                // update the storage intervals
                {
                    let ie = ie(chunk_offset, chunk_offset + 1);
                    let mut intervals = self.intervals.write().unwrap();
                    let _ = intervals.insert_overwrite(ie, chunk_type);
                }
            }
        }

        Ok(())
    }

    /// Reads chunks from the specified range and returns their data and storage state
    ///
    /// Takes a range [start, end) of partition-relative offsets (end exclusive).
    /// Returns a map of chunk offsets to their data and type, excluding uninitialized chunks.
    /// Chunks are read from physical storage for initialized intervals that overlap the range.
    pub fn read_chunks(&self, chunk_range: Range<u32>) -> eyre::Result<ChunkMap> {
        let mut chunk_map = ChunkMap::new();
        // Query overlapping intervals from storage map
        let intervals = self.intervals.read().unwrap();
        let iter = intervals.overlapping(ie(chunk_range.start, chunk_range.end));
        for (interval, chunk_type) in iter {
            if *chunk_type != ChunkType::Uninitialized {
                // For each chunk in the interval
                for chunk_offset in interval.start()..=interval.end() {
                    // Read the chunk from disk
                    let bytes = self.read_chunk_internal(chunk_offset)?;

                    // Add it to the ChunkMap
                    chunk_map.insert(chunk_offset, (bytes, chunk_type.clone()));
                }
            }
        }
        Ok(chunk_map)
    }

    /// Reads a single chunk from its physical storage location
    ///
    /// Given a logical chunk offset, this function:
    /// 1. Locates the appropriate submodule containing the chunk
    /// 2. Calculates the physical file offset
    /// 3. Reads the chunk data into a buffer
    ///
    /// Returns the chunk bytes or an error if read fails
    fn read_chunk_internal(&self, chunk_offset: u32) -> eyre::Result<ChunkBytes> {
        // Find submodule containing this chunk
        let (interval, submodule) = self
            .submodules
            .get_key_value_at_point(chunk_offset)
            .unwrap();

        // Calculate file offset and prepare buffer
        let chunk_size = self.config.chunk_size as u32;
        let file_offset = (chunk_offset - interval.start()) * chunk_size;
        let mut buf = vec![0u8; chunk_size as usize];

        // Read chunk from file
        let mut file = submodule.file.lock().unwrap();
        file.seek(SeekFrom::Start(file_offset.into()))?;
        file.read_exact(&mut buf)?;

        Ok(buf)
    }

    /// Gets all chunk intervals in a given storage state, merging adjacent ranges
    ///
    /// Collects all intervals matching the requested state and combines them when:
    /// - Intervals are touching (e.g., 0-5 and 6-10)
    /// - Intervals overlap (e.g., 0-5 and 3-8)
    ///
    /// Returns a NoditSet containing the merged intervals for efficient range operations
    pub fn get_intervals(&self, chunk_type: ChunkType) -> Vec<Interval<u32>> {
        let intervals = self.intervals.read().unwrap();
        let mut set = NoditSet::new();
        for (interval, ct) in intervals.iter() {
            if *ct == chunk_type {
                let _ = set.insert_merge_touching_or_overlapping(interval.clone());
            }
        }
        // NoditSet is a BTreeMap underneath, meaning collecting them into a vec
        // is done in ascending key order.
        set.into_iter().collect::<Vec<_>>()
    }

    /// Queues chunk data for later disk write. Chunks are batched for efficiency
    /// and written during periodic sync operations.
    pub fn write_chunk(&mut self, chunk_offset: u32, bytes: Vec<u8>, chunk_type: ChunkType) {
        // Add the chunk to pending writes
        let mut pending = self.pending_writes.write().unwrap();
        pending.insert(chunk_offset, (bytes, chunk_type));
    }

    /// Writes chunk data to physical storage and updates state tracking
    ///
    /// Process:
    /// 1. Locates correct submodule for chunk offset
    /// 2. Calculates physical storage position
    /// 3. Writes chunk data to disk
    /// 4. Updates interval tracking with new chunk state
    ///
    /// Note: Chunk size must match size in StorageModule.config
    fn write_chunk_internal(
        &mut self,
        chunk_offset: u32,
        bytes: Vec<u8>,
        chunk_type: ChunkType,
    ) -> eyre::Result<()> {
        let chunk_size = self.config.chunk_size;
        // Get the correct submodule reference based on chunk_offset
        let (interval, submodule) = self
            .submodules
            .get_key_value_at_point(chunk_offset)
            .unwrap();

        // Get the submodule relative offset of the chunk
        let submodule_offset = chunk_offset - interval.start();
        {
            // Lock to the submodules internal file handle & write the chunk
            let mut file = submodule.file.lock().unwrap();
            file.seek(SeekFrom::Start(submodule_offset as u64 * chunk_size))?;
            let result = file.write(bytes.as_slice());
            match result {
                // TODO: better logging
                Ok(bytes_written) => {
                    println!("write_chunk_internal() -> bytes_written: {}", bytes_written)
                }
                Err(err) => println!("{:?}", err),
            }
        }

        // If successful, update the StorageModules interval state
        let mut intervals = self.intervals.write().unwrap();
        let chunk_interval = ie(chunk_offset, chunk_offset + 1);
        let _ = intervals
            .insert_merge_touching_if_values_equal(chunk_interval, chunk_type.clone())
            .unwrap_or_else(|_| {
                let _ = intervals.insert_overwrite(chunk_interval, chunk_type);
                chunk_interval // Return original interval, but it's discarded by outer _
            });
        Ok(())
    }
}

fn initialize_storage_files(base: &str, ranges: &Vec<(Range<u32>, &str)>) -> Result<()> {
    println!("base: {:?}", base);
    let base_path = Path::new(base);
    // Create base storage directory if it doesn't exist
    fs::create_dir_all(base_path)?;

    // Create subdirectories for each range
    for (_, dir) in ranges {
        let path = base_path.join(dir);
        println!(".{:?}", path);
        fs::create_dir_all(&path)?;

        // Create empty data file if it doesn't exist
        let data_file = path.join("data.dat");
        if !data_file.exists() {
            fs::File::create(data_file)?;
        }
    }

    Ok(())
}

//==============================================================================
// Tests
//------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use nodit::interval::ii;

    use super::*;
    #[test]
    fn test() {
        let ranges = vec![
            (0..5, "hdd0-4TB"),  // 0 to 4 inclusive
            (5..10, "hdd1-4TB"), // 5 to 9 inclusive
            (10..20, "hdd-8TB"), // 10 to 19 inclusive
        ];

        // TODO: Update this to use ernesto's temp path
        let base_path = "./storage_modules/1/";
        let _ = initialize_storage_files(base_path, &ranges);

        // Override the default StorageModule config for testing
        let config = StorageModuleConfig {
            min_writes_before_sync: 1,
            chunk_size: 32,
        };

        // Create a StorageModule with the specified submodules and config
        let mut storage_module = StorageModule::new(base_path, ranges, Some(config));

        // Verify the entire storage module range is uninitialized
        let unpacked = storage_module.get_intervals(ChunkType::Uninitialized);
        assert_eq!(unpacked, [ie(0u32, 20u32)]);

        // Create a test (fake) entropy chunk
        let entropy_chunk = vec![0xff; 32]; // All bytes set to 0xff
        storage_module.write_chunk(1, entropy_chunk.to_vec(), ChunkType::Entropy);

        // Invoke the sync task so it gets written to disk
        let _ = storage_module.sync_pending_chunks();

        // Validate the uninitialized intervals have been updated to reflect the new chunk
        let unpacked = storage_module.get_intervals(ChunkType::Uninitialized);
        assert_eq!(unpacked, [ie(0u32, 1u32), ie(2u32, 20u32)]);

        // Validate the Entropy (Packed/unsynced) intervals have been updated
        let packed = storage_module.get_intervals(ChunkType::Entropy);
        assert_eq!(packed, [ie(1u32, 2u32)]);

        // Validate entropy chunk can be read after writing
        let chunks = storage_module.read_chunks(1..2).unwrap();
        let chunk = chunks.get(&1).unwrap();
        assert_eq!(*chunk, (entropy_chunk.clone(), ChunkType::Entropy));

        // Validate that uninitialized chunks are not returned by read_chunks
        let chunks = storage_module.read_chunks(1..3).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(*chunk, (entropy_chunk.clone(), ChunkType::Entropy));

        // Write and sync two sequential data chunks that span a submodule boundary
        let data1_chunk = vec![0x4; 32];
        let data2_chunk = vec![0x5; 32];

        storage_module.write_chunk(4, data1_chunk.to_vec(), ChunkType::Data);
        storage_module.write_chunk(5, data2_chunk.to_vec(), ChunkType::Data);

        // Validate that the pending_writes has two entries
        let num_pending_writes: usize;
        {
            num_pending_writes = storage_module.pending_writes.read().unwrap().len();
        }
        assert_eq!(num_pending_writes, 2);

        // Write the data chunks to disk
        let _ = storage_module.sync_pending_chunks();

        // Validate the data intervals
        let data = storage_module.get_intervals(ChunkType::Data);
        assert_eq!(data, [ie(4u32, 6u32)]);

        // Validate the unpacked intervals are updated
        let unpacked = storage_module.get_intervals(ChunkType::Uninitialized);
        assert_eq!(unpacked, [ii(0u32, 0u32), ii(2u32, 3u32), ii(6u32, 19u32)]);

        // Validate a read_chunks operation across submodule boundaries
        let chunks = storage_module.read_chunks(4..6).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(
            chunks.into_iter().collect::<Vec<_>>(),
            [
                (4, (data1_chunk.clone(), ChunkType::Data)),
                (5, (data2_chunk.clone(), ChunkType::Data))
            ]
        );

        // Query past the range of the StorageModule
        let chunks = storage_module.read_chunks(0..25).unwrap();

        // Verify only initialized chunks are returned
        assert_eq!(
            chunks.into_iter().collect::<Vec<_>>(),
            [
                (1, (entropy_chunk, ChunkType::Entropy)),
                (4, (data1_chunk.clone(), ChunkType::Data)),
                (5, (data2_chunk.clone(), ChunkType::Data))
            ]
        );
    }
}
