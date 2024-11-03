use std::{
    fs::File,
    io::{Read, Seek as _, SeekFrom, Write},
    path::PathBuf,
    sync::{RwLock, RwLockWriteGuard},
};

use irys_types::CHUNK_SIZE;
use nodit::{
    interval::{ie, ii},
    InclusiveInterval, Interval, NoditMap,
};

use tracing::warn;

use crate::{interval::WriteLock, provider::generate_chunk_test_data};

use super::interval::{
    get_duration_since_epoch, get_now, IntervalState, IntervalStateWrapped, PackingState,
};

#[derive(Debug)]
/// Struct representing all the state of a storage module
pub struct StorageModule {
    /// Path of the SM's storage file
    pub path: PathBuf,
    /// Non-overlapping interval map representing the states of different segments of the SM
    pub interval_map: RwLock<NoditMap<u32, Interval<u32>, IntervalStateWrapped>>,
    /// capacity (in chunks) allocated to this storage module
    pub capacity: u32,
}

const STALE_LOCK_TIMEOUT_MS: u64 = 60_000;

impl StorageModule {
    /// Acquire a File handle for the SM's path
    pub fn get_handle(&self) -> Result<File, std::io::Error> {
        File::open(&self.path)
    }

    /// read some chunks with a sm-relative offset
    pub fn read_chunks(
        &self,
        interval: Interval<u32>,
    ) -> eyre::Result<Vec<[u8; CHUNK_SIZE as usize]>> {
        let chunks_to_read = interval.width() + 1;
        let mut chunks_done: u32 = 0;
        let mut handle = self.get_handle()?;

        // move the handle to the requested start
        if interval.start() > 0 {
            handle.seek(SeekFrom::Start(interval.start() as u64 * CHUNK_SIZE))?;
        }

        let mut result = Vec::with_capacity(chunks_to_read.try_into()?);
        while chunks_done < chunks_to_read {
            // read 1 chunk
            let mut buf: [u8; CHUNK_SIZE as usize] = [0; CHUNK_SIZE as usize];
            handle.read_exact(&mut buf)?;
            result.push(buf);
            chunks_done = chunks_done + 1;
        }
        return Ok(result);
    }
    /// Read an interval of chunks, ensuring that chunks read are of a specific state (see PackingState)
    pub fn read_chunk_tagged(
        &self,
        interval: Interval<u32>,
        state: PackingState,
    ) -> eyre::Result<Vec<[u8; CHUNK_SIZE as usize]>> {
        todo!()
    }

    /// Scan for an "clean" any stale locks
    /// this function will reset the intervals that are under a stale lock by marking them as unpacked
    /// as there's no definitive way to recover otherwise
    pub fn clean_stale_locks(&self) -> eyre::Result<()> {
        let interval_map = self.interval_map.read().unwrap();
        let now = get_now();
        let mut stale_locks = vec![];
        for (interval, state) in interval_map.iter() {
            match &state.state {
                PackingState::WriteLocked(wl_state) => {
                    // check if the write lock was refreshed > CHUNK_WL_SECS ago
                    if (now - wl_state.refreshed_at) > STALE_LOCK_TIMEOUT_MS {
                        // this lock is stale
                        warn!(
                            "Stale lock for interval {:?}, initially locked at {:?} storage module path {:?}",
                            &interval, &wl_state.locked_at,  &self.path
                        );
                        stale_locks.push(interval);
                    }
                }
                _ => (),
            }
        }
        // clear the stale locks by setting their intervals to "unpacked", effectively clearing them
        // we do a read then write pass as the most likely path is that the read finds no stale locks
        // so this is done to prevent unneeded write locking of the interval map
        let mut interval_map_w = self.interval_map.write().unwrap();
        for invalid_interval in stale_locks.iter() {
            let _cut = interval_map_w.insert_overwrite(
                **invalid_interval,
                IntervalStateWrapped::new(IntervalState {
                    state: PackingState::Unpacked,
                }),
            );
        }
        Ok(())
    }

    /// Create a new SM instance at the specified path with the specified capacity (in bytes)
    /// the file will be 0-allocated if it doesn't exist
    pub fn new(path: PathBuf, capacity_bytes: u64) -> Self {
        let capacity_chunks = (capacity_bytes / CHUNK_SIZE) as u32;
        let mut map = NoditMap::new();
        map.insert_strict(
            ie(0 as u32, capacity_chunks as u32),
            IntervalStateWrapped::new(IntervalState {
                state: PackingState::Unpacked,
            }),
        )
        .unwrap(); // TODO @JesseTheRobot - map error

        // TODO @JesseTheRobot - 0 allocate the file if it doesn't exist
        return StorageModule {
            path: path,
            interval_map: RwLock::new(map),
            capacity: capacity_chunks,
        };
    }

    /// Writes some chunks to an interval, and tags the interval with the provided new state
    pub fn write_chunks(
        &self,
        chunks: Vec<[u8; CHUNK_SIZE as usize]>,
        interval: Interval<u32>,
        new_state: IntervalStateWrapped,
    ) -> eyre::Result<()> {
        let write_start_time = get_duration_since_epoch();

        if interval.end() > self.capacity {
            return Err(eyre::Report::msg(
                "Storage module is too small for write interval",
            ));
        }

        // we "lock" the range we're writing to
        let mut map = self.interval_map.write().unwrap();
        let _ = map.insert_overwrite(
            interval,
            IntervalStateWrapped::new(IntervalState {
                state: PackingState::WriteLocked(WriteLock::new(Some(
                    write_start_time.as_millis() as u64,
                ))),
            }),
        );
        drop(map); // explicitly release write lock on interval map

        let chunks_to_write = interval.width() + 1;

        // TODO QUESTION - should we enforce that the chunks vec has to be the same length as the interval's width?
        if chunks.len() < chunks_to_write as usize {
            return Err(eyre::Report::msg("Chunks vec is too small for interval"));
        }

        let mut chunks_written = 0;

        let mut handle = File::create(&self.path)?;

        // move the handle to the requested start
        if interval.start() > 0 {
            handle.seek(SeekFrom::Start(interval.start() as u64 * CHUNK_SIZE))?;
        }

        while chunks_written < chunks_to_write {
            let now = get_duration_since_epoch();
            // update the lock halfway to the stale timeout
            let d = (now - write_start_time).as_millis();
            if d > (STALE_LOCK_TIMEOUT_MS / 2).into() {
                // update the lock
                // TODO QUESTION There's probably a better way of doing this (maintaining a reference to a Rw/Mutex for WriteLock?)
                let mut map = self.interval_map.write().unwrap();
                let point = map.get_at_point_mut(interval.start()).unwrap();
                match &mut point.state {
                    PackingState::WriteLocked(wl_data) => {
                        wl_data.refresh();
                    }
                    _ => (),
                }
                drop(map); // explicitly release write lock
            }
            // write chunk
            // TODO @JesseTheRobot use vectored read and write
            handle.write(chunks.get(chunks_written as usize).unwrap())?;
            chunks_written = chunks_written + 1;
        }
        // check interval state
        // we error if our interval isn't as we expect (width & lock start)
        let mut map = self.interval_map.write().unwrap();

        let (cur_interval, cur_state) = map.get_key_value_at_point(interval.start()).unwrap();
        if *cur_interval != interval {
            reset_range(map, interval);
            return Err(eyre::Report::msg(
                "Interval has changed unexpectedly during write operation",
            ));
        }
        match &cur_state.state {
            PackingState::WriteLocked(write_lock) => {
                if write_lock.locked_at != write_start_time.as_millis() as u64 {
                    reset_range(map, interval);
                    return Err(eyre::Report::msg(
                        "Interval has changed lock start time during write operation",
                    ));
                }
            }
            _ => {
                reset_range(map, interval);
                return Err(eyre::Report::msg(
                    "Interval has changed state type during write operation",
                ));
            }
        }

        let _ = map.insert_overwrite(interval, new_state);

        // let mut point = map.get_at_point_mut(interval.start()).unwrap();
        // // TODO @JesseTheRobot - nicer way of doing this?
        // let mut overlap_iter: Vec<(&Interval<u32>, &mut IntervalStateWrapped)> =
        //     map.overlapping_mut(interval).collect();
        // if overlap_iter.len() > 1 {
        //     // wipe the interval we should have control over, something else has tampered with it
        //     let _ = map.insert_overwrite(
        //         interval,
        //         IntervalStateWrapped::new(IntervalState {
        //             state: PackingState::Unpacked,
        //         }),
        //     );
        //     return Err(eyre::Report::msg(
        //         "more than the one expected overlap interval",
        //     ));
        // }

        // let point = overlap_iter.get_mut(0).unwrap();
        // // set the new state
        // *point.1 = new_state;

        drop(map);
        Ok(())
    }
}

/// Resets an interval to `PackingState::Unpacked`
pub fn reset_range(
    mut map: RwLockWriteGuard<'_, NoditMap<u32, Interval<u32>, IntervalStateWrapped>>,
    interval: Interval<u32>,
) {
    let _ = map.insert_overwrite(
        interval,
        IntervalStateWrapped::new(IntervalState {
            state: PackingState::Unpacked,
        }),
    );
}

#[test]
fn test_sm_rw() -> eyre::Result<()> {
    let chunks: u8 = 10;
    generate_chunk_test_data("/tmp/sample".into(), chunks, 0)?;
    let sm1_interval_map = NoditMap::from_slice_strict([(
        ie(0, chunks as u32),
        IntervalStateWrapped::new(IntervalState {
            state: PackingState::Data,
        }),
    )])
    .unwrap();

    // let mut sp = StorageProvider {
    //     sm_map: NoditMap::from_slice_strict([(
    //         ie(0, chunks as u32),
    //         StorageModule {
    //             path: "/tmp/sample".into(),
    //             interval_map: RwLock::new(sm1_interval_map),
    //             capacity: chunks as u32,
    //         },
    //     )])
    //     .unwrap(),
    // };

    let sm = StorageModule {
        path: "/tmp/sample".into(),
        interval_map: RwLock::new(sm1_interval_map),
        capacity: chunks as u32,
    };

    let interval = ii(1, 1);

    let chunks = sm.read_chunks(interval)?;

    sm.write_chunks(
        vec![[69; CHUNK_SIZE as usize]],
        interval,
        IntervalStateWrapped::new(IntervalState {
            state: PackingState::Packed,
        }),
    )?;

    let chunks2 = sm.read_chunks(interval)?;

    dbg!(&chunks, &chunks2);

    Ok(())
}
