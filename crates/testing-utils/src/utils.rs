use std::path::PathBuf;
use tracing_subscriber::{util::SubscriberInitExt, FmtSubscriber};

pub fn setup_tracing_and_temp_dir() -> PathBuf {
    // tracing-subscriber is so the tracing log macros (i.e info!) work
    FmtSubscriber::new().init();

    temporary_directory()
}

pub fn temporary_directory() -> PathBuf {
    let builder = tempfile::Builder::new()
        .prefix("irys-test-")
        .rand_bytes(8)
        .tempdir();

    builder
        .expect("Not able to create a temporary directory.")
        .into_path()
}
