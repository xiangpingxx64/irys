//! Contains a common set of types used across all of the `irys-chain` modules.
//!
//! This module implements a single location where these types are managed,
//! making them easy to reference and maintain.
pub mod block_header;
pub mod consensus;
pub mod serialization_types;

pub use block_header::*;
pub use serialization_types::*;
