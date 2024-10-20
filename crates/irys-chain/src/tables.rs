use irys_types::{IrysBlockHeader, H256};
use reth_codecs::Compact;
use reth_db::{
    table::{DupSort, Table},
    DatabaseError,
};
use reth_db_api::table::{Compress, Decompress};
use reth_primitives::revm_primitives::B256;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Adds wrapper structs for some primitive types so they can use `StructFlags` from Compact, when
/// used as pure table values.
macro_rules! add_wrapper_struct {
	($(($name:tt, $wrapper:tt)),+) => {
			$(
					/// Wrapper struct so it can use StructFlags from Compact, when used as pure table values.
					#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Compact)]
					#[derive(arbitrary::Arbitrary)]
					//#[add_arbitrary_tests(compact)]
					pub struct $wrapper(pub $name);

					impl From<$name> for $wrapper {
							fn from(value: $name) -> Self {
									$wrapper(value)
							}
					}

					impl From<$wrapper> for $name {
							fn from(value: $wrapper) -> Self {
									value.0
							}
					}

					impl std::ops::Deref for $wrapper {
							type Target = $name;

							fn deref(&self) -> &Self::Target {
									&self.0
							}
					}

			)+
	};
}

macro_rules! impl_compression_for_compact {
	($($name:tt),+) => {
			$(
					impl Compress for $name {
							type Compressed = Vec<u8>;

							fn compress_to_buf<B: bytes::BufMut + AsMut<[u8]>>(self, buf: &mut B) {
									let _ = Compact::to_compact(&self, buf);
							}
					}

					impl Decompress for $name {
							fn decompress(value: &[u8]) -> Result<$name, DatabaseError> {
									let (obj, _) = Compact::from_compact(value, value.len());
									Ok(obj)
							}
					}
			)+
	};
}

add_wrapper_struct!((IrysBlockHeader, CompactIrysBlockHeader));
impl_compression_for_compact!(CompactIrysBlockHeader);

/// Enum for the types of tables present in libmdbx.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum TableType {
    /// key value table
    Table,
    /// Duplicate key value table
    DupSort,
}

/// The general purpose of this is to use with a combination of Tables enum,
/// by implementing a `TableViewer` trait you can operate on db tables in an abstract way.
///
/// # Example
///
/// ```
/// use reth_db::{TableViewer, Tables};
/// use reth_db_api::table::{DupSort, Table};
///
/// struct MyTableViewer;
///
/// impl TableViewer<()> for MyTableViewer {
///     type Error = &'static str;
///
///     fn view<T: Table>(&self) -> Result<(), Self::Error> {
///         // operate on table in a generic way
///         Ok(())
///     }
///
///     fn view_dupsort<T: DupSort>(&self) -> Result<(), Self::Error> {
///         // operate on a dupsort table in a generic way
///         Ok(())
///     }
/// }
///
/// let viewer = MyTableViewer {};
///
/// let _ = Tables::Headers.view(&viewer);
/// let _ = Tables::Transactions.view(&viewer);
/// ```
pub trait TableViewer<R> {
    /// The error type returned by the viewer.
    type Error;

    /// Calls `view` with the correct table type.
    fn view_rt(&self, table: Tables) -> Result<R, Self::Error> {
        table.view(self)
    }

    /// Operate on the table in a generic way.
    fn view<T: Table>(&self) -> Result<R, Self::Error>;

    /// Operate on the dupsort table in a generic way.
    ///
    /// By default, the `view` function is invoked unless overridden.
    fn view_dupsort<T: DupSort>(&self) -> Result<R, Self::Error> {
        self.view::<T>()
    }
}

/// Defines all the tables in the database.
#[macro_export]
macro_rules! tables {
	(@bool) => { false };
	(@bool $($t:tt)+) => { true };

	(@view $name:ident $v:ident) => { $v.view::<$name>() };
	(@view $name:ident $v:ident $_subkey:ty) => { $v.view_dupsort::<$name>() };

	($( $(#[$attr:meta])* table $name:ident<Key = $key:ty, Value = $value:ty $(, SubKey = $subkey:ty)? $(,)?>; )*) => {
			// Table marker types.
			$(
					$(#[$attr])*
					///
					#[doc = concat!("Marker type representing a database table mapping [`", stringify!($key), "`] to [`", stringify!($value), "`].")]
					$(
							#[doc = concat!("\n\nThis table's `DUPSORT` subkey is [`", stringify!($subkey), "`].")]
					)?
					pub struct $name {
							_private: (),
					}

					// Ideally this implementation wouldn't exist, but it is necessary to derive `Debug`
					// when a type is generic over `T: Table`. See: https://github.com/rust-lang/rust/issues/26925
					impl fmt::Debug for $name {
							fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
									unreachable!("this type cannot be instantiated")
							}
					}

					impl reth_db_api::table::Table for $name {
							const NAME: &'static str = table_names::$name;

							type Key = $key;
							type Value = $value;
					}

					$(
							impl DupSort for $name {
									type SubKey = $subkey;
							}
					)?
			)*

			// Tables enum.
			// NOTE: the ordering of the enum does not matter, but it is assumed that the discriminants
			// start at 0 and increment by 1 for each variant (the default behavior).
			// See for example `reth_db::implementation::mdbx::tx::Tx::db_handles`.

			/// A table in the database.
			#[derive(Clone, Copy, PartialEq, Eq, Hash)]
			pub enum Tables {
					$(
							#[doc = concat!("The [`", stringify!($name), "`] database table.")]
							$name,
					)*
			}

			impl Tables {
					/// All the tables in the database.
					pub const ALL: &'static [Self] = &[$(Self::$name,)*];

					/// The number of tables in the database.
					pub const COUNT: usize = Self::ALL.len();

					/// Returns the name of the table as a string.
					pub const fn name(&self) -> &'static str {
							match self {
									$(
											Self::$name => table_names::$name,
									)*
							}
					}

					/// Returns `true` if the table is a `DUPSORT` table.
					pub const fn is_dupsort(&self) -> bool {
							match self {
									$(
											Self::$name => tables!(@bool $($subkey)?),
									)*
							}
					}

					/// The type of the given table in database.
					pub const fn table_type(&self) -> TableType {
							if self.is_dupsort() {
									TableType::DupSort
							} else {
									TableType::Table
							}
					}

					/// Allows to operate on specific table type
					pub fn view<T, R>(&self, visitor: &T) -> Result<R, T::Error>
					where
							T: ?Sized + TableViewer<R>,
					{
							match self {
									$(
											Self::$name => tables!(@view $name visitor $($subkey)?),
									)*
							}
					}
			}

			impl fmt::Debug for Tables {
					#[inline]
					fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
							f.write_str(self.name())
					}
			}

			impl fmt::Display for Tables {
					#[inline]
					fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
							self.name().fmt(f)
					}
			}

			impl std::str::FromStr for Tables {
					type Err = String;

					fn from_str(s: &str) -> Result<Self, Self::Err> {
							match s {
									$(
											table_names::$name => Ok(Self::$name),
									)*
									s => Err(format!("unknown table: {s:?}")),
							}
					}
			}

			// Need constants to match on in the `FromStr` implementation.
			#[allow(non_upper_case_globals)]
			mod table_names {
					$(
							pub(super) const $name: &'static str = stringify!($name);
					)*
			}

			/// Maps a run-time [`Tables`] enum value to its corresponding compile-time [`Table`] type.
			///
			/// This is a simpler alternative to [`TableViewer`].
			///
			/// # Examples
			///
			/// ```
			/// use reth_db::{Tables, tables_to_generic};
			/// use reth_db_api::table::Table;
			///
			/// let table = Tables::Headers;
			/// let result = tables_to_generic!(table, |GenericTable| GenericTable::NAME);
			/// assert_eq!(result, table.name());
			/// ```
			#[macro_export]
			macro_rules! tables_to_generic {
					($table:expr, |$generic_name:ident| $e:expr) => {
							match $table {
									$(
											Tables::$name => {
													use $crate::tables::$name as $generic_name;
													$e
											},
									)*
							}
					};
			}
	};
}

tables! {
    /// Stores the header hashes belonging to the canonical chain.
    table IrysBlockHeaders<Key = B256, Value = CompactIrysBlockHeader>;
}
