// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An embedded, in-memory, lock-free, transaction-based, key-value database
//! engine.
//!
//! A [`Database`] holds versioned keys and hands out [`Transaction`] values
//! from a pool. Transactions read a consistent snapshot and are committed
//! under one of the [`IsolationLevel`] variants, with conflicting commits
//! rejected rather than serialised. Reads and writes never block on a global
//! lock: the datastore is a lock-free skip list, and version visibility is
//! resolved against a logical clock.
//!
//! On non-wasm targets the engine can additionally be backed by persistence
//! (an append-only log plus periodic snapshots); see [`PersistenceOptions`].

/// Tracing target for transaction conflict diagnostics
pub const LOG_TARGET_CONFLICTS: &str = "surrealmx::conflicts";

mod bloom;
mod compression;
mod cursor;
mod db;
mod direction;
mod err;
mod inner;
mod iter;
mod kv;
mod options;
mod oracle;
mod persistence;
mod pool;
mod queue;
mod tx;
mod version;
mod versions;

#[doc(hidden)]
pub mod bench_internals;

#[doc(inline)]
pub use bytes::Bytes;

#[doc(inline)]
pub use self::cursor::*;
#[doc(inline)]
pub use self::db::*;
#[doc(inline)]
pub use self::direction::*;
#[doc(inline)]
pub use self::err::*;
#[doc(inline)]
pub use self::kv::*;
#[doc(inline)]
pub use self::options::*;
#[doc(inline)]
pub use self::tx::*;

#[cfg(not(target_arch = "wasm32"))]
#[doc(inline)]
pub use self::compression::*;
#[cfg(not(target_arch = "wasm32"))]
#[doc(inline)]
pub use self::persistence::*;
