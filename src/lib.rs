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

#![allow(clippy::bool_comparison)]

#[cfg(not(target_arch = "wasm32"))]
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
#[cfg(not(target_arch = "wasm32"))]
mod persistence;
mod pool;
mod queue;
mod tx;
mod version;
mod versions;

#[doc(inline)]
pub use bytes::Bytes;

#[cfg(not(target_arch = "wasm32"))]
#[doc(inline)]
pub use self::compression::*;
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
#[cfg(not(target_arch = "wasm32"))]
#[doc(inline)]
pub use self::persistence::*;
#[doc(inline)]
pub use self::tx::*;
