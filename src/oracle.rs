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

//! This module stores the monotonic logical clock for merge versions.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// A monotonic logical clock minting merge versions.
///
/// Merge versions are claimed from `alloc`, a dense allocation counter,
/// and published to `timestamp` opportunistically once the claimed
/// version's merge-queue entry has been inserted. The two counters must
/// be separate: claiming by merge-queue slot insertion alone is unsound,
/// because a committer removes its merge entry once applied, and a slow
/// concurrent committer that loaded the clock before the publish could
/// then re-claim the vacated slot — minting the same version twice and
/// silently overwriting a committed value in the version chain.
///
/// `timestamp` holds the latest *published* merge version (`0` when no
/// merge has happened yet). Readers snapshot it directly. A committer
/// publishes by waiting for `timestamp` to reach its own claimed
/// version's predecessor, then advancing it to its own version in one
/// step (`Inner::atomic_merge`): every publish is in strict claim order,
/// so a thread always sees its own immediately-prior commit reflected in
/// `timestamp` before it can start another. `Inner::try_advance_merge_clock`
/// additionally walks `timestamp` forward opportunistically through
/// consecutive claimed-and-inserted versions on the cleanup/GC paths,
/// where no thread is waiting on the result. Either way, a snapshot at
/// `v` sees every merge `<= v` (in the merge queue or already applied):
/// `timestamp` only ever crosses a slot once its entry is confirmed
/// present, and merge entries are never physically removed before the
/// clock has already passed them (see `Inner::merge_retire_id` and the
/// persistence-failure path in `TransactionInner::commit`), so "absent"
/// at the next slot can only mean "not yet inserted", never "removed
/// early". On persistent databases both counters are seeded at load
/// time with the maximum version found across the snapshot file and the
/// append-only log, so newly minted versions always continue strictly
/// above every persisted version.
pub(crate) struct Oracle {
	/// The merge version allocation counter
	pub(crate) alloc: AtomicU64,
	/// The latest published merge version
	pub(crate) timestamp: AtomicU64,
}

impl Oracle {
	/// Creates a new logical clock starting at version zero
	pub fn new() -> Arc<Self> {
		Arc::new(Self {
			alloc: AtomicU64::new(0),
			timestamp: AtomicU64::new(0),
		})
	}
}
