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

use crossbeam_utils::CachePadded;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// A monotonic logical clock minting merge versions.
///
/// A merge version is the sequence number of the commit ring slot its
/// commit claims (`Inner::commit_ring`), so versions are dense and unique, and
/// follow the order of the slots that validation serialises commits in.
/// Every claimed slot publishes exactly one merge at its version, empty if
/// its transaction aborts. Versions are published to `timestamp`
/// opportunistically once the claimed version's merge-queue entry has been
/// inserted. Claiming by merge-queue slot insertion alone would be unsound,
/// because a committer removes its merge entry once applied, and a slow
/// concurrent committer that loaded the clock before the publish could
/// then re-claim the vacated slot — minting the same version twice and
/// silently overwriting a committed value in the version chain.
///
/// `timestamp` holds the latest *published* merge version (`0` when no
/// merge has happened yet). Readers snapshot it directly. A committer
/// publishes by waiting for `timestamp` to reach its own claimed
/// version's predecessor, then advancing it to its own version in one
/// step (`TransactionInner::publish_merge`): every publish is in strict
/// claim order, so a thread always sees its own immediately-prior commit
/// reflected in `timestamp` before it can start another. A committer
/// appends to the append-only log between claiming and publishing, so a
/// published version and every version below it are already persisted.
/// `Inner::try_advance_merge_clock` additionally walks `timestamp` forward
/// opportunistically through consecutive claimed-and-inserted versions on the
/// cleanup/GC paths, where no thread is waiting on the result. Either way, a
/// snapshot at `v` sees every merge `<= v` (in the merge queue or already
/// applied): `timestamp` only ever crosses a version once its entry is in the
/// merge queue, and merge entries are never removed before the clock has
/// already passed them (see `Inner::merge_retire_id`). On persistent databases
/// the clock and the commit ring are seeded at load time with the maximum
/// version found across the snapshot file and the append-only log, so newly
/// minted versions always continue strictly above every persisted version.
pub(crate) struct Oracle {
	/// The latest published merge version
	pub(crate) timestamp: CachePadded<AtomicU64>,
}

impl Oracle {
	/// Creates a new logical clock starting at version zero
	pub fn new() -> Arc<Self> {
		Arc::new(Self {
			timestamp: CachePadded::new(AtomicU64::new(0)),
		})
	}
}
