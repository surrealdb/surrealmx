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

//! This module stores the transaction commit and merge queues.

use ahash::AHashSet;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A transaction entry in the transaction commit queue
pub struct Commit {
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The local set of updates and deletes
	pub(crate) writeset: Arc<BTreeMap<Bytes, Option<Bytes>>>,
}

/// A transaction entry in the transaction merge queue
pub struct Merge {
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The local set of updates and deletes
	pub(crate) writeset: Arc<BTreeMap<Bytes, Option<Bytes>>>,
}

impl Commit {
	/// Returns true if self has no elements in common with other
	pub fn is_disjoint_readset(&self, other: &AHashSet<Bytes>) -> bool {
		// Check if the readset is not empty
		if !other.is_empty() {
			// Check if any key in writeset exists in the readset
			for key in self.writeset.keys() {
				if other.contains(key) {
					return false;
				}
			}
		}
		// No overlap was found
		true
	}
	/// Returns true if self has no elements in common with other
	pub fn is_disjoint_writeset(&self, other: &Arc<Commit>) -> bool {
		// Create a key iterator for each writeset
		let mut a = self.writeset.keys();
		let mut b = other.writeset.keys();
		// Move to the next value in each iterator
		let mut next_a = a.next();
		let mut next_b = b.next();
		// Advance each iterator independently in order
		while let (Some(ka), Some(kb)) = (next_a, next_b) {
			match ka.cmp(kb) {
				std::cmp::Ordering::Less => next_a = a.next(),
				std::cmp::Ordering::Greater => next_b = b.next(),
				std::cmp::Ordering::Equal => return false,
			}
		}
		// No overlap was found
		true
	}
}
