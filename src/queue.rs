use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
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
	pub fn is_disjoint_readset(&self, other: &BTreeSet<Bytes>) -> bool {
		// Create a key iterator for each writeset
		let mut a = self.writeset.keys();
		let mut b = other.iter();
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
