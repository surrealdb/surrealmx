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
//! This module stores a MVCC versioned entry.

use bytes::Bytes;
use std::cmp::Ordering;

#[derive(Clone, Eq, PartialEq)]

pub struct Version {
	/// The version of this entry
	pub(crate) version: u64,
	/// The value of this entry. If this is
	/// None, then the key is deleted and if
	/// it is Some then the key exists.
	pub(crate) value: Option<Bytes>,
}

impl Ord for Version {
	#[inline]

	fn cmp(&self, other: &Self) -> Ordering {

		self.version.cmp(&other.version)
	}
}

impl PartialOrd for Version {
	#[inline]

	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {

		Some(self.cmp(other))
	}
}
