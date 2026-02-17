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

use bytes::Bytes;
use std::borrow::Cow;

/// An optimised trait for converting values to bytes only when needed
pub trait IntoBytes {
	/// Convert the key to a slice of bytes
	fn as_slice(&self) -> &[u8];
	/// Convert the key to an owned bytes slice
	fn into_bytes(self) -> Bytes;
}

impl IntoBytes for &[u8] {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self
	}

	fn into_bytes(self) -> Bytes {
		// Must copy from &[u8]
		Bytes::copy_from_slice(self)
	}
}

impl IntoBytes for Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		self.as_slice()
	}

	fn into_bytes(self) -> Bytes {
		// Zero-copy from Vec<u8>
		Bytes::from(self)
	}
}

impl IntoBytes for &Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		&self[..]
	}

	fn into_bytes(self) -> Bytes {
		// Must copy from &Vec<u8>
		Bytes::copy_from_slice(&self[..])
	}
}

impl IntoBytes for Bytes {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> Bytes {
		// Zero-copy from self
		self
	}
}

impl IntoBytes for &Bytes {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> Bytes {
		// Zero-copy from self
		self.clone()
	}
}

impl IntoBytes for &str {
	fn as_slice(&self) -> &[u8] {
		// Get the string bytes reference
		self.as_bytes()
	}

	fn into_bytes(self) -> Bytes {
		// Must copy from &str
		Bytes::copy_from_slice(self.as_bytes())
	}
}

impl IntoBytes for String {
	fn as_slice(&self) -> &[u8] {
		// Get the string bytes reference
		self.as_bytes()
	}

	fn into_bytes(self) -> Bytes {
		// Zero-copy from String
		Bytes::from(self.into_bytes())
	}
}

impl IntoBytes for &String {
	fn as_slice(&self) -> &[u8] {
		// Get the string bytes reference
		self.as_bytes()
	}

	fn into_bytes(self) -> Bytes {
		// Must copy from &String
		Bytes::copy_from_slice(self.as_bytes())
	}
}

impl IntoBytes for Box<[u8]> {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> Bytes {
		// Zero-copy from Box<[u8]>
		Bytes::from(self)
	}
}

impl<'a> IntoBytes for Cow<'a, [u8]> {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> Bytes {
		// Match the Cow variant
		match self {
			Cow::Borrowed(s) => Bytes::copy_from_slice(s),
			Cow::Owned(v) => Bytes::from(v),
		}
	}
}
