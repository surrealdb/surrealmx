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
use byteslice::ByteSlice;
use std::borrow::Cow;

/// An optimised trait for converting values to bytes only when needed
pub trait IntoBytes {
	/// Convert the key to a slice of bytes
	fn as_slice(&self) -> &[u8];
	/// Convert the key to an owned byteslice
	fn into_bytes(self) -> ByteSlice;
}

impl IntoBytes for &[u8] {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_slice(self)
	}
}

impl<const N: usize> IntoBytes for &[u8; N] {
	fn as_slice(&self) -> &[u8] {
		&self[..]
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_slice(&self[..])
	}
}

impl<const N: usize> IntoBytes for [u8; N] {
	fn as_slice(&self) -> &[u8] {
		&self[..]
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_slice(&self[..])
	}
}

impl IntoBytes for Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		&self[..]
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from(self)
	}
}

impl IntoBytes for &Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		&self[..]
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_slice(&self[..])
	}
}

impl IntoBytes for ByteSlice {
	fn as_slice(&self) -> &[u8] {
		self.as_ref()
	}

	fn into_bytes(self) -> ByteSlice {
		self
	}
}

impl IntoBytes for &ByteSlice {
	fn as_slice(&self) -> &[u8] {
		self.as_ref()
	}

	fn into_bytes(self) -> ByteSlice {
		self.clone()
	}
}

impl IntoBytes for Bytes {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_bytes(&self)
	}
}

impl IntoBytes for &Bytes {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_bytes(self)
	}
}

impl IntoBytes for &str {
	fn as_slice(&self) -> &[u8] {
		// Get the string bytes reference
		self.as_bytes()
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_slice(self.as_bytes())
	}
}

impl IntoBytes for String {
	fn as_slice(&self) -> &[u8] {
		// Get the string bytes reference
		self.as_bytes()
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from(self.into_bytes())
	}
}

impl IntoBytes for &String {
	fn as_slice(&self) -> &[u8] {
		// Get the string bytes reference
		self.as_bytes()
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_slice(self.as_bytes())
	}
}

impl IntoBytes for Box<[u8]> {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> ByteSlice {
		ByteSlice::from_slice(self.as_ref())
	}
}

impl IntoBytes for Cow<'_, [u8]> {
	fn as_slice(&self) -> &[u8] {
		// Get the bytes reference
		self.as_ref()
	}

	fn into_bytes(self) -> ByteSlice {
		// Match the Cow variant
		match self {
			Cow::Borrowed(s) => ByteSlice::from_slice(s),
			Cow::Owned(v) => ByteSlice::from(v),
		}
	}
}
