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

//! This module stores the compression logic.

use lz4::{Decoder as Lz4Decoder, EncoderBuilder as Lz4EncoderBuilder};
use std::io::{self, Read, Write};
use std::io::{BufRead, BufReader, BufWriter};

/// Compression mode for snapshots
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMode {
	/// No compression (default for backward compatibility)
	#[default]
	None,
	/// LZ4 compression (fast compression/decompression)
	Lz4,
}

/// Compressed writer wrapper that handles different compression modes
pub(crate) struct CompressedWriter {
	inner: Box<dyn Write>,
}

impl CompressedWriter {
	/// Create a new compressed writer based on compression mode
	pub(crate) fn new<W: Write + 'static>(
		writer: W,
		compression_mode: CompressionMode,
	) -> io::Result<Self> {
		// Determine the compression mode
		let inner: Box<dyn Write> = match compression_mode {
			CompressionMode::None => {
				// Wrap with BufWriter for efficient writing
				Box::new(BufWriter::new(writer))
			}
			CompressionMode::Lz4 => {
				// LZ4 encoder does its own buffering, so no need for BufWriter
				let encoder = Lz4EncoderBuilder::new().level(7).build(writer)?;
				Box::new(encoder)
			}
		};
		// Return the writer
		Ok(Self {
			inner,
		})
	}

	/// Finish compression (for LZ4, this finalizes the stream)
	pub(crate) fn finish(self) -> io::Result<()> {
		// The encoder will be dropped here, which finalizes the stream
		Ok(())
	}
}

impl Write for CompressedWriter {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.inner.write(buf)
	}

	fn flush(&mut self) -> io::Result<()> {
		self.inner.flush()
	}
}

/// Compressed reader wrapper that handles different compression modes
pub(crate) struct CompressedReader {
	inner: Box<dyn Read>,
}

impl CompressedReader {
	/// Create a new compressed reader that auto-detects compression mode
	pub(crate) fn new<R: Read + 'static>(reader: R) -> io::Result<Self> {
		// Wrap in BufReader to allow peeking at magic bytes
		let mut buf_reader = BufReader::new(reader);
		// Detect compression mode by peeking at magic bytes
		let compression = {
			// Fill buffer to ensure we can peek at the magic bytes
			buf_reader.fill_buf()?;
			// Try to peek at the first 4 bytes without consuming them
			let buffer = buf_reader.buffer();
			if buffer.len() >= 4 {
				// Check for LZ4 magic number (0x184D2204 in little endian)
				if buffer[0..4] == [0x04, 0x22, 0x4D, 0x18] {
					CompressionMode::Lz4
				} else {
					CompressionMode::None
				}
			} else {
				// If we can't read 4 bytes, assume uncompressed
				CompressionMode::None
			}
		};
		//
		tracing::debug!("Detected snapshot compression: {compression:?}");
		// Create the appropriate decompressor based on detected mode
		let inner: Box<dyn Read> = match compression {
			CompressionMode::None => {
				// For uncompressed data, use the buffered reader as-is
				Box::new(buf_reader)
			}
			CompressionMode::Lz4 => {
				// For LZ4, wrap the buffered reader with LZ4 decoder
				let decoder = Lz4Decoder::new(buf_reader)?;
				Box::new(decoder)
			}
		};
		// Return the reader
		Ok(Self {
			inner,
		})
	}
}

impl Read for CompressedReader {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		self.inner.read(buf)
	}
}
