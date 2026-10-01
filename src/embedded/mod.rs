//! Asynchronous COBS encoding and decoding using `embedded-io-async`.
//!
//! Available with the `embedded-io` feature:
//!
//! ```toml
//! [dependencies]
//! cobs-io-async = { version = "0.0.2", default-features = false, features = ["embedded-io"] }
//! ```
//!
//! This backend supports `no_std` environments and does not require heap
//! allocation for codec buffering. It does not provide an executor.
//! Sources and destinations implement the traits from [`embedded_io_async`];
//! standard-library and Tokio I/O types require compatible adapters.
//!
//! # Choosing an API
//!
//! | Operation | API | I/O requirements |
//! |---|---|---|
//! | Encode a complete slice | [`encode_from_slice_async`] | Destination: `Write` |
//! | Encode a slice with surrounding delimiters | [`encode_from_slice_including_sentinels_async`] | Destination: `Write` |
//! | Encode successive payload chunks | [`CobsEncoderAsync`] | Destination: `Write + Seek`; reader input: `Read` |
//! | Decode one frame into a slice | [`decode_to_slice_async`] | Source: `Read` |
//! | Decode one frame into a slice with batched reads | [`decode_to_slice_buffered_async`] | Source: `BufRead` |
//! | Decode successive encoded chunks | [`CobsDecoderAsync`] | Destination: `Write`; reader input: `Read`, or `BufRead` for [`push_buffered_async`](CobsDecoderAsync::push_buffered_async) |
//!
//! `Read`, `BufRead`, `Write`, and `Seek` refer to the `embedded_io_async` traits.
//! Only incremental encoding requires a seekable destination, because it
//! backpatches earlier code bytes. Decoding never requires seeking.
//!
//! [`decode_to_slice_async`] reads one byte at a time so that it never
//! consumes input belonging to the next frame. [`decode_to_slice_buffered_async`]
//! and [`CobsDecoderAsync::push_buffered_async`] keep that guarantee while
//! inspecting and consuming buffered input in larger steps. For input already
//! in memory, the synchronous [`crate::sync`] module avoids asynchronous I/O
//! altogether.
//!
//! # Framing
//!
//! Zero is the fixed frame delimiter. Payload zeros are encoded as data.
//! [`encode_from_slice_async`] produces an undelimited frame body;
//! [`encode_from_slice_including_sentinels_async`] adds one leading and one
//! trailing zero.
//!
//! Successive encoder pushes form one body. Call
//! [`CobsEncoderAsync::finalize_async`] explicitly to complete it.
//!
//! Decoder pushes stop at input exhaustion or the first delimiter completing
//! or invalidating an active frame. Input exhaustion alone does not finish
//! the frame. Use [`CobsDecoderAsync::finish_frame`] only when the application
//! independently knows the boundary of an undelimited frame.
//!
//! [`decode_to_slice_async`] also accepts structurally complete EOF.
//! Structural completeness cannot detect truncation at a COBS block boundary.
//! Leading zeros are padding; `[1, 0]` is a valid empty frame.
//!
//! # I/O errors and cancellation
//!
//! Operations do not explicitly flush or truncate destinations.
//! I/O errors are wrapped in [`crate::EncodeError`] or [`crate::DecodeError`],
//! preserving the underlying error types. One-shot decoding reports
//! [`crate::SeekableError::OutOfBounds`] when its output slice is too small.
//!
//! Errors and cancellation may leave consumed input and partial output.
//! Cancelling a polled, unfinished stateful operation leaves it poisoned;
//! dropping an unpolled future has no effect.
//!
//! [`CobsEncoderAsync::reset_async`] establishes a new output boundary.
//! [`CobsDecoderAsync::discard_frame_async`] consumes input through the next
//! delimiter. Neither operation rolls back previous I/O. An
//! [`InvalidFrame`](crate::DecodeError::InvalidFrame) result already consumed
//! the bad frame's delimiter, so another push may process the next frame
//! without discarding.
//!
//! # Example
//!
//! Decode a delimited frame representing `[7, 0, 8]`. This host-side example
//! uses `futures` as its executor; applications may use another executor.
//!
//! ```
//! use cobs_io_async::embedded::decode_to_slice_async;
//!
//! # futures::executor::block_on(async {
//! let mut source: &[u8] = &[2, 7, 2, 8, 0];
//! let mut output = [0u8; 3];
//!
//! let written = decode_to_slice_async(&mut source, &mut output)
//!     .await
//!     .unwrap();
//!
//! assert_eq!(written, 3);
//! assert_eq!(output, [7, 0, 8]);
//! assert!(source.is_empty());
//! # });
//! ```

mod decode;
mod encode;

#[doc(inline)]
pub use decode::{
    CobsDecoderAsync, CobsDecoderSliceAsync, decode_to_slice_async, decode_to_slice_buffered_async,
};

pub use embedded_io_async::{BufRead, ErrorType, Read, Seek, Write};

#[doc(inline)]
pub use encode::{
    CobsEncoderAsync, CobsEncoderSliceAsync, encode_from_slice_async,
    encode_from_slice_including_sentinels_async,
};

use crate::codec::DEFAULT_BUF_SIZE;
use crate::{CodecError, DecodeError, EncodeError, SeekableError};

/// COBS codec wrapper around an `embedded-io` stream.
///
/// Implements [`Read`] and [`Write`] when `S` does. Reads return decoded
/// frame payloads, and writes encode `buf` as a single zero-delimited frame.
pub struct CobsAsync<S> {
    stream: S,
    decoder: CobsDecoderSliceAsync<DEFAULT_BUF_SIZE>,
}

impl<S> CobsAsync<S> {
    /// Creates a codec wrapper around `stream`.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            decoder: CobsDecoderAsync::new_to_slice([0u8; DEFAULT_BUF_SIZE]),
        }
    }
}

impl<S> ErrorType for CobsAsync<S>
where
    S: Read + Write,
{
    type Error =
        CodecError<DecodeError<S::Error, SeekableError>, EncodeError<SeekableError, S::Error>>;
}

impl<S> Read for CobsAsync<S>
where
    S: Read + Write,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        loop {
            match self.decoder.push_async(&mut self.stream).await {
                Ok(progress) => {
                    if let Some(frame_len) = progress.frame_len.map(|s| s as usize) {
                        buf[..frame_len].copy_from_slice(&self.decoder.dest()[..frame_len]);
                        return Ok(frame_len);
                    }
                }
                Err(e) => return Err(CodecError::Decode(e)),
            }
        }
    }
}

impl<S> Write for CobsAsync<S>
where
    S: Read + Write,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        let n = encode_from_slice_including_sentinels_async(buf, &mut self.stream)
            .await
            .map_err(CodecError::Encode)?;
        Ok(n as usize)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
