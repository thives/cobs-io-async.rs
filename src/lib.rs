//! # Asynchronous COBS encoding and decoding
//!
//! Encode and decode Consistent Overhead Byte Stuffing (COBS) frames using
//! independent embedded and Tokio asynchronous I/O backends.
//!
//! COBS transforms a payload into an encoded body containing no zero bytes.
//! Zero can then delimit frames in a byte stream. Payload zeros are encoded
//! as data; this crate uses zero as its fixed delimiter.
//!
//! The Cargo package is `cobs-io-async`; the Rust import name is
//! `cobs_io_async`.
//!
//! # Choose a backend
//!
//! No Cargo features are enabled by default. Enable the backend matching
//! your application's I/O types:
//!
//! | Feature | Module | I/O traits |
//! |---|---|---|
//! | `embedded-io` | `cobs_io_async::embedded` | `embedded_io_async::{Read, Write, Seek}` |
//! | `tokio` | `cobs_io_async::tokio` | `tokio::io::{AsyncRead, AsyncWrite, AsyncSeek}` |
//!
//! For example, to use the embedded backend:
//!
//! ```toml
//! [dependencies]
//! cobs-io-async = { version = "0.1", default-features = false, features = ["embedded-io"] }
//! ```
//!
//! For Tokio, select `features = ["tokio"]` instead. Both backends may be
//! enabled together; their entry points remain in separate modules.
//!
//! The embedded backend supports `no_std`. The Tokio backend enables this
//! crate's `std` feature. Neither backend creates an executor or spawns tasks;
//! executor and runtime requirements depend on the supplied I/O types.
//!
//! Without a backend, the crate still exposes size helpers, [`DecodeProgress`],
//! and the shared error types.
//!
//! # Encoding
//!
//! Both backend modules provide:
//!
//! - `encode_from_slice_async`: encodes a complete payload as an undelimited
//!   body. Each call produces a separate body.
//! - `encode_from_slice_including_sentinels_async`: adds one leading and one
//!   trailing zero delimiter around a complete body's encoding.
//! - `CobsEncoderAsync`: combines successive payload chunks into one body.
//!   Call `finalize_async` explicitly to complete it.
//!
//! Incremental encoding requires a seekable destination because it
//! backpatches code bytes. The one-shot slice helpers write sequentially
//! and do not require destination seeking.
//!
//! An empty payload encodes as `[1]`, or `[0, 1, 0]` with surrounding
//! delimiters. A terminal full block of 254 nonzero payload bytes does not
//! receive a redundant trailing code byte.
//!
//! Use [`max_encoding_length`] to size a buffer for an undelimited body.
//! Framed encoding requires two additional bytes; use checked arithmetic
//! when calculating sizes from untrusted or potentially large lengths.
//!
//! # Decoding and frame boundaries
//!
//! Both backend modules provide `CobsDecoderAsync` for incremental decoding
//! and `decode_to_slice_async` for decoding one frame into a caller-provided
//! slice. Decoding does not require seeking.
//!
//! **Input exhaustion is not frame completion for a stateful decoder.**
//! A push stops at input exhaustion or the first delimiter completing or
//! invalidating an active frame. The delimiter is consumed, but subsequent
//! encoded bytes are not requested by the decoder.
//!
//! For an undelimited frame, the application must independently establish
//! its boundary and call `finish_frame`. The `check_complete` method only
//! checks structural completeness; it neither finishes the frame nor proves
//! that the message was not truncated at a COBS block boundary.
//!
//! The one-shot `decode_to_slice_async` helper accepts either a completing
//! delimiter or structurally complete EOF after a frame has started.
//! Use the stateful decoder when the protocol requires explicit delimiter
//! completion.
//!
//! Zeros while no frame is active are ignored as padding. `[1, 0]` completes
//! a valid empty frame, whereas `[0]` does not. [`DecodeProgress`] distinguishes
//! per-call input/output counts from the cumulative length of a completed
//! frame.
//!
//! # Errors, cancellation, and recovery
//!
//! Codec operations do not explicitly flush or truncate destinations.
//! Normal codec processing uses no heap allocation; user-provided I/O,
//! executors, and backend error construction may allocate.
//!
//! Operations are not transactional. Errors and cancellation can leave input
//! consumed and output partially written. Dropping an unpolled future has
//! no effect. Cancelling a polled, unfinished stateful operation leaves the
//! encoder or decoder poisoned, even if individual I/O operations are
//! cancellation-safe.
//!
//! Encoder `reset_async` establishes a new output boundary. Decoder
//! `discard_frame_async` consumes input through the next delimiter.
//! Recovery does not undo earlier I/O or resume the abandoned frame.
//!
//! [`DecodeError::InvalidFrame`] is different from an I/O failure: the invalid
//! frame's delimiter has already been consumed and framing reset. Another
//! push can process the next frame without discarding first.
//!
//! [`EncodeError`] and [`DecodeError`] preserve backend error types through
//! their generic parameters. [`CompletionError`] describes failures to check
//! or explicitly finish decoder state. [`SeekableError`] represents
//! slice-buffer bounds errors in APIs that expose it.
//!
//! # Additional features
//!
//! | Feature | Effect |
//! |---|---|
//! | `std` | Enables standard-library integration for the embedded I/O dependency when it is also enabled. Does not select a backend. |
//! | `serde` | Enables serialization and deserialization of progress and error types, subject to their generic parameter bounds. Encoder and decoder state is not serializable. |
//! | `defmt` | Enables compact diagnostic formatting for supported types. See individual types for implementations and bounds. |
//!
//! There is no separate `alloc` feature. Shared error types implement
//! [`core::error::Error`] without requiring `std`, subject to applicable
//! generic bounds.
//!
//! # Example
//!
//! Encode and decode a delimited frame using the `embedded-io` backend.
//! The buffers are fixed-size; `futures` supplies the host executor for this
//! example only.
//!
//! ```
//! # #[cfg(feature = "embedded-io")]
//! # {
//! use cobs_io_async::{
//!     embedded::{
//!         decode_to_slice_async,
//!         encode_from_slice_including_sentinels_async,
//!     },
//!     max_encoding_length,
//! };
//!
//! # futures::executor::block_on(async {
//! let payload = [7, 0, 8];
//! let mut encoded = [0u8; max_encoding_length(3) + 2];
//!
//! let encoded_len = {
//!     let mut writer = &mut encoded[..];
//!     encode_from_slice_including_sentinels_async(&payload, &mut writer)
//!         .await
//!         .unwrap()
//! };
//!
//! assert_eq!(&encoded[..encoded_len as usize], &[0, 2, 7, 2, 8, 0]);
//!
//! let mut reader = &encoded[..encoded_len as usize];
//! let mut decoded = [0u8; 3];
//! let decoded_len = decode_to_slice_async(&mut reader, &mut decoded)
//!     .await
//!     .unwrap();
//!
//! assert_eq!(decoded_len, payload.len() as u64);
//! assert_eq!(decoded, payload);
//! assert!(reader.is_empty());
//! # });
//! # }
//! ```
//!
//! # Protocol limitations
//!
//! COBS provides framing, not integrity or authenticity. A structurally valid
//! frame may still contain corrupted or truncated application data. Add an
//! appropriate checksum, authentication mechanism, or independently known
//! message length when the protocol requires it.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

#[cfg(test)]
extern crate std;

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
pub use codec::decode::DecodeProgress;
#[cfg(any(feature = "embedded-io", feature = "tokio"))]
pub use error::{CompletionError, DecodeError, EncodeError, SeekableError};

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
mod codec;
#[cfg(any(feature = "embedded-io", feature = "tokio"))]
mod error;

#[cfg(test)]
mod tests;

#[cfg(feature = "embedded-io")]
#[cfg_attr(docsrs, doc(cfg(feature = "embedded-io")))]
pub mod embedded;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

/// Returns the maximum additional bytes needed for an undelimited encoding.
///
/// Returns `1` for empty input; otherwise returns `source_len.div_ceil(254)`.
/// The result excludes leading and trailing frame delimiters.
///
/// This is an upper bound across payloads of the given length, not necessarily
/// the actual overhead for a particular payload.
///
/// See [`max_encoding_length`] for the corresponding total buffer size.
#[inline]
pub const fn max_encoding_overhead(source_len: usize) -> usize {
    if source_len == 0 {
        return 1;
    }
    source_len.div_ceil(254)
}

/// Returns the maximum encoded body length for `source_len` payload bytes.
///
/// The result is `source_len + max_encoding_overhead(source_len)`.
/// It excludes frame delimiters. To surround the body with leading and
/// trailing zero delimiters, reserve two additional bytes, checking that
/// the addition fits.
///
/// Empty input requires one byte. For example, 254 payload bytes require at
/// most 255 encoded bytes.
///
/// # Overflow
///
/// The addition is not checked explicitly. Overflow panics when overflow
/// checks are enabled and otherwise wraps; constant evaluation rejects it.
///
/// For potentially unrepresentable sizes, use
/// `source_len.checked_add(max_encoding_overhead(source_len))` instead.
#[inline]
pub const fn max_encoding_length(source_len: usize) -> usize {
    source_len + max_encoding_overhead(source_len)
}
