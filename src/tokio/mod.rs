//! Asynchronous COBS encoding and decoding using Tokio I/O traits.
//!
//! Available with the `tokio` feature, which also enables this crate's
//! `std` feature:
//!
//! ```toml
//! [dependencies]
//! cobs-io-async = { version = "0.1", features = ["tokio"] }
//! ```
//!
//! This backend uses Tokio's `AsyncRead`, `AsyncWrite`, and `AsyncSeek`
//! traits. It does not construct a runtime or spawn tasks. Whether a runtime
//! is required depends on the supplied I/O types; in-memory slice I/O can
//! complete without one.
//!
//! Enable any additional Tokio features needed by your application, such as
//! runtime, networking, or filesystem support, in its own Tokio dependency.
//!
//! # Choosing an API
//!
//! | Operation | API | I/O requirements |
//! |---|---|---|
//! | Encode a complete slice | [`encode_from_slice_async`] | Destination: `AsyncWrite + Unpin` |
//! | Encode a slice with surrounding delimiters | [`encode_from_slice_including_sentinels_async`] | Destination: `AsyncWrite + Unpin` |
//! | Encode successive payload chunks | [`CobsEncoderAsync`] | Destination: `AsyncWrite + AsyncSeek + Unpin`; reader input: `AsyncRead + Unpin` |
//! | Decode one frame into a slice | [`decode_to_slice_async`] | Source: `AsyncRead + Unpin` |
//! | Decode successive encoded chunks | [`CobsDecoderAsync`] | Destination: `AsyncWrite + Unpin`; reader input: `AsyncRead + Unpin` |
//!
//! Only incremental encoding requires a seekable destination, because it
//! backpatches earlier code bytes. Decoding never requires seeking.
//! The APIs do not impose `Send` or `'static` bounds; additional bounds may
//! be required when moving their futures between threads or spawning tasks.
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
//! Operations do not explicitly flush, shut down, or truncate destinations.
//! Tokio I/O failures use [`std::io::Error`] and are wrapped in
//! [`crate::EncodeError`] or [`crate::DecodeError`].
//!
//! Errors and cancellation may leave consumed input and partial output.
//! Cancelling a polled, unfinished stateful operation leaves it poisoned;
//! dropping an unpolled future has no effect. Individual Tokio operations
//! being cancellation-safe does not make an entire codec operation
//! cancellation-safe.
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
//! Decode a delimited frame representing `[7, 0, 8]`. The slice reader does
//! not require a Tokio runtime, so this example uses the `futures` executor.
//!
//! ```
//! use cobs_io_async::tokio::decode_to_slice_async;
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
pub use decode::{CobsDecoderAsync, CobsDecoderSliceAsync, decode_to_slice_async};

#[doc(inline)]
pub use encode::{
    CobsEncoderAsync, CobsEncoderSliceAsync, encode_from_slice_async,
    encode_from_slice_including_sentinels_async,
};
