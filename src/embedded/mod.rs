//! Asynchronous COBS encoding and decoding using `embedded-io-async`.
//!
//! Available with the `embedded-io` feature:
//!
//! ```toml
//! [dependencies]
//! cobs-io-async = { version = "0.1", default-features = false, features = ["embedded-io"] }
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
//! | Decode successive encoded chunks | [`CobsDecoderAsync`] | Destination: `Write`; reader input: `Read` |
//!
//! `Read`, `Write`, and `Seek` refer to the `embedded_io_async` traits.
//! Only incremental encoding requires a seekable destination, because it
//! backpatches earlier code bytes. Decoding never requires seeking.
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
pub use decode::{CobsDecoderAsync, CobsDecoderSliceAsync, decode_to_slice_async};

pub use embedded_io_async::Write;

#[doc(inline)]
pub use encode::{
    CobsEncoderAsync, CobsEncoderSliceAsync, encode_from_slice_async,
    encode_from_slice_including_sentinels_async,
};
