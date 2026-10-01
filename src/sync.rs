//! Synchronous COBS encoding and decoding between in-memory slices.
//!
//! This module is always available, including with
//! `default-features = false` and in `no_std` builds. It needs no backend
//! feature, executor, or heap allocation, and uses the same framing rules
//! as the asynchronous backends.
//!
//! | Operation | API |
//! |---|---|
//! | Encode a complete payload as an undelimited body | [`encode_from_slice`] |
//! | Encode a payload with surrounding zero delimiters | [`encode_from_slice_including_sentinels`] |
//! | Decode one frame from the start of a slice | [`decode_to_slice`] |
//!
//! Size encoding buffers with [`max_encoding_length`](crate::max_encoding_length),
//! adding two bytes for delimiters. Decoding requires capacity only for the
//! decoded payload.
//!
//! # Example
//!
//! Decode consecutive frames from one buffer, using
//! [`DecodedFrame::consumed`] to locate the next frame.
//!
//! ```
//! use cobs_io_async::{max_encoding_length, sync};
//!
//! let mut stream = [0u8; 2 * (max_encoding_length(3) + 2)];
//! let first = sync::encode_from_slice_including_sentinels(&[7, 0, 8], &mut stream).unwrap();
//! let second =
//!     sync::encode_from_slice_including_sentinels(&[9], &mut stream[first..]).unwrap();
//! let mut input = &stream[..first + second];
//!
//! let mut payload = [0u8; 3];
//! let frame = sync::decode_to_slice(input, &mut payload).unwrap();
//! assert_eq!(&payload[..frame.len], &[7, 0, 8]);
//! input = &input[frame.consumed..];
//!
//! let frame = sync::decode_to_slice(input, &mut payload).unwrap();
//! assert_eq!(&payload[..frame.len], &[9]);
//! assert!(input[frame.consumed..].is_empty());
//! ```

use core::convert::Infallible;

use crate::codec::decode::SliceFrameDecoder;
use crate::codec::encode::{EncoderState, PushResult};
use crate::{DecodeError, SeekableError};

/// Result of successfully decoding one frame with [`decode_to_slice`].
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct DecodedFrame {
    /// Number of decoded payload bytes written to the start of the destination.
    pub len: usize,

    /// Number of encoded bytes processed from the start of the source.
    ///
    /// Includes leading zero padding and the completing delimiter, if any.
    /// Input following the frame starts at `source[consumed..]`.
    pub consumed: usize,
}

/// Encodes all of `source` as one complete, undelimited COBS body into `dest`.
///
/// Returns the encoded length; the body occupies `dest[..len]`. Payload zeros
/// are encoded as data. Empty input produces `[1]`. A terminal full block of
/// 254 nonzero payload bytes does not receive a redundant trailing code byte.
///
/// A destination of [`max_encoding_length(source.len())`](crate::max_encoding_length)
/// bytes is always sufficient.
///
/// # Errors
///
/// Returns [`SeekableError::OutOfBounds`] if `dest` is too small. The
/// destination may then have been partially overwritten.
///
/// # Example
///
/// ```
/// use cobs_io_async::sync::encode_from_slice;
///
/// let mut encoded = [0u8; 4];
/// let len = encode_from_slice(&[7, 0, 8], &mut encoded).unwrap();
/// assert_eq!(&encoded[..len], &[2, 7, 2, 8]);
/// ```
pub fn encode_from_slice(source: &[u8], dest: &mut [u8]) -> Result<usize, SeekableError> {
    let mut output = SliceOutput { dest, len: 0 };
    let mut state = EncoderState::default();
    let mut needs_code_placeholder = false;
    output.append(0)?;
    for &byte in source {
        if needs_code_placeholder {
            output.append(0)?;
            needs_code_placeholder = false;
        }
        match state.push(byte).ok_or(SeekableError::OutOfBounds)? {
            PushResult::AddSingle(byte) => output.append(byte)?,
            PushResult::ModifyFromStartAndSkip((idx, code)) => {
                output.set(idx, code)?;
                output.append(0)?;
            }
            PushResult::ModifyFromStartAndPushAndSkip((idx, code, byte)) => {
                output.set(idx, code)?;
                output.append(byte)?;
                needs_code_placeholder = true;
            }
        }
    }
    if !needs_code_placeholder {
        let (idx, code) = state.finalize();
        output.set(idx, code)?;
    }
    Ok(output.len)
}

/// Encodes `source` with one leading and one trailing zero delimiter.
///
/// On success, `dest[..len]` contains `[0, encoded_body..., 0]`. Empty input
/// produces `[0, 1, 0]`. Payload zeros remain data and do not split the frame.
///
/// Reserve [`max_encoding_length(source.len())`](crate::max_encoding_length)
/// plus two bytes.
///
/// # Errors
///
/// Returns [`SeekableError::OutOfBounds`] if `dest` is too small. The
/// destination may then have been partially overwritten.
pub fn encode_from_slice_including_sentinels(
    source: &[u8],
    dest: &mut [u8],
) -> Result<usize, SeekableError> {
    let (first, rest) = dest.split_first_mut().ok_or(SeekableError::OutOfBounds)?;
    *first = 0;
    let body_len = encode_from_slice(source, rest)?;
    *rest.get_mut(body_len).ok_or(SeekableError::OutOfBounds)? = 0;
    Ok(body_len + 2)
}

/// Decodes the first COBS frame in `source` into the beginning of `dest`.
///
/// Leading zeros are ignored as padding. Decoding stops after the zero
/// delimiter completing the frame; bytes after it are not examined, and
/// [`DecodedFrame::consumed`] identifies where they begin.
///
/// If `source` ends before a delimiter, the frame is accepted when one has
/// started and its current COBS block is structurally complete. Both `[1]`
/// and `[1, 0]` decode to an empty payload. This end-of-input acceptance
/// cannot detect truncation at a COBS block boundary; check that `consumed`
/// includes a delimiter (`source[consumed - 1] == 0`) when the protocol
/// requires delimited frames.
///
/// On success, bytes of `dest` after the decoded payload remain unchanged.
///
/// # Errors
///
/// - `DecodeError::Destination(SeekableError::OutOfBounds)`: `dest` cannot
///   hold the decoded payload.
/// - [`DecodeError::InvalidFrame`]: a delimiter arrived inside an incomplete
///   block. The attached progress counts the invalidating delimiter, so the
///   next frame starts at `source[progress.consumed as usize..]`.
/// - [`DecodeError::UnexpectedSourceEof`]: `source` ended inside an
///   incomplete block.
/// - [`DecodeError::EmptyFrame`]: `source` is empty or contains only padding.
///
/// Errors may leave `dest` partially overwritten. Source and poisoning errors
/// are never returned.
///
/// # Example
///
/// ```
/// use cobs_io_async::sync::decode_to_slice;
///
/// let source = [0, 2, 7, 2, 8, 0, 2, 9, 0];
/// let mut output = [0x80u8; 5];
/// let frame = decode_to_slice(&source, &mut output).unwrap();
///
/// assert_eq!(frame.len, 3);
/// assert_eq!(output, [7, 0, 8, 0x80, 0x80]);
/// assert_eq!(&source[frame.consumed..], &[2, 9, 0]);
/// ```
pub fn decode_to_slice(
    source: &[u8],
    dest: &mut [u8],
) -> Result<DecodedFrame, DecodeError<Infallible, SeekableError>> {
    let mut decoder = SliceFrameDecoder::new(dest);
    let (consumed, status) = decoder.push(source);
    let len = match decoder.outcome(status, || SeekableError::OutOfBounds) {
        Some(result) => result?,
        None => decoder.finish_at_eof()?,
    };
    Ok(DecodedFrame {
        // The decoded length never exceeds `dest.len()`.
        len: len as usize,
        consumed,
    })
}

struct SliceOutput<'a> {
    dest: &'a mut [u8],
    len: usize,
}

impl SliceOutput<'_> {
    #[inline]
    fn append(&mut self, byte: u8) -> Result<(), SeekableError> {
        *self
            .dest
            .get_mut(self.len)
            .ok_or(SeekableError::OutOfBounds)? = byte;
        self.len += 1;
        Ok(())
    }

    #[inline]
    fn set(&mut self, idx: u64, byte: u8) -> Result<(), SeekableError> {
        let slot = usize::try_from(idx)
            .ok()
            .and_then(|idx| self.dest.get_mut(idx))
            .ok_or(SeekableError::OutOfBounds)?;
        *slot = byte;
        Ok(())
    }
}
