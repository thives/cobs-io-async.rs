use crate::CompletionError;
use ::tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::io;

use crate::codec::decode::define_decoder;

define_decoder! {
    D: [AsyncWrite + Unpin],
    S: [AsyncRead + Unpin + ?Sized],
    errors: [io::Error, io::Error],
}

/// Decodes one COBS frame into the beginning of `dest`.
///
/// Reads from the current position of a Tokio `AsyncRead + Unpin` source.
/// The source need not support seeking and may be unsized. Decoded bytes
/// are written directly into the supplied slice.
///
/// Returns the decoded payload length. Capacity is required only for decoded
/// payload bytes, not code bytes or delimiters. Bytes after the written prefix
/// of `dest` remain unchanged.
///
/// # Frame boundaries
///
/// Leading zeros are ignored as padding. A zero delimiter completes a
/// structurally complete frame and is consumed. The decoder does not request
/// bytes after that delimiter; subsequent input remains available through
/// `source`.
///
/// If a read returns `Ok(0)` before a delimiter, the function accepts EOF only
/// when a non-padding frame has started and its current COBS block is
/// structurally complete. Both `[1]` followed by EOF and `[1, 0]` decode to an
/// empty payload. Empty or padding-only input does not constitute a frame.
///
/// EOF acceptance cannot detect truncation at a COBS block boundary. Use
/// [`CobsDecoderAsync`] when the protocol requires explicit delimiter
/// completion or when encoded input arrives in separately delivered chunks.
///
/// Each invocation creates fresh decoding state. Repeated calls do not
/// combine incomplete input into one frame.
///
/// # Errors
///
/// - [`Source`](crate::DecodeError::Source): reading the source failed.
/// - [`Destination`](crate::DecodeError::Destination): `dest` cannot hold the
///   next decoded byte. Contains a `std::io::Error` with
///   `std::io::ErrorKind::InvalidInput`, wrapping
///   [`SeekableError::OutOfBounds`](crate::SeekableError::OutOfBounds).
/// - [`InvalidFrame`](crate::DecodeError::InvalidFrame): a delimiter appeared
///   before the current block contained enough payload bytes. Includes
///   [`DecodeProgress`](crate::DecodeProgress) for this call, counting the
///   invalidating delimiter as consumed.
/// - [`UnexpectedSourceEof`](crate::DecodeError::UnexpectedSourceEof): input
///   ended inside an incomplete block.
/// - [`EmptyFrame`](crate::DecodeError::EmptyFrame): input ended without
///   starting a non-padding encoded frame.
///
/// Errors preserve partial output. On a capacity error, the encoded byte
/// producing the next output byte has already been consumed. That byte may
/// be a payload byte or a code byte requiring a reconstructed zero.
///
/// Only invalid-frame errors include progress, including the invalidating delimiter.
/// Other errors provide no decoded-length result or reliable retry offset.
///
/// # Cancellation
///
/// Dropping an unpolled future performs no I/O. Cancelling after polling may
/// leave input consumed and `dest` partially overwritten.
///
/// Local decoding state is lost when the future is dropped. Calling this
/// function again does not resume the interrupted frame. No rollback is
/// performed.
///
/// # Execution
///
/// This function neither creates a Tokio runtime nor spawns tasks. Executor
/// requirements depend on the source: in-memory slice reads can complete
/// without a Tokio runtime, whereas other Tokio I/O types may require one.
///
/// The source is not required to be `Send` or `'static`; additional bounds
/// may be necessary when spawning or moving the future between threads.
///
/// # Example
///
/// Decode one frame while preserving unused output capacity and the next
/// frame's encoded input. This in-memory example uses the `futures` executor.
///
/// ```
/// use cobs_io_async::tokio::decode_to_slice_async;
///
/// # futures::executor::block_on(async {
/// let mut source: &[u8] = &[0, 2, 7, 2, 8, 0, 2, 9, 0];
/// let mut output = [0x80u8; 5];
///
/// let written = decode_to_slice_async(&mut source, &mut output)
///     .await
///     .unwrap();
///
/// assert_eq!(written, 3);
/// assert_eq!(output, [7, 0, 8, 0x80, 0x80]);
/// assert_eq!(source, &[2, 9, 0]);
/// # });
/// ```
pub async fn decode_to_slice_async<S>(
    source: &mut S,
    dest: &mut [u8],
) -> Result<u64, DecodeError<io::Error, io::Error>>
where
    S: AsyncRead + Unpin + ?Sized,
{
    use crate::codec::decode::{DecodeAction, DecoderCore};
    let mut decoder = DecoderCore::new();
    let mut progress = DecodeProgress::default();
    let mut output = dest.iter_mut();
    let mut byte = [0u8; 1];
    loop {
        let n = source.read(&mut byte).await.map_err(DecodeError::Source)?;
        if n == 0 {
            return decoder.finish_frame().map_err(|error| match error {
                CompletionError::IncompleteFrame(_) => DecodeError::UnexpectedSourceEof,
                CompletionError::InvalidState => DecodeError::Poisoned,
                CompletionError::NoFrame => DecodeError::EmptyFrame,
            });
        }
        progress.consumed += 1;
        match decoder.accept_byte(byte[0]) {
            DecodeAction::Skip => {}
            DecodeAction::Write(value) => {
                let slot = output.next().ok_or_else(|| {
                    DecodeError::Destination(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        crate::SeekableError::OutOfBounds,
                    ))
                })?;
                *slot = value;
                decoder.acknowledge_write();
                progress.written += 1;
            }
            DecodeAction::FrameComplete(len) => return Ok(len),
            DecodeAction::InvalidFrame => {
                return Err(DecodeError::InvalidFrame(progress));
            }
        }
    }
}
