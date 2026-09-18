use crate::{CompletionError, SeekableError};
use ::embedded_io_async::{ErrorType, Read, Write};

use crate::codec::decode::define_decoder;

define_decoder! {
    D: [Write],
    S: [Read + ?Sized],
    errors: [S::Error, D::Error],
    slice_source_error: SeekableError,
    slice_dest_error: SeekableError,
}

/// Decodes one COBS frame into the beginning of `dest`.
///
/// Returns the decoded payload length. On success, bytes after that prefix
/// of `dest` remain unchanged. Capacity is required for the decoded payload,
/// not for COBS code bytes or delimiters.
///
/// Leading zeros are ignored as padding. A terminating zero completes a
/// structurally complete frame and is consumed. The decoder does not request
/// bytes after that delimiter; subsequent input remains available through
/// `source`.
///
/// If a read returns `Ok(0)` first, this function accepts the frame when one
/// has started and its current COBS block is structurally complete.
/// `[1]` followed by EOF and `[1, 0]` both decode to zero bytes.
///
/// EOF acceptance cannot detect truncation at a block boundary. Bound the
/// source to the intended frame, or require delimiter completion through
/// [`CobsDecoderAsync`], when the protocol needs that distinction.
///
/// For separately delivered chunks, use [`CobsDecoderAsync`] rather than
/// repeatedly calling this one-shot function.
///
/// # Errors
///
/// - Source errors produce [`DecodeError::Source`].
/// - Insufficient output capacity produces
///   `DecodeError::Destination(SeekableError::OutOfBounds)`.
/// - A delimiter inside an incomplete block produces
///   [`DecodeError::InvalidFrame`], including per-call progress.
/// - EOF inside an incomplete block produces
///   [`DecodeError::UnexpectedSourceEof`].
/// - EOF without a non-padding encoded byte produces [`DecodeError::EmptyFrame`].
///
/// Errors leave partial output intact. On an output-capacity error, the encoded
/// byte producing the next decoded byte has already been consumed. That byte may
/// be a payload byte or a code byte requiring a reconstructed zero.
///
/// Only invalid-frame errors include progress, including the invalidating delimiter.
/// Other errors provide no decoded-length result or reliable retry offset.
///
/// # Cancellation
///
/// Cancelling after polling can leave input consumed and `dest` partially
/// overwritten. Internal decoder state is lost; restarting the function
/// does not resume the interrupted frame. No rollback or retry offset is
/// provided.
///
/// Dropping an unpolled future performs no I/O.
pub async fn decode_to_slice_async<S>(
    source: &mut S,
    dest: &mut [u8],
) -> Result<u64, DecodeError<S::Error, SeekableError>>
where
    S: Read + ?Sized,
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
                let slot = output
                    .next()
                    .ok_or(DecodeError::Destination(SeekableError::OutOfBounds))?;
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

impl<D> ErrorType for CobsDecoderAsync<D>
where
    D: Write,
{
    type Error = DecodeError<SeekableError, D::Error>;
}

impl<D> Write for CobsDecoderAsync<D>
where
    D: Write,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, DecodeError<SeekableError, D::Error>> {
        let mut curr_buf = buf;
        let mut progress = Ok(DecodeProgress::default());
        loop {
            if curr_buf.is_empty() {
                return Ok(progress?.written as usize);
            }
            progress = self.push_slice_async(curr_buf).await;
            match progress {
                Ok(DecodeProgress {
                    consumed,
                    frame_len,
                    ..
                }) => {
                    if let Some(frame_len) = frame_len {
                        return Ok(frame_len as usize);
                    }
                    curr_buf = &curr_buf[consumed as usize..];
                    continue;
                }
                Err(DecodeError::InvalidFrame(progress)) => {
                    curr_buf = &curr_buf[progress.consumed as usize..];
                    continue;
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
    }

    async fn flush(&mut self) -> Result<(), DecodeError<SeekableError, D::Error>> {
        Ok(())
    }
}

impl ErrorType for CobsDecoderSliceAsync<'_> {
    type Error = DecodeError<SeekableError, SeekableError>;
}

impl Write for CobsDecoderSliceAsync<'_> {
    async fn write(
        &mut self,
        buf: &[u8],
    ) -> Result<usize, DecodeError<SeekableError, SeekableError>> {
        self.0.write(buf).await
    }

    async fn flush(&mut self) -> Result<(), DecodeError<SeekableError, SeekableError>> {
        Ok(())
    }
}
