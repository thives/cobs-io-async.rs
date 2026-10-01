use crate::SeekableError;
use ::embedded_io_async::{BufRead, ErrorType, Read, Write};

use crate::codec::decode::{SliceFrameDecoder, define_decoder};

define_decoder! {
    D: [Write],
    S: [Read + ?Sized],
    buffered: [BufRead + ?Sized],
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
/// # Read granularity
///
/// Because a plain [`Read`] source cannot return unconsumed bytes, this
/// function requests one byte per read so that it never consumes input after
/// the delimiter. When the source implements [`BufRead`], prefer
/// [`decode_to_slice_buffered_async`], which batches reads with the same
/// frame-boundary behavior.
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
    let mut decoder = SliceFrameDecoder::new(dest);
    let mut byte = [0u8; 1];
    loop {
        let n = source.read(&mut byte).await.map_err(DecodeError::Source)?;
        if n == 0 {
            return decoder.finish_at_eof();
        }
        let (_, status) = decoder.push(&byte);
        if let Some(result) = decoder.outcome(status, || SeekableError::OutOfBounds) {
            return result;
        }
    }
}

/// Decodes one COBS frame from a buffered source into the beginning of `dest`.
///
/// Behaves like [`decode_to_slice_async`], including its padding, EOF,
/// capacity, error, and cancellation behavior, but inspects the source's
/// buffered input through [`BufRead::fill_buf`] instead of reading one byte
/// at a time. Only bytes through the frame's terminating delimiter are
/// [consumed](BufRead::consume); bytes belonging to subsequent frames remain
/// buffered in `source`.
///
/// On a capacity error, input is consumed through the encoded byte that
/// produced the rejected output byte, matching [`decode_to_slice_async`].
///
/// # Example
///
/// ```
/// use cobs_io_async::embedded::decode_to_slice_buffered_async;
///
/// # futures::executor::block_on(async {
/// let mut source: &[u8] = &[0, 2, 7, 2, 8, 0, 2, 9, 0];
/// let mut output = [0u8; 3];
///
/// let written = decode_to_slice_buffered_async(&mut source, &mut output)
///     .await
///     .unwrap();
///
/// assert_eq!(written, 3);
/// assert_eq!(output, [7, 0, 8]);
/// assert_eq!(source, &[2, 9, 0]);
/// # });
/// ```
pub async fn decode_to_slice_buffered_async<S>(
    source: &mut S,
    dest: &mut [u8],
) -> Result<u64, DecodeError<S::Error, SeekableError>>
where
    S: BufRead + ?Sized,
{
    let mut decoder = SliceFrameDecoder::new(dest);
    loop {
        let input = source.fill_buf().await.map_err(DecodeError::Source)?;
        if input.is_empty() {
            return decoder.finish_at_eof();
        }
        let (consumed, status) = decoder.push(input);
        source.consume(consumed);
        if let Some(result) = decoder.outcome(status, || SeekableError::OutOfBounds) {
            return result;
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

impl<const N: usize> ErrorType for CobsDecoderSliceAsync<N> {
    type Error = DecodeError<SeekableError, SeekableError>;
}

impl<const N: usize> Write for CobsDecoderSliceAsync<N> {
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
