//! Asynchronous COBS encoding and decoding using Tokio I/O traits.
//!
//! Available with the `tokio` feature, which also enables this crate's
//! `std` feature:
//!
//! ```toml
//! [dependencies]
//! cobs-io-async = { version = "0.0.2", features = ["tokio"] }
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
//! | Decode one frame into a slice with batched reads | [`decode_to_slice_buffered_async`] | Source: `AsyncBufRead + Unpin` |
//! | Decode successive encoded chunks | [`CobsDecoderAsync`] | Destination: `AsyncWrite + Unpin`; reader input: `AsyncRead + Unpin`, or `AsyncBufRead + Unpin` for [`push_buffered_async`](CobsDecoderAsync::push_buffered_async) |
//!
//! Only incremental encoding requires a seekable destination, because it
//! backpatches earlier code bytes. Decoding never requires seeking.
//! The APIs do not impose `Send` or `'static` bounds; additional bounds may
//! be required when moving their futures between threads or spawning tasks.
//!
//! [`decode_to_slice_async`] reads one byte at a time so that it never
//! consumes input belonging to the next frame. [`decode_to_slice_buffered_async`]
//! and [`CobsDecoderAsync::push_buffered_async`] keep that guarantee while
//! inspecting and consuming buffered input in larger steps; wrap unbuffered
//! readers in `tokio::io::BufReader`. For input already in memory, the
//! synchronous [`crate::sync`] module avoids asynchronous I/O altogether.
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
pub use decode::{
    CobsDecoderAsync, CobsDecoderSliceAsync, decode_to_slice_async, decode_to_slice_buffered_async,
};

use core::convert::Infallible;
pub use tokio::io::{
    AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite, Error, ErrorKind, ReadBuf, Result,
};

#[doc(inline)]
pub use encode::{
    CobsEncoderAsync, CobsEncoderSliceAsync, encode_from_slice_async,
    encode_from_slice_including_sentinels_async,
};

use core::pin::{Pin, pin};
use core::task::{Context, Poll};

use crate::codec::DEFAULT_BUF_SIZE;
use crate::{CompletionError, DecodeError, EncodeError};

/// Bidirectional COBS codec wrapper around a Tokio stream.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`] when `S` does, so encoded
/// frames are written to and decoded from the wrapped stream. Writes are
/// finalized into a delimited frame by [`AsyncWrite::poll_shutdown`].
pub struct CobsAsync<S>
where
    S: Unpin,
{
    stream: S,
    encoder: CobsEncoderSliceAsync<DEFAULT_BUF_SIZE>,
    decoder: CobsDecoderSliceAsync<DEFAULT_BUF_SIZE>,
    inbuf: [u8; DEFAULT_BUF_SIZE],
    in_len: usize,
    pending_frame: Option<usize>,
    frame_off: usize,
    body_pushed: bool,
    shutdown_flush: Option<(usize, usize)>,
}

impl<S> CobsAsync<S>
where
    S: Unpin,
{
    /// Creates a codec wrapper around `stream`.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            encoder: CobsEncoderAsync::new_to_slice([0u8; DEFAULT_BUF_SIZE]),
            decoder: CobsDecoderAsync::new_to_slice([0u8; DEFAULT_BUF_SIZE]),
            inbuf: [0u8; DEFAULT_BUF_SIZE],
            in_len: 0,
            pending_frame: None,
            frame_off: 0,
            body_pushed: false,
            shutdown_flush: None,
        }
    }
}

impl<S> AsyncRead for CobsAsync<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let this = self.get_mut();
        loop {
            if let Some(len) = this.pending_frame {
                let n = (len - this.frame_off).min(buf.remaining());
                let src = &this.decoder.dest()[this.frame_off..this.frame_off + n];
                buf.initialize_unfilled()[..n].copy_from_slice(src);
                buf.advance(n);
                this.frame_off += n;
                if this.frame_off == len {
                    this.pending_frame = None;
                    this.frame_off = 0;
                }
                if buf.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }
                continue;
            }
            let mut rb = ReadBuf::new(&mut this.inbuf[this.in_len..]);
            match Pin::new(&mut this.stream).poll_read(cx, &mut rb) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => this.in_len += rb.filled().len(),
            }
            if this.in_len == 0 {
                match this.decoder.finish_frame() {
                    Ok(len) => {
                        this.pending_frame = Some(len as usize);
                        continue;
                    }
                    Err(CompletionError::IncompleteFrame(_)) => {
                        return Poll::Ready(Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "truncated COBS frame",
                        )));
                    }
                    Err(CompletionError::NoFrame) => return Poll::Ready(Ok(())),
                    Err(CompletionError::InvalidState) => {
                        return Poll::Ready(Err(Error::new(
                            ErrorKind::InvalidData,
                            "decoder poisoned",
                        )));
                    }
                }
            }
            let progress = {
                let mut fut = pin!(this.decoder.push_slice_async(&this.inbuf[..this.in_len]));
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(p)) => p,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(io_error_from_decode(e))),
                    Poll::Pending => return Poll::Pending,
                }
            };
            let consumed = progress.consumed as usize;
            this.in_len -= consumed;
            this.inbuf.copy_within(consumed.., 0);
            if let Some(len) = progress.frame_len {
                this.pending_frame = Some(len as usize);
            }
        }
    }
}

impl<S> AsyncWrite for CobsAsync<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        let this = self.get_mut();
        if this.shutdown_flush.is_some() {
            return Poll::Ready(Err(Error::new(
                ErrorKind::BrokenPipe,
                "stream already shut down",
            )));
        }
        let mut fut = pin!(this.encoder.push_slice_async(buf));
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(_)) => {
                if !buf.is_empty() {
                    this.body_pushed = true;
                }
                Poll::Ready(Ok(buf.len()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io_error_from_encode(e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        if this.shutdown_flush.is_none() && this.body_pushed {
            let mut fut = pin!(this.encoder.finalize_async());
            let len = match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(len)) => len as usize,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(io_error_from_encode(e))),
                Poll::Pending => return Poll::Pending,
            };
            this.shutdown_flush = Some((len, 0));
        }
        match this.shutdown_flush {
            Some((total, written)) if written < total => {
                match Pin::new(&mut this.stream)
                    .poll_write(cx, &this.encoder.dest()[written..total])
                {
                    Poll::Ready(Ok(n)) => {
                        if n == 0 && written < total {
                            return Poll::Ready(Err(Error::new(
                                ErrorKind::WriteZero,
                                "zero-byte write",
                            )));
                        }
                        this.shutdown_flush = Some((total, written + n));
                        if written + n == total {
                            this.shutdown_flush = None;
                        } else {
                            return Poll::Pending;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            _ => {}
        }
        Pin::new(&mut this.stream).poll_shutdown(cx)
    }
}

fn io_error_from_decode(e: DecodeError<Error, Error>) -> Error {
    match e {
        DecodeError::Source(e) | DecodeError::Destination(e) => e,
        DecodeError::InvalidFrame(_) => Error::new(ErrorKind::InvalidData, "invalid COBS frame"),
        DecodeError::UnexpectedSourceEof => {
            Error::new(ErrorKind::UnexpectedEof, "truncated COBS frame")
        }
        DecodeError::EmptyFrame => Error::new(ErrorKind::InvalidData, "empty COBS frame"),
        DecodeError::Poisoned => Error::new(ErrorKind::InvalidData, "codec poisoned"),
    }
}

/// Converts an encoder source error into the `io::Error` used by this backend.
trait IntoIoError: Sized {
    fn into_io_error(self) -> Error;
}

impl IntoIoError for Error {
    fn into_io_error(self) -> Error {
        self
    }
}

impl IntoIoError for Infallible {
    fn into_io_error(self) -> Error {
        match self {}
    }
}

fn io_error_from_encode<SourceError: IntoIoError>(e: EncodeError<SourceError, Error>) -> Error {
    match e {
        EncodeError::Source(e) => e.into_io_error(),
        EncodeError::Destination(e) => e,
        EncodeError::UnexpectedSourceEof => {
            Error::new(ErrorKind::UnexpectedEof, "truncated COBS frame")
        }
        EncodeError::Poisoned => Error::new(ErrorKind::InvalidData, "codec poisoned"),
        EncodeError::SourceChanged => todo!(),
        EncodeError::WriteZero => todo!(),
        EncodeError::AlreadyFinalized => todo!(),
        EncodeError::PositionOverflow => todo!(),
    }
}
