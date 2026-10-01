#[cfg(feature = "tokio")]
use core::pin::Pin;
#[cfg(feature = "tokio")]
use core::task::{Context, Poll};
use std::vec::Vec;

/// In-memory reader that delivers at most `chunk` bytes per read or fill,
/// simulating short reads, and counts I/O calls.
///
/// `consume` asserts that callers never consume more than the most recently
/// offered window, i.e. they cannot claim bytes they have not inspected.
#[derive(Debug)]
pub(crate) struct ChunkedReader {
    data: Vec<u8>,
    pos: usize,
    chunk: usize,
    window: usize,
    reads: usize,
    fills: usize,
}

impl ChunkedReader {
    pub(crate) fn new(data: impl Into<Vec<u8>>, chunk: usize) -> Self {
        assert!(chunk > 0);
        Self {
            data: data.into(),
            pos: 0,
            chunk,
            window: 0,
            reads: 0,
            fills: 0,
        }
    }

    /// Bytes not yet read or consumed.
    pub(crate) fn remaining(&self) -> &[u8] {
        &self.data[self.pos..]
    }

    /// Number of `read` calls.
    pub(crate) fn reads(&self) -> usize {
        self.reads
    }

    /// Number of `fill_buf` calls.
    pub(crate) fn fills(&self) -> usize {
        self.fills
    }

    fn read_into(&mut self, buf: &mut [u8]) -> usize {
        self.reads += 1;
        let n = buf.len().min(self.chunk).min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        self.window = 0;
        n
    }

    fn fill(&mut self) -> &[u8] {
        self.fills += 1;
        self.window = self.chunk.min(self.data.len() - self.pos);
        &self.data[self.pos..self.pos + self.window]
    }

    fn consume_bytes(&mut self, amt: usize) {
        assert!(
            amt <= self.window,
            "consumed {amt} bytes from a {}-byte window",
            self.window
        );
        self.window -= amt;
        self.pos += amt;
    }
}

#[cfg(feature = "embedded-io")]
impl embedded_io_async::ErrorType for ChunkedReader {
    type Error = core::convert::Infallible;
}

#[cfg(feature = "embedded-io")]
impl embedded_io_async::Read for ChunkedReader {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        Ok(self.read_into(buf))
    }
}

#[cfg(feature = "embedded-io")]
impl embedded_io_async::BufRead for ChunkedReader {
    async fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        Ok(self.fill())
    }

    fn consume(&mut self, amt: usize) {
        self.consume_bytes(amt);
    }
}

#[cfg(feature = "tokio")]
impl ::tokio::io::AsyncRead for ChunkedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ::tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let n = this.read_into(buf.initialize_unfilled());
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl ::tokio::io::AsyncBufRead for ChunkedReader {
    fn poll_fill_buf(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        Poll::Ready(Ok(self.get_mut().fill()))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.get_mut().consume_bytes(amt);
    }
}
