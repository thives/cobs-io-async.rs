// The backend-specific state machines, macros, and slice adapters in these
// modules are unused when only the synchronous API is compiled.
#[cfg_attr(
    not(any(feature = "embedded-io", feature = "tokio")),
    allow(dead_code, unused_imports, unused_macros)
)]
pub(crate) mod decode;
#[cfg_attr(
    not(any(feature = "embedded-io", feature = "tokio")),
    allow(dead_code, unused_imports, unused_macros)
)]
pub(crate) mod encode;

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
use crate::error::SeekableError;

/// Default internal buffer size in bytes used by the backend codec wrappers.
#[cfg(any(feature = "embedded-io", feature = "tokio"))]
pub const DEFAULT_BUF_SIZE: usize = 256;

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
#[derive(Debug)]
pub struct OutputSeekable<const N: usize> {
    pub(crate) buf: [u8; N],
    idx: usize,
}

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
impl<const N: usize> OutputSeekable<N> {
    pub(crate) fn new(buf: [u8; N]) -> Self {
        Self { buf, idx: 0 }
    }
}

#[cfg(feature = "embedded-io")]
impl<const N: usize> embedded_io_async::ErrorType for OutputSeekable<N> {
    type Error = SeekableError;
}

#[cfg(feature = "embedded-io")]
impl<const N: usize> embedded_io_async::Write for OutputSeekable<N> {
    async fn write(&mut self, data: &[u8]) -> Result<usize, SeekableError> {
        if data.len() > self.buf.len() - self.idx {
            return Err(SeekableError::OutOfBounds);
        }
        let len = data.len().min(self.buf.len() - self.idx);
        self.buf[self.idx..(self.idx + len)].copy_from_slice(&data[..len]);
        self.idx += len;
        Ok(len)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(feature = "tokio")]
impl<const N: usize> ::tokio::io::AsyncWrite for OutputSeekable<N> {
    fn poll_write(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
        data: &[u8],
    ) -> core::task::Poll<std::io::Result<usize>> {
        if data.len() > self.buf.len() - self.idx {
            return core::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                SeekableError::OutOfBounds,
            )));
        }
        let this = self.get_mut();
        let len = data.len();
        this.buf[this.idx..(this.idx + len)].copy_from_slice(&data[..len]);
        this.idx += len;
        core::task::Poll::Ready(Ok(len))
    }

    fn poll_flush(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        core::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        core::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "embedded-io")]
impl<const N: usize> embedded_io_async::Seek for OutputSeekable<N> {
    async fn seek(&mut self, pos: embedded_io_async::SeekFrom) -> Result<u64, SeekableError> {
        match pos {
            embedded_io_async::SeekFrom::Start(offset) => {
                if offset > self.buf.len() as u64 {
                    return Err(SeekableError::OutOfBounds);
                }
                self.idx = offset as usize;
            }
            embedded_io_async::SeekFrom::End(offset) => {
                let new_idx = self.buf.len() as i64 + offset;
                if new_idx > self.buf.len() as i64 {
                    return Err(SeekableError::OutOfBounds);
                }
                self.idx = new_idx as usize;
            }
            embedded_io_async::SeekFrom::Current(offset) => {
                let new_idx = self.idx as i64 + offset;
                if new_idx > self.buf.len() as i64 {
                    return Err(SeekableError::OutOfBounds);
                }
                self.idx = new_idx as usize;
            }
        }
        Ok(self.idx as u64)
    }
}

#[cfg(feature = "tokio")]
impl<const N: usize> ::tokio::io::AsyncSeek for OutputSeekable<N> {
    fn start_seek(self: core::pin::Pin<&mut Self>, pos: std::io::SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        match pos {
            std::io::SeekFrom::Start(offset) => {
                if offset > this.buf.len() as u64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        SeekableError::OutOfBounds,
                    ));
                }
                this.idx = offset as usize;
            }
            std::io::SeekFrom::End(offset) => {
                let new_idx = this.buf.len() as i64 + offset;
                if new_idx > this.buf.len() as i64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        SeekableError::OutOfBounds,
                    ));
                }
                this.idx = new_idx as usize;
            }
            std::io::SeekFrom::Current(offset) => {
                let new_idx = this.idx as i64 + offset;
                if new_idx > this.buf.len() as i64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        SeekableError::OutOfBounds,
                    ));
                }
                this.idx = new_idx as usize;
            }
        }
        Ok(())
    }
    fn poll_complete(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<u64>> {
        core::task::Poll::Ready(Ok(self.idx as u64))
    }
}
