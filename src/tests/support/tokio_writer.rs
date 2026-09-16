use std::{
    io::{self, SeekFrom},
    pin::Pin,
    task::{Context, Poll},
};

use ::tokio::io::{AsyncSeek, AsyncWrite};

pub(crate) use super::writer::TestWriter;

impl AsyncWrite for TestWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .poll_write_bytes(cx, bytes)
            .map(|result| result.map_err(io::Error::other))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().record_io();
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().record_io();
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for TestWriter {
    fn start_seek(self: Pin<&mut Self>, from: SeekFrom) -> io::Result<()> {
        self.get_mut()
            .seek_to(from)
            .map(|_| ())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let writer = self.get_mut();
        writer.record_io();
        Poll::Ready(Ok(writer.position()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_seek_maps_errors_and_preserves_cursor() {
        let mut writer = TestWriter::new(8);
        writer.set_position(2);
        let error =
            AsyncSeek::start_seek(Pin::new(&mut writer), SeekFrom::Current(i64::MIN)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(writer.position(), 2);
        assert!(writer.bytes().is_empty());
        assert_eq!(writer.io_calls(), 1);
        let position = futures::executor::block_on(core::future::poll_fn(|cx| {
            AsyncSeek::poll_complete(Pin::new(&mut writer), cx)
        }))
        .unwrap();
        assert_eq!(position, 2);
        assert_eq!(writer.io_calls(), 2);
    }
}
