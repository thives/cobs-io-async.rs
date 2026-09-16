use embedded_io_async::{Error, ErrorKind, ErrorType, Seek, SeekFrom, Write};

use super::writer::TestError;
pub(crate) use super::writer::TestWriter;

impl Error for TestError {
    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}

impl ErrorType for TestWriter {
    type Error = TestError;
}

impl Write for TestWriter {
    async fn write(&mut self, bytes: &[u8]) -> Result<usize, Self::Error> {
        core::future::poll_fn(|cx| self.poll_write_bytes(cx, bytes)).await
    }
    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.record_io();
        Ok(())
    }
}

impl Seek for TestWriter {
    async fn seek(&mut self, from: SeekFrom) -> Result<u64, Self::Error> {
        let from = match from {
            SeekFrom::Start(position) => std::io::SeekFrom::Start(position),
            SeekFrom::End(offset) => std::io::SeekFrom::End(offset),
            SeekFrom::Current(offset) => std::io::SeekFrom::Current(offset),
        };
        self.seek_to(from)
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
            futures::executor::block_on(Seek::seek(&mut writer, SeekFrom::Current(i64::MIN)))
                .unwrap_err();
        assert_eq!(error, TestError::OutOfBounds);
        assert_eq!(Error::kind(&error), ErrorKind::Other);
        assert_eq!(writer.position(), 2);
        assert!(writer.bytes().is_empty());
        assert_eq!(writer.io_calls(), 1);
    }
}
