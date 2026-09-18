use crate::SeekableError;
use embedded_io_async::{ErrorType, Read, Seek, SeekFrom, Write};

use crate::codec::encode::define_encoder;

define_encoder! {
    D: [Write + Seek],
    S: [Read + ?Sized],
    source_seek: Seek,
    dest_nonseek: [Write],
    errors: [S::Error, D::Error],
    slice_source_error: SeekableError,
    slice_dest_error: SeekableError,
    seek_from: SeekFrom,
}

impl<D> ErrorType for CobsEncoderAsync<D>
where
    D: Write,
{
    type Error = EncodeError<SeekableError, D::Error>;
}

impl<D> Write for CobsEncoderAsync<D>
where
    D: Write + Seek,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, EncodeError<SeekableError, D::Error>> {
        if let Err(error) = self.push_slice_async(buf).await {
            self.reset_async().await?;
            return Err(error);
        }
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), EncodeError<SeekableError, D::Error>> {
        Ok(())
    }
}

// Hack to be able to place embedded-io stream tests into the shared tests module.
#[cfg(test)]
impl<D> CobsEncoderAsync<D> {
    pub(crate) fn can_stream() -> bool {
        true
    }
}

impl ErrorType for CobsEncoderSliceAsync<'_> {
    type Error = EncodeError<SeekableError, SeekableError>;
}

impl Write for CobsEncoderSliceAsync<'_> {
    async fn write(
        &mut self,
        buf: &[u8],
    ) -> Result<usize, EncodeError<SeekableError, SeekableError>> {
        self.0.write(buf).await
    }

    async fn flush(&mut self) -> Result<(), EncodeError<SeekableError, SeekableError>> {
        Ok(())
    }
}
