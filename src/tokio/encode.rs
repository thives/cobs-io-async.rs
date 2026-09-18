use std::io;

use ::tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SeekFrom,
};

use crate::codec::encode::define_encoder;

define_encoder! {
    D: [AsyncWrite + AsyncSeek + Unpin],
    S: [AsyncRead + Unpin + ?Sized],
    source_seek: AsyncSeek,
    dest_nonseek: [AsyncWrite + Unpin],
    errors: [io::Error, io::Error],
    slice_source_error: io::Error,
    slice_dest_error: io::Error,
    seek_from: io::SeekFrom,
}

// Hack to be able to place embedded-io stream tests into the shared tests module.
#[cfg(test)]
impl<D> CobsEncoderAsync<D>
where
    D: AsyncWrite + AsyncSeek + Unpin,
{
    pub(crate) async fn write(
        &mut self,
        _: &[u8],
    ) -> Result<usize, EncodeError<io::Error, io::Error>> {
        Ok(0)
    }

    pub(crate) async fn flush(&mut self) -> Result<(), EncodeError<io::Error, io::Error>> {
        Ok(())
    }

    pub(crate) fn can_stream() -> bool {
        false
    }
}
