#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use util::*;

    use crate::{OutputSeekable, SeekableError};
    pub mod util {
        use super::*;
        // Simple test type to test async encoding and decoding.
        pub struct TestBuffer<'a, const N: usize> {
            pub buf: &'a mut [u8; N],
            idx: usize,
            len: usize,
        }

        #[derive(Debug, PartialEq, Eq)]
        pub enum TestBufferError {
            OutOfBounds,
        }

        #[cfg(feature = "no_std")]
        impl<const N: usize> ErrorType for TestBuffer<'_, N> {
            type Error = TestBufferError;
        }

        impl Error for TestBufferError {
            fn kind(&self) -> ErrorKind {
                ErrorKind::Other
            }
        }

        impl core::error::Error for TestBufferError {}
        impl core::fmt::Display for TestBufferError {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "TestBufferError")
            }
        }

        impl<const N: usize> Read for TestBuffer<'_, N> {
            async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
                let len = buf.len().min(self.len - self.idx);
                buf[..len].copy_from_slice(&self.buf[self.idx..(self.idx + len)]);
                self.idx += len;
                Ok(len)
            }
        }

        impl<const N: usize> Write for TestBuffer<'_, N> {
            async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
                let len = buf.len();
                if self.idx + len > self.buf.len() {
                    return Err(TestBufferError::OutOfBounds);
                }
                self.buf[self.idx..self.idx + len].copy_from_slice(buf);
                self.idx += len;
                self.len = self.len.max(self.idx);
                Ok(len)
            }

            async fn flush(&mut self) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        impl<const N: usize> Seek for TestBuffer<'_, N> {
            async fn seek(&mut self, pos: SeekFrom) -> Result<u64, Self::Error> {
                match pos {
                    SeekFrom::Start(offset) => {
                        if offset > self.len as u64 {
                            return Err(TestBufferError::OutOfBounds);
                        }
                        self.idx = offset as usize;
                    }
                    SeekFrom::End(offset) => {
                        let new_idx = self.len.saturating_add_signed(offset as isize);
                        if new_idx > self.len {
                            return Err(TestBufferError::OutOfBounds);
                        }
                        self.idx = new_idx;
                    }
                    SeekFrom::Current(offset) => {
                        let new_idx = self.idx.saturating_add_signed(offset as isize);
                        if new_idx > self.len {
                            return Err(TestBufferError::OutOfBounds);
                        }
                        self.idx = new_idx;
                    }
                }
                Ok(self.idx as u64)
            }
        }

        impl<'a, const N: usize> TestBuffer<'a, N> {
            pub(crate) fn from_buf(buf: &'a mut [u8; N]) -> TestBuffer<'a, N> {
                TestBuffer {
                    buf,
                    idx: 0,
                    len: 0,
                }
            }
            pub(crate) fn from_buf_full(buf: &'a mut [u8; N]) -> TestBuffer<'a, N> {
                let len = buf.len();
                TestBuffer { buf, idx: 0, len }
            }
        }
        impl<const N: usize> TestBuffer<'_, N> {
            pub(crate) fn mangle(&mut self) {
                self.buf.iter_mut().for_each(|i| *i = 0x80);
            }
            pub(crate) fn append_sentinel(&mut self) {
                assert!(self.len < N, "TestBuffer has no room for a sentinel");
                self.buf[self.len] = 0;
                if self.idx == self.len {
                    self.idx += 1;
                }
                self.len += 1;
            }
            pub(crate) fn prepend_sentinel(&mut self) {
                assert!(self.len < N, "TestBuffer has no room for a sentinel");
                self.buf.copy_within(0..self.len, 1);
                self.buf[0] = 0;
                self.idx += 1;
                self.len += 1;
            }
        }

        #[test]
        fn test_buffer_constructor_borrows_input_and_resets_state() {
            let mut source = [0x11, 0x22, 0x33];
            let buffer = TestBuffer::from_buf(&mut source);
            assert_eq!(buffer.buf, &[0x11, 0x22, 0x33]);
            assert_eq!(buffer.idx, 0);
            assert_eq!(buffer.len, 0);
            buffer.buf[0] = 0xFF;
            assert_eq!(source, [0xFF, 0x22, 0x33]);
        }

        #[test]
        fn test_buffer_mangle_replaces_backing_bytes() {
            let mut raw = [0x11, 0x22, 0x33];
            let mut buffer = TestBuffer::from_buf(&mut raw);
            buffer.idx = 2;
            buffer.len = 2;

            buffer.mangle();

            assert_eq!(buffer.buf, &[0x80, 0x80, 0x80]);
            assert_eq!(buffer.idx, 2);
            assert_eq!(buffer.len, 2);
        }

        #[test]
        fn test_buffer_append_sentinel_updates_state() {
            let mut raw = [0x80, 0x80, 0x80];
            let mut buffer = TestBuffer::from_buf(&mut raw);
            block_on(buffer.write(&[0x11, 0x22])).unwrap();

            buffer.append_sentinel();

            assert_eq!(buffer.buf, &[0x11, 0x22, 0x00]);
            assert_eq!(buffer.idx, 3);
            assert_eq!(buffer.len, 3);
        }

        #[test]
        fn test_buffer_prepend_sentinel_updates_state() {
            let mut raw = [0x80, 0x80, 0x80];
            let mut buffer = TestBuffer::from_buf(&mut raw);
            block_on(buffer.write(&[0x11, 0x22])).unwrap();

            buffer.prepend_sentinel();

            assert_eq!(buffer.buf, &[0x00, 0x11, 0x22]);
            assert_eq!(buffer.idx, 3);
            assert_eq!(buffer.len, 3);
        }

        #[test]
        fn test_buffer_write_and_flush() {
            let mut raw = [0x80; 4];
            let mut buffer = TestBuffer::from_buf(&mut raw);

            assert_eq!(block_on(buffer.write(&[0x11, 0x22])).unwrap(), 2);
            block_on(buffer.flush()).unwrap();

            assert_eq!(buffer.buf, &[0x11, 0x22, 0x80, 0x80]);
            assert_eq!(buffer.idx, 2);
            assert_eq!(buffer.len, 2);
        }

        #[test]
        fn test_buffer_seek_uses_embedded_io_semantics() {
            let mut raw = [0x80; 8];
            let mut buffer = TestBuffer::from_buf(&mut raw);
            block_on(buffer.write(&[0x11, 0x22, 0x33, 0x44])).unwrap();

            assert_eq!(block_on(buffer.seek(SeekFrom::Start(2))).unwrap(), 2);
            assert_eq!(block_on(buffer.seek(SeekFrom::Current(1))).unwrap(), 3);
            assert_eq!(block_on(buffer.seek(SeekFrom::Current(-10))).unwrap(), 0);
            assert_eq!(block_on(buffer.seek(SeekFrom::End(-1))).unwrap(), 3);
            assert_eq!(block_on(buffer.seek(SeekFrom::End(-10))).unwrap(), 0);
        }

        #[test]
        fn test_buffer_overwrite_does_not_extend_logical_length() {
            let mut raw = [0x80; 4];
            let mut buffer = TestBuffer::from_buf(&mut raw);
            block_on(buffer.write(b"abcd")).unwrap();
            block_on(buffer.seek(SeekFrom::Start(1))).unwrap();
            block_on(buffer.write(b"ZZ")).unwrap();

            assert_eq!(buffer.buf, b"aZZd");
            assert_eq!(buffer.idx, 3);
            assert_eq!(buffer.len, 4);
        }
    }

    #[test]
    fn test_buffer_out_of_bounds_write_returns_error() {
        block_on(async {
            let mut raw = [0x80; 4];
            let mut buffer = OutputSeekable::new(&mut raw);
            matches!(
                buffer.write(b"abcde").await.unwrap_err(),
                SeekableError::OutOfBounds
            );
        });
    }

    async fn continuous_decoding<'a, S, const N: usize>(
        decoder: &mut CobsDecoderAsync<TestBuffer<'a, N>>,
        expected_data: &[u8],
        encoded_frame: &mut S,
    ) where
        S: embedded_io_async::Read + embedded_io_async::Seek,
    {
        assert_eq!(N, 10 * expected_data.len());
        for frame in 0..10 {
            let progress = decoder.push_async(encoded_frame).await.unwrap();
            assert!(progress.consumed > 0);
            assert_eq!(expected_data.len() as u64, progress.written);
            assert_eq!(Some(expected_data.len() as u64), progress.frame_len);
            let start = frame * expected_data.len();
            let end = start + expected_data.len();
            assert_eq!(expected_data, &decoder.dest().buf[start..end]);
        }
    }

    macro_rules! test {
        ($test_suffix:ident, $decoded:expr, $encoded:expr) => {
            paste::item! {
                #[test]
                fn [< encode_streaming_async $test_suffix >] () {
                    block_on(async {
                        let mut src_buf = $decoded;
                        let mut dest_buf = $encoded;
                        let mut src = TestBuffer::from_buf_full(&mut src_buf);
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let mut encoder = CobsEncoderAsync::new(&mut dest);
                            encoder.push_async(&mut src)
                                .await
                                .unwrap();
                        let written = encoder.finalize_async().await.unwrap();
                        assert_eq!($encoded.len() as u64, written);
                        assert_eq!($encoded, &dest.buf[..]);
                    })
                }

                #[test]
                fn [< encode_streaming_from_slice_async $test_suffix >] () {
                    block_on(async {
                        let src_buf = $decoded;
                        let mut dest_buf = $encoded;
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let mut encoder = CobsEncoderAsync::new(&mut dest);
                            encoder.push_slice_async(&src_buf)
                                .await
                                .unwrap();
                        let written = encoder.finalize_async().await.unwrap();
                        assert_eq!($encoded.len() as u64, written);
                        assert_eq!($encoded, &dest.buf[..]);
                    })
                }

                #[test]
                fn [< encode_from_slice_async $test_suffix >] () {
                    block_on(async {
                        let src = $decoded;
                        let mut dest_buf = $encoded;
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let written =
                            encode_from_slice_async(&src, &mut dest)
                                .await
                                .unwrap();
                        assert_eq!($encoded.len() as u64, written);
                        assert_eq!($encoded, &dest.buf[..]);
                    })
                }

                #[test]
                fn [< encode_from_slice_including_sentinels_async $test_suffix >] () {
                    block_on(async {
                        let src = $decoded;
                        let mut dest_buf: [u8; $encoded.len() + 2] = [0x80; $encoded.len() + 2];
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let written =
                            encode_from_slice_including_sentinels_async(&src, &mut dest)
                                .await
                                .unwrap();
                        assert_eq!(dest.buf.len() as u64, written);
                        assert_eq!(0, dest.buf[0]);
                        assert_eq!(0, dest.buf.as_slice()[dest.buf.len() - 1]);
                        assert_eq!($encoded, &dest.buf[1..$encoded.len() + 1]);
                    })
                }

                #[test]
                fn [< encode_owned_streaming_async $test_suffix >] () {
                    block_on(async {
                        let mut src_buf = $decoded;
                        let mut dest_buf = $encoded;
                        let mut src = TestBuffer::from_buf_full(&mut src_buf);
                        dest_buf.iter_mut().for_each(|i| *i = 0x80);
                        let mut encoder = CobsEncoderAsync::new(TestBuffer::from_buf(&mut dest_buf));
                            encoder.push_async(&mut src)
                                .await
                                .unwrap();
                        let written = encoder.finalize_async().await.unwrap();
                        assert_eq!($encoded.len() as u64, written);
                        assert_eq!($encoded, &encoder.dest().buf[..]);
                    })
                }

                #[test]
                fn [< encode_owned_streaming_from_slice_async $test_suffix >] () {
                    block_on(async {
                        let src_buf = $decoded;
                        let mut dest_buf = $encoded;
                        dest_buf.iter_mut().for_each(|i| *i = 0x80);
                        let mut encoder = CobsEncoderAsync::new(TestBuffer::from_buf(&mut dest_buf));
                            encoder.push_slice_async(&src_buf)
                                .await
                                .unwrap();
                        let written = encoder.finalize_async().await.unwrap();
                        assert_eq!($encoded.len() as u64, written);
                        assert_eq!($encoded, &encoder.dest().buf[..]);
                    })
                }

                #[test]
                fn [< decode_streaming_async $test_suffix >] () {
                    block_on(async {
                        let mut src_buf = $encoded;
                        let mut dest_buf = $decoded;
                        let mut src = TestBuffer::from_buf_full(&mut src_buf);
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let mut decoder = CobsDecoderAsync::new(&mut dest);
                        let progress =
                            decoder.push_async(&mut src)
                                .await
                                .unwrap();
                        assert_eq!($encoded.len() as u64, progress.consumed);
                        assert_eq!($decoded.len() as u64, progress.written);
                        assert_eq!(None, progress.frame_len);
                        assert_eq!($decoded.len() as u64, decoder.finish_frame().unwrap());
                        assert_eq!($decoded, &dest.buf[..]);
                    })
                }

                #[test]
                fn [< decode_streaming_slice_async $test_suffix >] () {
                    block_on(async {
                        let src_buf = $encoded;
                        let mut dest_buf = $decoded;
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let mut decoder = CobsDecoderAsync::new(&mut dest);
                        let progress =
                            decoder.push_slice_async(&src_buf)
                                .await
                                .unwrap();
                        assert_eq!($encoded.len() as u64, progress.consumed);
                        assert_eq!($decoded.len() as u64, progress.written);
                        assert_eq!(None, progress.frame_len);
                        assert_eq!($decoded.len() as u64, decoder.finish_frame().unwrap());
                        assert_eq!($decoded, &dest.buf[..]);
                    })
                }

                #[test]
                fn [< decode_to_slice_async $test_suffix >] () {
                    block_on(async {
                        let mut src_buf = $encoded;
                        let mut dest = $decoded;
                        let mut src = TestBuffer::from_buf_full(&mut src_buf);
                        let written =
                            decode_to_slice_async(&mut src, &mut dest)
                                .await
                                .unwrap();
                        assert_eq!($decoded.len() as u64, written);
                        assert_eq!($decoded, dest);
                    })
                }

                #[test]
                fn [< decode_owned_streaming_async $test_suffix >] () {
                    block_on(async {
                        let mut src_buf = $encoded;
                        let mut dest_buf = $decoded;
                        let mut src = TestBuffer::from_buf_full(&mut src_buf);
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let mut decoder = CobsDecoderAsync::new(dest);
                        let progress =
                            decoder.push_async(&mut src)
                                .await
                                .unwrap();
                        assert_eq!($encoded.len() as u64, progress.consumed);
                        assert_eq!($decoded.len() as u64, progress.written);
                        assert_eq!(None, progress.frame_len);
                        assert_eq!($decoded.len() as u64, decoder.finish_frame().unwrap());
                        assert_eq!($decoded, &decoder.dest().buf[..]);
                    })
                }

                #[test]
                fn [< decode_owned_streaming_slice_async $test_suffix >] () {
                    block_on(async {
                        let src_buf = $encoded;
                        let mut dest_buf = $decoded;
                        let mut dest = TestBuffer::from_buf(&mut dest_buf);
                        dest.mangle();
                        let mut decoder = CobsDecoderAsync::new(dest);
                        let progress =
                            decoder.push_slice_async(&src_buf)
                                .await
                                .unwrap();
                        assert_eq!($encoded.len() as u64, progress.consumed);
                        assert_eq!($decoded.len() as u64, progress.written);
                        assert_eq!(None, progress.frame_len);
                        assert_eq!($decoded.len() as u64, decoder.finish_frame().unwrap());
                        assert_eq!($decoded, &decoder.dest().buf[..]);
                    })
                }

                #[test]
                fn [< decode_async_continuously $test_suffix >]() {
                    block_on(async {
                        let mut src_buf = [0x80; 10 * ($encoded.len() + 1)];
                        let mut src = TestBuffer::from_buf(&mut src_buf);
                        for _ in 0..10 {
                            src.write_all(&$encoded).await.unwrap();
                            src.append_sentinel();
                        }
                        src.rewind().await.unwrap();
                        let mut dest_buf = [0x80; 10 * $decoded.len()];
                        let dest = TestBuffer::from_buf(&mut dest_buf);
                        let mut decoder = CobsDecoderAsync::new(dest);
                        continuous_decoding(&mut decoder, &$decoded, &mut src).await;
                    })
                }

                #[test]
                fn [< decode_async_continuously2 $test_suffix >]() {
                    block_on(async {
                        let mut src_buf = [0x80; 10 * ($encoded.len() + 1) + 2];
                        let mut src = TestBuffer::from_buf(&mut src_buf);
                        for _ in 0..10 {
                            src.write_all(&$encoded).await.unwrap();
                            src.append_sentinel();
                        }
                        src.prepend_sentinel();
                        src.prepend_sentinel();
                        src.rewind().await.unwrap();
                        let mut dest_buf = [0x80; 10 * $decoded.len()];
                        let dest = TestBuffer::from_buf(&mut dest_buf);
                        let mut decoder = CobsDecoderAsync::new(dest);
                        continuous_decoding(&mut decoder, &$decoded, &mut src).await;
                    })
                }
            }
        };
    }

    test!(_0, [] as [u8; 0], [1]);
    test!(_1, [10, 11, 0, 12], [3, 10, 11, 2, 12]);
    test!(_2, [10, 11, 1, 12], [5, 10, 11, 1, 12]);
    test!(_3, [0, 0, 1, 0], [1, 1, 2, 1, 1]);
    test!(_4, [255, 0], [2, 255, 1]);
    test!(_5, [1], [2, 1]);
    test!(
        _6,
        [
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5,
        ],
        [
            0xFF, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5, 0xA5, 0xA5,
        ]
    );
    test!(
        _double,
        [10, 11, 0, 12, 0, 0, 3],
        [3, 10, 11, 2, 12, 1, 2, 3]
    );

    #[test]
    fn test_overhead_empty() {
        assert_eq!(max_encoding_overhead(0), 1);
    }

    #[test]
    fn test_overhead_one() {
        assert_eq!(max_encoding_overhead(1), 1);
    }

    #[test]
    fn test_overhead_larger() {
        assert_eq!(max_encoding_overhead(253), 1);
        assert_eq!(max_encoding_overhead(254), 1);
    }

    #[test]
    fn test_overhead_two() {
        assert_eq!(max_encoding_overhead(255), 2);
    }

    #[test]
    fn decoding_broken_packet() {
        block_on(async {
            let mut dest: [u8; 32] = [0; 32];
            let src = b"hello world";
            let mut buf = [0u8; 32];
            let mut encoded_data = TestBuffer::from_buf(&mut buf);
            encode_from_slice_async(src, &mut encoded_data)
                .await
                .unwrap();
            encoded_data.seek(SeekFrom::Start(6)).await.unwrap();
            let encoded_len = encode_from_slice_async(src, &mut encoded_data)
                .await
                .unwrap();
            // Sentinel byte at start and end.
            encoded_data.append_sentinel();
            // Another frame abruptly starts. This simulates a broken frame, and the streaming decoder
            // should be able to recover from this.
            encoded_data.buf[5] = 0;
            encoded_data.buf[5 + encoded_len as usize + 1] = 0;
            let mut decode_dest = TestBuffer::from_buf_full(&mut dest);
            decode_dest.mangle();
            encoded_data.rewind().await.unwrap();
            let mut decoder = CobsDecoderAsync::new(&mut decode_dest);
            match decoder.push_async(&mut encoded_data).await {
                Ok(progress) => panic!(
                    "decoding call did not yield expected invalid frame, ok({}) instead",
                    progress.written
                ),
                Err(DecodeError::InvalidFrame(progress)) => {
                    assert_eq!(progress.written, 4);
                }
                Err(e) => panic!(
                    "decoding call did not yield expected invalid frame, {} instad",
                    e
                ),
            }

            decoder.dest_mut().rewind().await.unwrap();
            if let Ok(progress) = decoder.push_async(&mut encoded_data).await {
                assert_eq!(Some(src.len() as u64), progress.frame_len);
                assert_eq!(
                    src,
                    &decoder.dest().buf[0..(progress.frame_len.unwrap() as usize)]
                );
            } else {
                panic!("decoding call did not yield expected frame");
            }
        })
    }

    const fn get_buf<const N: usize>(len: usize, offset: usize) -> [u8; N] {
        let mut buf = [0u8; N];
        let mut i = 0;
        while i < len {
            let x = i + offset;
            buf[i] = (x & 0xFF) as u8;
            i += 1;
        }
        buf
    }

    #[test]
    fn stream_roundtrip() {
        block_on(async {
            for ct in 1..=1000usize {
                let source = get_buf::<1000>(ct, 0);
                let src = &source[..ct];

                let mut encoded_buf = [0x80; max_encoding_length(1000)];
                let mut encoded = TestBuffer::from_buf(&mut encoded_buf);

                let encoded_size = {
                    let mut encoder = CobsEncoderAsync::new(&mut encoded);
                    for chunk in src.chunks(17) {
                        encoder.push_slice_async(chunk).await.unwrap();
                    }
                    encoder.finalize_async().await.unwrap()
                };

                assert!(encoded_size > 0);
                assert!(encoded_size <= max_encoding_length(ct) as u64);

                let mut decoded_buf = [0x80; 1000];
                let decoded = TestBuffer::from_buf(&mut decoded_buf);
                let mut decoder = CobsDecoderAsync::new(decoded);
                let mut total_written = 0;

                for chunk in encoded.buf[..(encoded_size as usize)].chunks(11) {
                    assert_eq!(
                        DecodeProgress::default(),
                        decoder.push_slice_async(&[]).await.unwrap()
                    );
                    let progress = decoder.push_slice_async(chunk).await.unwrap();
                    assert_eq!(chunk.len() as u64, progress.consumed);
                    assert_eq!(None, progress.frame_len);
                    total_written += progress.written;
                }
                assert_eq!(total_written, ct as u64);
                assert_eq!(Ok(ct as u64), decoder.finish_frame());
                assert_eq!(src, &decoder.dest().buf[..ct]);
                assert!(decoder.dest().buf[ct..].iter().all(|&b| b == 0x80));
            }
        })
    }

    #[test]
    fn test_max_encoding_length() {
        assert_eq!(max_encoding_length(0), 1);
        assert_eq!(max_encoding_length(253), 254);
        assert_eq!(max_encoding_length(254), 255);
        assert_eq!(max_encoding_length(255), 257);
        assert_eq!(max_encoding_length(254 * 2), 255 * 2);
        assert_eq!(max_encoding_length(254 * 2 + 1), 256 * 2);
    }

    #[test]
    fn issue_15() {
        // Reported: https://github.com/awelkie/cobs.rs/issues/15
        block_on(async {
            let my_string = b"\x00\x11\x00\x22";
            let max_len = max_encoding_length(my_string.len());
            assert!(max_len < 128);
            let mut raw_buf = [0u8; 128];
            let mut buf = TestBuffer::from_buf(&mut raw_buf);
            encode_from_slice_async(my_string, &mut buf).await.unwrap();
            let mut decoded_dest_buf = [0u8; 128];
            buf.rewind().await.unwrap();
            let new_len = decode_to_slice_async(&mut buf, &mut decoded_dest_buf)
                .await
                .unwrap();
            let decoded_buf = &decoded_dest_buf[0..(new_len as usize)];
            assert_eq!(my_string, decoded_buf);
        })
    }

    #[test]
    fn issue_19_test_254_block_all_ones() {
        block_on(async {
            let src_buf: [u8; 254] = [1; 254];
            let mut buf: [u8; 256] = [0; 256];
            let mut dest = TestBuffer::from_buf(&mut buf);
            let encode_len = encode_from_slice_async(&src_buf, &mut dest).await.unwrap();
            assert_eq!(encode_len, 255);
            let mut decoded: [u8; 254] = [0; 254];
            dest.rewind().await.unwrap();
            let result = decode_to_slice_async(&mut dest, &mut decoded)
                .await
                .expect("decoding failed");
            assert_eq!(result, 254);
            assert_eq!(&src_buf, &decoded);
        })
    }
}
