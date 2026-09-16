use crate::{EncodeError, InputSeekable, SeekableError};
use core::convert::Infallible;
use embedded_io_async::{Read, Seek, SeekFrom, Write};

#[cfg(test)]
mod tests {
    use crate::{CobsEncoderAsync, EncodeError, OutputSeekable};
    use embedded_io_async::Seek;
    use futures::executor::block_on;

    #[test]
    fn dest_returns_correctly() {
        block_on(async {
            let mut dest_buf: [u8; 32] = [0; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let expected = core::ptr::from_ref(&output_buffer);
            let encoder = CobsEncoderAsync::new(&mut output_buffer);
            assert_eq!(expected, core::ptr::from_ref(&**encoder.dest()));
        });
    }

    #[test]
    fn dest_mut_returns_correctly() {
        block_on(async {
            let mut dest_buf: [u8; 32] = [0; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let expected = core::ptr::from_mut(&mut output_buffer);
            let mut encoder = CobsEncoderAsync::new(&mut output_buffer);
            assert_eq!(expected, core::ptr::from_mut(&mut **encoder.dest_mut()));
        });
    }

    #[test]
    fn into_inner_returns_correctly() {
        block_on(async {
            let mut dest_buf: [u8; 32] = [0; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let expected = core::ptr::from_mut(&mut output_buffer);
            let encoder = CobsEncoderAsync::new(&mut output_buffer);
            assert_eq!(expected, core::ptr::from_mut(&mut *encoder.into_inner()));
        });
    }

    #[test]
    fn immediate_finalization_after_initialization_returns_empty_encoding() {
        block_on(async {
            let mut dest_buf: [u8; 32] = [0; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let mut encoder = CobsEncoderAsync::new(&mut output_buffer);
            let res = encoder.finalize_async().await;
            assert!(res.is_ok());
            assert_eq!(1, res.unwrap());
            assert_eq!(1, dest_buf[0]);
        });
    }

    #[test]
    fn repeated_finalization_returns_same_result() {
        block_on(async {
            let mut dest_buf: [u8; 32] = [0; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let mut encoder = CobsEncoderAsync::new(&mut output_buffer);
            let res1 = encoder.finalize_async().await;
            let res2 = encoder.finalize_async().await;
            assert_eq!(res1.unwrap(), res2.unwrap());
        });
    }

    #[test]
    fn push_after_finalization_returns_error() {
        block_on(async {
            let mut dest_buf: [u8; 32] = [0; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let mut encoder = CobsEncoderAsync::new(&mut output_buffer);
            let _ = encoder.finalize_async().await;
            let mut input_data: &[u8] = &[1, 2, 3];
            let res = encoder.push_async(&mut input_data).await;
            assert_eq!(res.unwrap_err(), EncodeError::AlreadyFinalized);
        });
    }

    #[test]
    fn finalization_after_push_fail_returns_poisoned_error() {
        block_on(async {
            // Deliberately too short destination buffer.
            let mut dest_buf = [0u8; 3];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let mut encoder = CobsEncoderAsync::new(&mut output_buffer);
            let mut input_data: &[u8] = &[1, 2, 3];
            let _ = encoder.push_async(&mut input_data).await;
            let res = encoder.finalize_async().await;
            assert_eq!(res.unwrap_err(), EncodeError::Poisoned);
        });
    }

    #[test]
    fn reset_allows_continued_encoding() {
        block_on(async {
            // Deliberately too short destination buffer.
            let mut dest_buf = [0u8; 3];
            let mut dest_buf2 = [0u8; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let mut output_buffer2 = OutputSeekable::new(&mut dest_buf2);
            let mut encoder = CobsEncoderAsync::new(&mut output_buffer);
            let mut input_data1: &[u8] = &[1, 2, 3];
            let mut input_data2: &[u8] = &[1, 2];
            let _ = encoder.push_async(&mut input_data1).await;
            let res1 = encoder.finalize_async().await;
            assert_eq!(res1.unwrap_err(), EncodeError::Poisoned);
            encoder.dest = &mut output_buffer2;
            assert!(encoder.reset_async().await.is_ok());
            let _ = encoder.push_async(&mut input_data2).await;
            let res2 = encoder.finalize_async().await;
            assert_eq!(res2, Ok(3));
            assert_eq!(
                [3, 1, 2],
                encoder.dest().buf[4..(res2.unwrap() as usize + 4)]
            );
        });
    }
    #[test]
    fn reset_and_initialize_preserves_state() {
        block_on(async {
            let input_data1 = b"A";
            let input_data2 = b"B";
            let expected_data1 = [2, b'A'];
            let expected_data2 = [2, b'A', 0, 2, b'B'];
            let mut dest_buf = [0u8; 32];
            let mut output_buffer = OutputSeekable::new(&mut dest_buf);
            let mut encoder = CobsEncoderAsync::new(&mut output_buffer);
            encoder.push_slice_async(input_data1).await.unwrap();
            let n1 = encoder.finalize_async().await.unwrap();
            assert_eq!(2, n1);
            assert_eq!(&expected_data1, &encoder.dest().buf[..2]);
            encoder.reset_async().await.unwrap();
            encoder.dest_mut().rewind().await.unwrap();
            encoder.push_slice_async(input_data2).await.unwrap();
            let n2 = encoder.finalize_async().await.unwrap();
            assert_eq!(2, n2);
            assert_eq!(&expected_data2, &encoder.dest().buf[..5]);
        });
    }
}
