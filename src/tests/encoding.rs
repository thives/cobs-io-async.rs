#[cfg(feature = "embedded-io")]
mod embedded {
    crate::tests::support::encoder_suite::encoder_tests!(
        crate::embedded,
        crate::tests::support::embedded_writer::TestWriter
    );
}

#[cfg(feature = "tokio")]
mod tokio {
    crate::tests::support::encoder_suite::encoder_tests!(
        crate::tokio,
        crate::tests::support::tokio_writer::TestWriter
    );

    #[test]
    fn operation_futures_are_send_for_send_io() {
        use crate::tests::support::tokio_writer::TestWriter;
        use crate::tokio::CobsEncoderAsync;
        use ::tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
        fn assert_send<T: Send>(_: T) {}
        fn check<S, D>(source: &mut S, encoder: &mut CobsEncoderAsync<D>)
        where
            S: AsyncRead + Unpin + Send + ?Sized,
            D: AsyncWrite + AsyncSeek + Unpin + Send,
        {
            assert_send(encoder.push_async(source));
            assert_send(encoder.push_slice_async(b"A"));
            assert_send(encoder.finalize_async());
            assert_send(encoder.reset_async());
        }
        let mut reader: &[u8] = b"A";
        let mut encoder = CobsEncoderAsync::new(TestWriter::new(16));
        check(&mut reader, &mut encoder);
        let mut writer = TestWriter::new(16);
        assert_send(crate::tokio::encode_from_slice_async(b"A", &mut writer));
        assert_send(crate::tokio::encode_from_slice_including_sentinels_async(
            b"A",
            &mut writer,
        ));
        assert_eq!(reader, b"A");
        assert_eq!(encoder.dest().io_calls(), 0);
        assert_eq!(writer.io_calls(), 0);
    }
}

#[test]
fn encoding_size_bounds() {
    for (input_len, overhead, encoded_len) in [
        (0, 1, 1),
        (1, 1, 2),
        (253, 1, 254),
        (254, 1, 255),
        (255, 2, 257),
        (508, 2, 510),
        (509, 3, 512),
    ] {
        assert_eq!(crate::max_encoding_overhead(input_len), overhead);
        assert_eq!(crate::max_encoding_length(input_len), encoded_len);
    }
}
