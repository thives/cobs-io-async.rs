#[cfg(feature = "embedded-io")]
mod embedded {
    crate::tests::support::decoder_suite::decoder_tests!(
        crate::embedded,
        crate::tests::support::embedded_writer::TestWriter
    );

    #[test]
    fn zero_write_panics_and_leaves_decoder_poisoned() {
        use crate::tests::support::{Action, Event};
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let mut writer = TestWriter::new(8);
        writer.set_script([Event {
            after_bytes: 1,
            action: Action::Zero,
        }]);
        let mut decoder = api::CobsDecoderAsync::new(writer);
        let mut source: &[u8] = &[3, b'A', b'B', 0];
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            block_on(decoder.push_async(&mut source))
        }));
        assert!(outcome.is_err());
        assert_eq!(source, &[0]);
        assert_eq!(decoder.dest().bytes(), b"A");
        assert_eq!(decoder.dest().accepted_bytes(), 1);
        assert!(matches!(
            block_on(decoder.push_async(&mut source)),
            Err(DecodeError::Poisoned)
        ));
        assert_eq!(decoder.dest().io_calls(), 2);
        assert_eq!(
            block_on(decoder.discard_frame_async(&mut source)).unwrap(),
            1
        );
        assert!(source.is_empty());
        assert_eq!(decoder.dest().io_calls(), 2);
        assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
    }
}

#[cfg(feature = "tokio")]
mod tokio {
    crate::tests::support::decoder_suite::decoder_tests!(
        crate::tokio,
        crate::tests::support::tokio_writer::TestWriter
    );

    #[test]
    fn zero_write_returns_write_zero_and_poisons() {
        use crate::tests::support::{Action, Event};
        block_on(async {
            let mut writer = TestWriter::new(8);
            writer.set_script([Event {
                after_bytes: 1,
                action: Action::Zero,
            }]);
            let mut decoder = api::CobsDecoderAsync::new(writer);
            let mut source: &[u8] = &[3, b'A', b'B', 0];
            match decoder.push_async(&mut source).await {
                Err(DecodeError::Destination(error)) => {
                    assert_eq!(error.kind(), std::io::ErrorKind::WriteZero);
                }
                other => panic!("expected WriteZero, got {other:?}"),
            }
            assert_eq!(source, &[0]);
            assert_eq!(decoder.dest().bytes(), b"A");
            assert_eq!(decoder.dest().accepted_bytes(), 1);
            assert!(matches!(
                decoder.push_async(&mut source).await,
                Err(DecodeError::Poisoned)
            ));
            assert_eq!(decoder.dest().io_calls(), 2);
            assert_eq!(decoder.discard_frame_async(&mut source).await.unwrap(), 1);
            assert!(source.is_empty());
            assert_eq!(decoder.dest().io_calls(), 2);
            assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
        });
    }
}
