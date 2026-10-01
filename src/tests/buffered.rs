//! Tests for the buffered decoders, checked against their byte-at-a-time
//! counterparts and the synchronous slice API.

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
macro_rules! buffered_decoder_tests {
    ($backend:path, $writer:path) => {
        use $backend as api;
        use $writer as TestWriter;

        use futures::executor::block_on;
        use std::{vec, vec::Vec};
        use $crate::tests::support::chunked_reader::ChunkedReader;
        use $crate::tests::support::{Action, Event};
        use $crate::{DecodeError, DecodeProgress, max_encoding_length, sync};

        type Outcome = Result<u64, DecodeError<(), ()>>;

        fn simplify<T, S, D>(
            result: Result<T, DecodeError<S, D>>,
        ) -> Result<T, DecodeError<(), ()>> {
            result.map_err(|error| match error {
                DecodeError::Source(_) => DecodeError::Source(()),
                DecodeError::Destination(_) => DecodeError::Destination(()),
                DecodeError::UnexpectedSourceEof => DecodeError::UnexpectedSourceEof,
                DecodeError::EmptyFrame => DecodeError::EmptyFrame,
                DecodeError::InvalidFrame(progress) => DecodeError::InvalidFrame(progress),
                DecodeError::Poisoned => DecodeError::Poisoned,
            })
        }

        /// Decodes one frame with a buffered reader delivering `chunk` bytes
        /// per fill. Returns the outcome, output, unread input, and fill count.
        fn buffered(
            input: &[u8],
            chunk: usize,
            capacity: usize,
        ) -> (Outcome, Vec<u8>, Vec<u8>, usize) {
            let mut reader = ChunkedReader::new(input, chunk);
            let mut dest = vec![0x55; capacity];
            let outcome = simplify(block_on(api::decode_to_slice_buffered_async(
                &mut reader,
                &mut dest,
            )));
            (outcome, dest, reader.remaining().to_vec(), reader.fills())
        }

        /// Same as [`buffered`], using byte-at-a-time reads.
        fn unbuffered(input: &[u8], capacity: usize) -> (Outcome, Vec<u8>, Vec<u8>, usize) {
            let mut reader = ChunkedReader::new(input, usize::MAX);
            let mut dest = vec![0x55; capacity];
            let outcome = simplify(block_on(api::decode_to_slice_async(&mut reader, &mut dest)));
            (outcome, dest, reader.remaining().to_vec(), reader.reads())
        }

        /// Asserts that buffered decoding matches unbuffered decoding for every
        /// chunk size, including output and the unread remainder.
        fn assert_matches_unbuffered(input: &[u8], capacity: usize) -> (Outcome, Vec<u8>) {
            let (expected, expected_dest, expected_rest, _) = unbuffered(input, capacity);
            for chunk in 1..=input.len().max(1) + 1 {
                let (outcome, dest, rest, _) = buffered(input, chunk, capacity);
                assert_eq!(outcome, expected, "input {input:?}, chunk {chunk}");
                assert_eq!(dest, expected_dest, "input {input:?}, chunk {chunk}");
                assert_eq!(rest, expected_rest, "input {input:?}, chunk {chunk}");
            }
            (expected, expected_rest)
        }

        fn framed(payload: &[u8]) -> Vec<u8> {
            let mut buf = vec![0; max_encoding_length(payload.len()) + 2];
            let len = sync::encode_from_slice_including_sentinels(payload, &mut buf).unwrap();
            buf.truncate(len);
            buf
        }

        #[test]
        fn delimiters_at_every_buffer_boundary_preserve_next_frame() {
            let first: Vec<u8> = (1..=40).chain([0, 0]).chain(1..=10).collect();
            let mut input = framed(&first);
            let next = framed(&[9, 0, 8]);
            input.extend_from_slice(&next);

            let (outcome, rest) = assert_matches_unbuffered(&input, first.len());
            assert_eq!(outcome, Ok(first.len() as u64));
            // The leading delimiter of the next frame is still unread.
            assert_eq!(rest, next);

            for chunk in 1..=input.len() {
                let mut reader = ChunkedReader::new(input.clone(), chunk);
                let mut dest = vec![0; first.len()];
                block_on(api::decode_to_slice_buffered_async(&mut reader, &mut dest)).unwrap();
                assert_eq!(dest, first);
                let mut dest = [0; 3];
                assert_eq!(
                    block_on(api::decode_to_slice_buffered_async(&mut reader, &mut dest)).unwrap(),
                    3
                );
                assert_eq!(dest, [9, 0, 8]);
                assert!(reader.remaining().is_empty());
            }
        }

        #[test]
        fn leading_padding_is_skipped() {
            let (outcome, rest) = assert_matches_unbuffered(&[0, 0, 0, 2, 9, 0, 2, 8, 0], 4);
            assert_eq!(outcome, Ok(1));
            assert_eq!(rest, [2, 8, 0]);
        }

        #[test]
        fn empty_frames() {
            assert_eq!(
                assert_matches_unbuffered(&[1, 0, 2, 9, 0], 0),
                (Ok(0), vec![2, 9, 0])
            );
            assert_eq!(assert_matches_unbuffered(&[0, 1], 0), (Ok(0), vec![]));
            assert_eq!(
                assert_matches_unbuffered(&[0, 0, 0], 4),
                (Err(DecodeError::EmptyFrame), vec![])
            );
            assert_eq!(
                assert_matches_unbuffered(&[], 4),
                (Err(DecodeError::EmptyFrame), vec![])
            );
        }

        #[test]
        fn malformed_frame_preserves_next_frame() {
            assert_eq!(
                assert_matches_unbuffered(&[0, 3, 1, 0, 2, 9, 0], 4),
                (
                    Err(DecodeError::InvalidFrame(DecodeProgress {
                        consumed: 4,
                        written: 1,
                        frame_len: None,
                    })),
                    vec![2, 9, 0]
                )
            );
        }

        #[test]
        fn eof_handling() {
            assert_eq!(assert_matches_unbuffered(&[2, 9], 4), (Ok(1), vec![]));
            assert_eq!(
                assert_matches_unbuffered(&[3, 9], 4),
                (Err(DecodeError::UnexpectedSourceEof), vec![])
            );
        }

        #[test]
        fn insufficient_capacity_consumes_through_rejected_byte() {
            assert_eq!(
                assert_matches_unbuffered(&[3, 1, 2, 0, 2, 9, 0], 1),
                (Err(DecodeError::Destination(())), vec![0, 2, 9, 0])
            );
        }

        #[test]
        fn matches_sync_decoder_for_streams() {
            let payloads: [&[u8]; 5] = [&[], &[0], &[1; 254], &[2; 300], &[7, 0, 0, 8]];
            let stream: Vec<u8> = payloads
                .iter()
                .flat_map(|payload| framed(payload))
                .collect();
            for chunk in [1, 2, 3, 7, 64, 255, 256, 1024] {
                let mut reader = ChunkedReader::new(stream.clone(), chunk);
                let mut rest = stream.as_slice();
                for payload in payloads {
                    let mut expected = vec![0; payload.len()];
                    let frame = sync::decode_to_slice(rest, &mut expected).unwrap();
                    rest = &rest[frame.consumed..];

                    let mut dest = vec![0; payload.len()];
                    let len = block_on(api::decode_to_slice_buffered_async(&mut reader, &mut dest))
                        .unwrap();
                    assert_eq!(len as usize, frame.len);
                    assert_eq!(dest, expected);
                    assert_eq!(dest, payload);
                    assert_eq!(reader.remaining(), rest, "chunk {chunk}");
                }
            }
        }

        #[test]
        fn batching_reduces_read_calls() {
            let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
            let mut input = framed(&payload);
            let first_len = input.len();
            input.extend_from_slice(&framed(&[5]));

            let (outcome, dest, rest, reads) = unbuffered(&input, payload.len());
            assert_eq!(outcome, Ok(payload.len() as u64));
            assert_eq!(dest, payload);
            assert_eq!(reads, first_len);

            let chunk = 64;
            let (outcome, dest, buffered_rest, fills) = buffered(&input, chunk, payload.len());
            assert_eq!(outcome, Ok(payload.len() as u64));
            assert_eq!(dest, payload);
            assert_eq!(buffered_rest, rest);
            assert_eq!(fills, first_len.div_ceil(chunk));
            assert!(fills * 10 < reads);
        }

        type PushOutcome = Result<DecodeProgress, DecodeError<(), ()>>;

        /// Pushes `stream` through a stateful decoder until EOF, using
        /// `push_buffered_async` with `chunk`-byte windows, or `push_async`
        /// when `chunk` is `None`. Returns every push outcome, the decoded
        /// output, and the final structural state.
        fn run_pushes(
            stream: &[u8],
            chunk: Option<usize>,
        ) -> (Vec<PushOutcome>, Vec<u8>, Result<(), $crate::CompletionError>) {
            let mut reader = ChunkedReader::new(stream, chunk.unwrap_or(usize::MAX));
            let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(4096));
            let mut outcomes = Vec::new();
            loop {
                let outcome = if chunk.is_some() {
                    simplify(block_on(decoder.push_buffered_async(&mut reader)))
                } else {
                    simplify(block_on(decoder.push_async(&mut reader)))
                };
                let eof = matches!(outcome, Ok(progress) if progress.consumed == 0);
                outcomes.push(outcome);
                if eof {
                    break;
                }
                assert!(outcomes.len() < 100, "decoder made no progress");
            }
            assert!(reader.remaining().is_empty());
            (
                outcomes,
                decoder.dest().bytes().to_vec(),
                decoder.check_complete(),
            )
        }

        #[test]
        fn stateful_push_matches_push_async() {
            let long_nonzero: Vec<u8> = (0..300u32).map(|i| (i % 255) as u8 + 1).collect();
            let long_mixed: Vec<u8> = (0..700u32).map(|i| (i % 97) as u8).collect();
            let mut stream = vec![0, 0];
            for payload in [&[][..], &[0], &[7, 0, 8], &long_nonzero, &long_mixed] {
                stream.extend_from_slice(&framed(payload));
            }
            stream.extend_from_slice(&[3, 1, 0]);
            stream.extend_from_slice(&framed(&[9]));
            stream.extend_from_slice(&[4, 1, 2]);

            let expected = run_pushes(&stream, None);
            let frame_lens: Vec<_> = expected
                .0
                .iter()
                .filter_map(|outcome| outcome.as_ref().ok()?.frame_len)
                .collect();
            assert_eq!(frame_lens, [0, 1, 3, 300, 700, 1]);
            assert!(expected.0.iter().any(|outcome| matches!(
                outcome,
                Err(DecodeError::InvalidFrame(_))
            )));
            assert_eq!(expected.2, Err($crate::CompletionError::IncompleteFrame(2)));

            for chunk in [1, 2, 3, 5, 63, 64, 65, 200, 4096] {
                assert_eq!(run_pushes(&stream, Some(chunk)), expected, "chunk {chunk}");
            }
        }

        #[test]
        fn stateful_push_batches_reads_and_writes() {
            let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
            let mut input = framed(&payload);
            let first_len = input.len();
            let next = framed(&[5]);
            input.extend_from_slice(&next);

            let mut reader = ChunkedReader::new(input.clone(), usize::MAX);
            let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(2048));
            let expected = block_on(decoder.push_async(&mut reader)).unwrap();
            assert_eq!(expected.frame_len, Some(payload.len() as u64));
            let (reads, writes) = (reader.reads(), decoder.dest().io_calls());
            assert_eq!(reads, first_len);
            assert_eq!(writes, payload.len());

            let mut reader = ChunkedReader::new(input, 256);
            let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(2048));
            assert_eq!(
                block_on(decoder.push_buffered_async(&mut reader)).unwrap(),
                expected
            );
            assert_eq!(decoder.dest().bytes(), payload);
            assert_eq!(reader.remaining(), next);
            assert!(reader.fills() * 10 < reads);
            assert!(decoder.dest().io_calls() * 10 < writes);
        }

        #[test]
        fn stateful_push_write_failure_keeps_delimiter_for_recovery() {
            let payload: Vec<u8> = (1..=10).collect();
            let mut input = framed(&payload);
            input.extend_from_slice(&framed(&[9]));
            let mut reader = ChunkedReader::new(input, 4096);
            let mut writer = TestWriter::new(64);
            writer.set_script([Event {
                after_bytes: 4,
                action: Action::Fail,
            }]);
            let mut decoder = api::CobsDecoderAsync::new(writer);

            assert!(matches!(
                block_on(decoder.push_buffered_async(&mut reader)),
                Err(DecodeError::Destination(_))
            ));
            // The failed frame's delimiter is still unread.
            assert_eq!(reader.remaining(), &[0, 0, 2, 9, 0]);
            assert!(matches!(
                block_on(decoder.push_buffered_async(&mut reader)),
                Err(DecodeError::Poisoned)
            ));
            assert_eq!(reader.remaining(), &[0, 0, 2, 9, 0]);

            // The partially written batch was never acknowledged.
            assert_eq!(block_on(decoder.discard_frame_async(&mut reader)).unwrap(), 0);
            assert_eq!(reader.remaining(), &[0, 2, 9, 0]);
            assert_eq!(
                block_on(decoder.push_buffered_async(&mut reader)).unwrap(),
                DecodeProgress {
                    consumed: 4,
                    written: 1,
                    frame_len: Some(1),
                }
            );
            assert_eq!(decoder.dest().bytes(), &[1, 2, 3, 4, 9]);
        }

        #[test]
        fn stateful_push_on_empty_source_keeps_state() {
            let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
            let mut reader = ChunkedReader::new([2, 7], 4);
            assert_eq!(
                block_on(decoder.push_buffered_async(&mut reader)).unwrap(),
                DecodeProgress {
                    consumed: 2,
                    written: 1,
                    frame_len: None,
                }
            );
            let calls = decoder.dest().io_calls();
            let mut empty = ChunkedReader::new([], 4);
            assert_eq!(
                block_on(decoder.push_buffered_async(&mut empty)).unwrap(),
                DecodeProgress::default()
            );
            assert_eq!(decoder.dest().io_calls(), calls);
            assert_eq!(decoder.finish_frame(), Ok(1));
        }

        #[test]
        fn slice_decoder_push_buffered() {
            let mut reader = ChunkedReader::new([0, 2, 7, 2, 8, 0, 2, 9, 0], 2);
            let mut decoder = api::CobsDecoderAsync::new_to_slice([0x80; 4]);
            assert_eq!(
                block_on(decoder.push_buffered_async(&mut reader)).unwrap(),
                DecodeProgress {
                    consumed: 6,
                    written: 3,
                    frame_len: Some(3),
                }
            );
            assert_eq!(decoder.dest(), &[7, 0, 8, 0x80]);
            assert_eq!(reader.remaining(), &[2, 9, 0]);
        }
    };
}

#[cfg(feature = "embedded-io")]
mod embedded {
    buffered_decoder_tests!(
        crate::embedded,
        crate::tests::support::embedded_writer::TestWriter
    );

    #[test]
    fn slices_are_buffered_sources() {
        let mut source: &[u8] = &[0, 2, 7, 2, 8, 0, 2, 9, 0];
        let mut dest = [0; 3];
        assert_eq!(
            block_on(api::decode_to_slice_buffered_async(&mut source, &mut dest)).unwrap(),
            3
        );
        assert_eq!(dest, [7, 0, 8]);
        assert_eq!(source, &[2, 9, 0]);
    }
}

#[cfg(feature = "tokio")]
mod tokio {
    buffered_decoder_tests!(
        crate::tokio,
        crate::tests::support::tokio_writer::TestWriter
    );

    #[test]
    fn buf_reader_reduces_inner_reads_and_keeps_next_frame() {
        use ::tokio::io::BufReader;

        let payload: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let mut input = framed(&payload);
        input.extend_from_slice(&framed(&[5, 0]));

        let mut reader = BufReader::with_capacity(128, ChunkedReader::new(input.clone(), 4096));
        let mut dest = vec![0; payload.len()];
        let len = block_on(api::decode_to_slice_buffered_async(&mut reader, &mut dest)).unwrap();
        assert_eq!(len as usize, payload.len());
        assert_eq!(dest, payload);

        let mut next = [0; 2];
        assert_eq!(
            block_on(api::decode_to_slice_buffered_async(&mut reader, &mut next)).unwrap(),
            2
        );
        assert_eq!(next, [5, 0]);
        assert!(reader.get_ref().reads() <= input.len().div_ceil(128) + 1);
    }
}
