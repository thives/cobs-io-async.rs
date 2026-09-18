macro_rules! encoder_tests {
    ($backend:path, $writer:path) => {
        use $backend as api;
        use $writer as TestWriter;

        #[cfg(feature = "embedded-io")]
        #[allow(unused_imports)]
        use $crate::embedded::Write;

        use futures::executor::block_on;
        use $crate::EncodeError;

        #[derive(Clone, Copy, Debug)]
        enum Mode {
            Reader,
            Slice,
            Stream,
            OneShot,
            Delimited,
        }

        fn check_vector(name: &str, input: &[u8], expected: &[u8]) {
            for mode in [
                Mode::Reader,
                Mode::Slice,
                Mode::Stream,
                Mode::OneShot,
                Mode::Delimited,
            ] {
                block_on(async {
                    let framed = matches!(mode, Mode::Delimited);
                    let expected_len = expected.len() + if framed { 2 } else { 0 };
                    let mut writer = TestWriter::new(expected_len + 8);
                    let written = match mode {
                        Mode::Reader => {
                            let mut reader = input;
                            let mut encoder = api::CobsEncoderAsync::new(&mut writer);

                            encoder.push_async(&mut reader).await.unwrap();
                            assert!(reader.is_empty(), "{name}: reader not exhausted");
                            encoder.finalize_async().await.unwrap()
                        }
                        Mode::Slice => {
                            let mut encoder = api::CobsEncoderAsync::new(&mut writer);

                            encoder.push_slice_async(input).await.unwrap();
                            encoder.finalize_async().await.unwrap()
                        }
                        Mode::Stream => {
                            if api::CobsEncoderAsync::<TestWriter>::can_stream() {
                                let mut reader = input;
                                let mut encoder = api::CobsEncoderAsync::new(&mut writer);
                                encoder.write(&mut reader).await.unwrap();
                                encoder.flush().await.unwrap();
                                encoder.finalize_async().await.unwrap()
                            } else {
                                return;
                            }
                        }
                        Mode::OneShot => api::encode_from_slice_async(input, &mut writer)
                            .await
                            .unwrap(),
                        Mode::Delimited => {
                            api::encode_from_slice_including_sentinels_async(input, &mut writer)
                                .await
                                .unwrap()
                        }
                    };
                    assert_eq!(written, expected_len as u64, "{name}: {mode:?}",);
                    assert_eq!(writer.bytes().len(), expected_len, "{name}: {mode:?}",);
                    assert_eq!(writer.position(), written, "{name}: {mode:?}",);
                    let start = if framed { 1 } else { 0 };
                    assert_eq!(
                        &writer.bytes()[start..start + expected.len()],
                        expected,
                        "{name}: {mode:?}",
                    );
                    if framed {
                        assert_eq!(writer.bytes()[0], 0);
                        assert_eq!(writer.bytes()[expected_len - 1], 0);
                    }
                    assert!(
                        writer.storage()[expected_len..]
                            .iter()
                            .all(|&byte| byte == 0x80),
                        "{name}: {mode:?} modified spare capacity",
                    );
                });
            }
        }

        #[test]
        fn known_vectors() {
            let cases: &[(&str, &[u8], &[u8])] = &[
                ("empty", &[], &[1]),
                ("interior_zero", &[10, 11, 0, 12], &[3, 10, 11, 2, 12]),
                ("nonzero", &[10, 11, 1, 12], &[5, 10, 11, 1, 12]),
                ("consecutive_zeros", &[0, 0, 1, 0], &[1, 1, 2, 1, 1]),
                ("trailing_zero", &[255, 0], &[2, 255, 1]),
                ("single_byte", &[1], &[2, 1]),
                (
                    "multiple_blocks",
                    &[10, 11, 0, 12, 0, 0, 3],
                    &[3, 10, 11, 2, 12, 1, 2, 3],
                ),
                ("issue_15", &[0, 17, 0, 34], &[1, 2, 17, 2, 34]),
            ];
            for &(name, input, expected) in cases {
                check_vector(name, input, expected);
            }
        }

        #[test]
        fn full_blocks_have_no_redundant_trailing_code() {
            for (name, byte) in [("issue_19_all_ones", 1), ("full_block_a5", 0xA5)] {
                let input = [byte; 254];
                let mut expected = [byte; 255];
                expected[0] = 0xFF;
                check_vector(name, &input, &expected);
            }
        }

        #[test]
        fn owned_writer_and_accessors_preserve_destination() {
            block_on(async {
                let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(16));
                assert!(encoder.dest().bytes().is_empty());
                assert_eq!(encoder.dest().io_calls(), 0);
                encoder.dest_mut().set_position(2);
                assert_eq!(encoder.dest().position(), 2);
                encoder.push_slice_async(b"A").await.unwrap();
                assert_eq!(encoder.finalize_async().await.unwrap(), 2);
                let writer = encoder.into_inner();
                assert_eq!(writer.position(), 4);
                assert_eq!(writer.bytes(), &[0x80, 0x80, 2, b'A']);
            });
        }

        #[test]
        fn owned_writer_and_accessors_preserve_destination_to_slice() {
            block_on(async {
                let mut slice = [0x80; 16];
                let mut encoder = api::CobsEncoderAsync::new_to_slice(&mut slice);
                encoder.push_slice_async(b"A").await.unwrap();
                let n = encoder.finalize_async().await.unwrap() as usize;
                assert_eq!(n, 2);
                let slice = encoder.into_inner();
                assert_eq!(&slice[..n], &[2, b'A']);
            });
        }

        #[test]
        fn empty_finalization_is_idempotent_and_rejects_pushes() {
            block_on(async {
                let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(16));
                assert_eq!(encoder.finalize_async().await.unwrap(), 1);
                assert_eq!(encoder.dest().bytes(), &[1]);
                encoder.dest_mut().set_position(0);
                let calls = encoder.dest().io_calls();
                assert_eq!(encoder.finalize_async().await.unwrap(), 1);
                assert_eq!(encoder.dest().position(), 0);
                assert!(matches!(
                    encoder.push_slice_async(b"A").await,
                    Err(EncodeError::AlreadyFinalized)
                ));
                let mut reader: &[u8] = b"A";
                assert!(matches!(
                    encoder.push_async(&mut reader).await,
                    Err(EncodeError::AlreadyFinalized)
                ));
                assert_eq!(reader, b"A");
                assert_eq!(encoder.dest().bytes(), &[1]);
                assert_eq!(encoder.dest().io_calls(), calls);
            });
        }

        #[test]
        fn failure_poisons_until_reset_at_acknowledged_boundary() {
            block_on(async {
                let mut writer = TestWriter::new(32);
                writer.set_write_limit(3);
                let mut encoder = api::CobsEncoderAsync::new(writer);
                assert!(matches!(
                    encoder.push_slice_async(&[1, 2, 3]).await,
                    Err(EncodeError::Destination(_))
                ));
                assert_eq!(encoder.dest().bytes(), &[0, 1, 2]);
                let calls = encoder.dest().io_calls();
                assert!(matches!(
                    encoder.finalize_async().await,
                    Err(EncodeError::Poisoned)
                ));
                assert!(matches!(
                    encoder.push_slice_async(&[9]).await,
                    Err(EncodeError::Poisoned)
                ));
                assert_eq!(encoder.dest().io_calls(), calls);
                encoder.dest_mut().set_write_limit(32);
                encoder.reset_async().await.unwrap();
                assert_eq!(encoder.dest().bytes(), &[0, 1, 2, 0]);
                encoder.push_slice_async(&[1, 2]).await.unwrap();
                assert_eq!(encoder.finalize_async().await.unwrap(), 3);
                assert_eq!(&encoder.dest().bytes()[4..], &[3, 1, 2]);
            });
        }

        #[test]
        fn reset_preserves_next_frame_start_after_cursor_move() {
            block_on(async {
                let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(32));
                encoder.push_slice_async(b"A").await.unwrap();
                assert_eq!(encoder.finalize_async().await.unwrap(), 2);
                encoder.reset_async().await.unwrap();
                encoder.dest_mut().set_position(0);
                encoder.push_slice_async(b"B").await.unwrap();
                assert_eq!(encoder.finalize_async().await.unwrap(), 2);
                assert_eq!(encoder.dest().bytes(), &[2, b'A', 0, 2, b'B']);
            });
        }

        #[test]
        fn multiple_full_blocks_have_no_redundant_trailing_code() {
            let input = [0xA5; 508];
            let mut expected = [0xA5; 510];
            expected[0] = 0xFF;
            expected[255] = 0xFF;
            check_vector("two_full_blocks", &input, &expected);
        }

        #[test]
        fn nonzero_byte_after_full_block_starts_another_block() {
            let input = [1; 255];
            let mut expected = [1; 257];
            expected[0] = 0xFF;
            expected[255] = 2;
            check_vector("full_block_then_nonzero", &input, &expected);
        }

        #[test]
        fn empty_pushes_do_not_materialize_deferred_placeholder() {
            block_on(async {
                let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(264));
                encoder.push_slice_async(&[1; 254]).await.unwrap();
                let before = encoder.dest().bytes().to_vec();
                let calls = encoder.dest().io_calls();
                let position = encoder.dest().position();
                encoder.push_slice_async(&[]).await.unwrap();
                let mut empty: &[u8] = &[];
                encoder.push_async(&mut empty).await.unwrap();
                assert_eq!(encoder.dest().bytes(), before.as_slice());
                assert_eq!(encoder.dest().io_calls(), calls);
                assert_eq!(encoder.dest().position(), position);
                assert_eq!(encoder.finalize_async().await.unwrap(), 255);
                assert_eq!(encoder.dest().bytes()[0], 0xFF);
                assert_eq!(&encoder.dest().bytes()[1..], &[1; 254]);
                assert!(
                    encoder.dest().storage()[255..]
                        .iter()
                        .all(|&byte| byte == 0x80)
                );
            });
        }

        #[test]
        fn zero_after_full_block_is_independent_of_chunk_boundaries() {
            let mut input = [1; 255];
            input[254] = 0;
            let mut expected = [1; 257];
            expected[0] = 0xFF;
            check_vector("full_block_then_zero", &input, &expected);
            block_on(async {
                for reader_input in [false, true] {
                    for split in 0..=input.len() {
                        let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(265));
                        for chunk in [&input[..split], &[], &input[split..]] {
                            if reader_input {
                                let mut reader = chunk;
                                encoder.push_async(&mut reader).await.unwrap();
                                assert!(reader.is_empty());
                            } else {
                                encoder.push_slice_async(chunk).await.unwrap();
                            }
                        }
                        assert_eq!(
                            encoder.finalize_async().await.unwrap(),
                            expected.len() as u64,
                            "split={split}, reader_input={reader_input}",
                        );
                        assert_eq!(
                            encoder.dest().bytes(),
                            expected.as_slice(),
                            "split={split}, reader_input={reader_input}",
                        );
                        assert!(
                            encoder.dest().storage()[expected.len()..]
                                .iter()
                                .all(|&byte| byte == 0x80)
                        );
                    }
                }
            });
        }

        #[test]
        fn zero_after_full_block_is_independent_of_chunk_boundaries_slice_dest() {
            let mut input = [1; 255];
            input[254] = 0;
            let mut expected = [1; 257];
            expected[0] = 0xFF;
            check_vector("full_block_then_zero", &input, &expected);
            block_on(async {
                for reader_input in [false, true] {
                    for split in 0..=input.len() {
                        let mut slice = [0x80; 265];
                        let mut encoder = api::CobsEncoderAsync::new_to_slice(&mut slice);
                        let mut encoded_len = 0;
                        for chunk in [&input[..split], &[], &input[split..]] {
                            if reader_input {
                                let mut reader = chunk;
                                encoded_len += encoder.push_async(&mut reader).await.unwrap();
                                assert!(reader.is_empty());
                            } else {
                                encoded_len += encoder.push_slice_async(chunk).await.unwrap();
                            }
                        }
                        assert_eq!(
                            encoder.finalize_async().await.unwrap(),
                            expected.len() as u64,
                            "split={split}, reader_input={reader_input}",
                        );
                        assert_eq!(
                            &encoder.dest()[..encoded_len],
                            expected.as_slice(),
                            "split={split}, reader_input={reader_input}",
                        );
                        assert!(
                            encoder.dest()[expected.len()..]
                                .iter()
                                .all(|&byte| byte == 0x80)
                        );
                    }
                }
            });
        }

        #[test]
        fn chunked_encoding_matches_one_shot_for_varying_lengths() {
            block_on(async {
                let source: [u8; 1000] = core::array::from_fn(|index| (index & 0xFF) as u8);
                for len in 0..=source.len() {
                    let input = &source[..len];
                    let bound = $crate::max_encoding_length(len);
                    let mut one_shot = TestWriter::new(bound + 8);
                    let expected_len = api::encode_from_slice_async(input, &mut one_shot)
                        .await
                        .unwrap();
                    let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(bound + 8));
                    for chunk in input.chunks(17) {
                        encoder.push_slice_async(chunk).await.unwrap();
                    }
                    let written = encoder.finalize_async().await.unwrap();
                    assert_eq!(written, expected_len, "input length={len}");
                    assert!(written > 0 && written <= bound as u64);
                    assert_eq!(
                        encoder.dest().bytes(),
                        one_shot.bytes(),
                        "input length={len}",
                    );
                    assert!(encoder.dest().bytes().iter().all(|&byte| byte != 0));
                    assert!(
                        encoder.dest().storage()[written as usize..]
                            .iter()
                            .all(|&byte| byte == 0x80)
                    );
                    assert!(
                        one_shot.storage()[expected_len as usize..]
                            .iter()
                            .all(|&byte| byte == 0x80)
                    );
                }
            });
        }

        #[test]
        fn dropping_unpolled_operations_has_no_effect() {
            let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(16));
            let mut reader: &[u8] = b"A";
            drop(encoder.push_async(&mut reader));
            drop(encoder.push_slice_async(b"B"));
            drop(encoder.finalize_async());
            drop(encoder.reset_async());
            assert_eq!(reader, b"A");
            assert!(encoder.dest().bytes().is_empty());
            assert_eq!(encoder.dest().position(), 0);
            assert_eq!(encoder.dest().io_calls(), 0);
            assert_eq!(block_on(encoder.finalize_async()).unwrap(), 1);
            assert_eq!(encoder.dest().bytes(), &[1]);
        }

        #[test]
        fn cancelled_write_poisons_until_reset() {
            use core::{future::Future, task::Context};
            let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(32));
            block_on(encoder.push_slice_async(b"A")).unwrap();
            assert_eq!(encoder.dest().bytes(), &[0, b'A']);
            encoder.dest_mut().set_pending_writes(true);
            let calls_before = encoder.dest().io_calls();
            {
                let waker = futures::task::noop_waker();
                let mut context = Context::from_waker(&waker);
                let mut future = core::pin::pin!(encoder.push_slice_async(b"B"));
                assert!(future.as_mut().poll(&mut context).is_pending());
            }
            assert!(encoder.dest().io_calls() > calls_before);
            assert_eq!(encoder.dest().bytes(), &[0, b'A']);
            let calls = encoder.dest().io_calls();
            assert!(matches!(
                block_on(encoder.finalize_async()),
                Err(EncodeError::Poisoned)
            ));
            assert!(matches!(
                block_on(encoder.push_slice_async(b"C")),
                Err(EncodeError::Poisoned)
            ));
            assert_eq!(encoder.dest().io_calls(), calls);
            encoder.dest_mut().set_pending_writes(false);
            block_on(encoder.reset_async()).unwrap();
            assert_eq!(encoder.dest().bytes(), &[0, b'A', 0]);
            block_on(encoder.push_slice_async(b"C")).unwrap();
            assert_eq!(block_on(encoder.finalize_async()).unwrap(), 2);
            assert_eq!(encoder.dest().bytes(), &[0, b'A', 0, 2, b'C']);
        }

        #[test]
        fn zero_write_is_reported_and_poisons_encoder() {
            block_on(async {
                let mut writer = TestWriter::new(16);
                writer.set_zero_writes(true);
                let mut encoder = api::CobsEncoderAsync::new(writer);
                assert!(matches!(
                    encoder.push_slice_async(b"A").await,
                    Err(EncodeError::WriteZero)
                ));
                assert!(encoder.dest().bytes().is_empty());
                let calls = encoder.dest().io_calls();
                assert!(matches!(
                    encoder.finalize_async().await,
                    Err(EncodeError::Poisoned)
                ));
                assert_eq!(encoder.dest().io_calls(), calls);
                encoder.dest_mut().set_zero_writes(false);
                encoder.reset_async().await.unwrap();
                encoder.push_slice_async(b"A").await.unwrap();
                assert_eq!(encoder.finalize_async().await.unwrap(), 2);
                assert_eq!(encoder.dest().bytes(), &[0, 2, b'A']);
            });
        }

        #[test]
        fn reset_from_new_and_repeated_reset_create_boundaries() {
            block_on(async {
                let mut encoder = api::CobsEncoderAsync::new(TestWriter::new(16));
                encoder.reset_async().await.unwrap();
                encoder.reset_async().await.unwrap();
                assert_eq!(encoder.dest().bytes(), &[0, 0]);
                assert_eq!(encoder.dest().position(), 2);
                assert_eq!(encoder.finalize_async().await.unwrap(), 1);
                assert_eq!(encoder.dest().bytes(), &[0, 0, 1]);
            });
        }

        #[test]
        fn reset_from_new_and_repeated_reset_create_boundaries_to_slice() {
            block_on(async {
                let mut slice = [0x80; 16];
                let mut encoder = api::CobsEncoderAsync::new_to_slice(&mut slice);
                encoder.reset_async().await.unwrap();
                encoder.reset_async().await.unwrap();
                assert_eq!(&encoder.dest()[..2], &mut [0, 0]);
            });
        }

        #[test]
        fn dest_returns_correctly() {
            block_on(async {
                let mut output_buffer = TestWriter::new(32);
                let expected = core::ptr::from_ref(&output_buffer);
                let encoder = api::CobsEncoderAsync::new(&mut output_buffer);
                assert_eq!(expected, core::ptr::from_ref(&**encoder.dest()));
            });
        }

        #[test]
        fn dest_mut_returns_correctly() {
            block_on(async {
                let mut output_buffer = TestWriter::new(32);
                let expected = core::ptr::from_mut(&mut output_buffer);
                let mut encoder = api::CobsEncoderAsync::new(&mut output_buffer);
                assert_eq!(expected, core::ptr::from_mut(&mut **encoder.dest_mut()));
            });
        }

        #[test]
        fn dest_returns_correctly_to_slice() {
            block_on(async {
                let mut slice = [0x80; 32];
                let expected = core::ptr::from_ref(slice.as_slice());
                let encoder = api::CobsEncoderAsync::new_to_slice(&mut slice);
                assert_eq!(expected, core::ptr::from_ref(encoder.dest()));
            });
        }

        #[test]
        fn dest_mut_returns_correctly_to_slice() {
            block_on(async {
                let mut slice = [0x80; 32];
                let expected = core::ptr::from_mut(slice.as_mut_slice());
                let mut encoder = api::CobsEncoderAsync::new_to_slice(&mut slice);
                assert_eq!(expected, core::ptr::from_mut(encoder.dest_mut()));
            });
        }

        #[test]
        fn overflow_into_dest_errors_no_panic() {
            block_on(async {
                let mut slice = [0x80; 32];
                let input = [0xFF; 64];
                let mut encoder = api::CobsEncoderAsync::new_to_slice(&mut slice);
                assert!(matches!(
                    encoder.push_slice_async(&input).await,
                    Err(EncodeError::Destination(_))
                ));
            });
        }
    };
}

pub(crate) use encoder_tests;
