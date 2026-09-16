macro_rules! decoder_tests {
    ($backend:path, $writer:path) => {
        use $backend as api;
        use $writer as TestWriter;

        use futures::executor::block_on;
        use $crate::{CompletionError, DecodeError, DecodeProgress};
        use $crate::tests::support::{Action, Event, WakeCounter};

        #[test]
        fn padding_is_not_an_empty_frame() {
            block_on(async {
                let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                assert_eq!(
                    decoder.push_slice_async(&[0, 0]).await.unwrap(),
                    DecodeProgress {
                        consumed: 2,
                        written: 0,
                        frame_len: None,
                    }
                );
                assert_eq!(decoder.check_complete(), Ok(()));
                assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                assert!(decoder.dest().bytes().is_empty());
                assert_eq!(decoder.dest().storage(), &[0x80; 8]);
                assert_eq!(decoder.dest().position(), 0);
                assert_eq!(decoder.dest().io_calls(), 0);
            });
        }

        #[test]
        fn empty_frames_complete_without_writes() {
            block_on(async {
                let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(0));
                assert_eq!(
                    decoder.push_slice_async(&[1]).await.unwrap(),
                    DecodeProgress {
                        consumed: 1,
                        written: 0,
                        frame_len: None,
                    }
                );
                assert_eq!(decoder.check_complete(), Ok(()));
                assert_eq!(decoder.finish_frame(), Ok(0));
                assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                assert_eq!(
                    decoder.push_slice_async(&[1, 0]).await.unwrap(),
                    DecodeProgress {
                        consumed: 2,
                        written: 0,
                        frame_len: Some(0),
                    }
                );
                assert_eq!(decoder.check_complete(), Ok(()));
                assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                assert!(decoder.dest().bytes().is_empty());
                assert!(decoder.dest().storage().is_empty());
                assert_eq!(decoder.dest().position(), 0);
                assert_eq!(decoder.dest().io_calls(), 0);
            });
        }

        #[test]
        fn chunk_exhaustion_preserves_frame() {
            block_on(async {
                let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                assert_eq!(
                    decoder.push_slice_async(&[2, 7]).await.unwrap(),
                    DecodeProgress {
                        consumed: 2,
                        written: 1,
                        frame_len: None,
                    }
                );
                assert_eq!(decoder.check_complete(), Ok(()));
                let calls = decoder.dest().io_calls();
                assert_eq!(
                    decoder.push_slice_async(&[]).await.unwrap(),
                    DecodeProgress::default()
                );
                assert_eq!(decoder.dest().io_calls(), calls);
                assert_eq!(decoder.dest().bytes(), &[7]);
                assert_eq!(decoder.dest().position(), 1);
                assert_eq!(
                    decoder.push_slice_async(&[2, 8]).await.unwrap(),
                    DecodeProgress {
                        consumed: 2,
                        written: 2,
                        frame_len: None,
                    }
                );
                assert_eq!(decoder.check_complete(), Ok(()));
                assert_eq!(decoder.finish_frame(), Ok(3));
                assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                assert_eq!(decoder.dest().bytes(), &[7, 0, 8]);
                assert_eq!(&decoder.dest().storage()[3..], &[0x80; 5]);
            });
        }

        #[test]
        fn incomplete_finish_preserves_state() {
            block_on(async {
                let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                assert_eq!(
                    decoder.push_slice_async(&[3, 7]).await.unwrap(),
                    DecodeProgress {
                        consumed: 2,
                        written: 1,
                        frame_len: None,
                    }
                );
                let storage = decoder.dest().storage().to_vec();
                let position = decoder.dest().position();
                let calls = decoder.dest().io_calls();
                for _ in 0..2 {
                    assert_eq!(
                        decoder.check_complete(),
                        Err(CompletionError::IncompleteFrame(1))
                    );
                    assert_eq!(
                        decoder.finish_frame(),
                        Err(CompletionError::IncompleteFrame(1))
                    );
                }
                assert_eq!(decoder.dest().bytes(), &[7]);
                assert_eq!(decoder.dest().storage(), storage.as_slice());
                assert_eq!(decoder.dest().position(), position);
                assert_eq!(decoder.dest().io_calls(), calls);
                assert_eq!(
                    decoder.push_slice_async(&[8]).await.unwrap(),
                    DecodeProgress {
                        consumed: 1,
                        written: 1,
                        frame_len: None,
                    }
                );
                assert_eq!(decoder.check_complete(), Ok(()));
                assert_eq!(decoder.finish_frame(), Ok(2));
                assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                assert_eq!(decoder.dest().bytes(), &[7, 8]);
                assert_eq!(&decoder.dest().storage()[2..], &[0x80; 6]);
            });
        }

        #[test]
        fn stops_at_first_frame_boundary() {
            block_on(async {
                let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                let encoded = [0, 0, 1, 0, 2, 7, 0, 2, 8, 0];
                let mut source = encoded.as_slice();
                for (consumed, written, remaining) in [
                    (4, 0, &encoded[4..]),
                    (3, 1, &encoded[7..]),
                    (3, 1, &encoded[10..]),
                ] {
                    assert_eq!(
                        decoder.push_async(&mut source).await.unwrap(),
                        DecodeProgress {
                            consumed,
                            written,
                            frame_len: Some(written),
                        }
                    );
                    assert_eq!(source, remaining);
                    assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                }
                assert!(source.is_empty());
                assert_eq!(decoder.dest().bytes(), &[7, 8]);
                assert_eq!(decoder.dest().position(), 2);
                assert_eq!(&decoder.dest().storage()[2..], &[0x80; 6]);
            });
        }

        #[test]
        fn invalid_frame_preserves_next_frame() {
            block_on(async {
                let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                assert_eq!(
                    decoder.push_slice_async(&[4, 7]).await.unwrap(),
                    DecodeProgress {
                        consumed: 2,
                        written: 1,
                        frame_len: None,
                    }
                );
                let remaining = [8, 0, 2, 9, 0];
                let progress = match decoder.push_slice_async(&remaining).await {
                    Err(DecodeError::InvalidFrame(progress)) => progress,
                    other => panic!("expected invalid framing, got {other:?}"),
                };
                assert_eq!(
                    progress,
                    DecodeProgress {
                        consumed: 2,
                        written: 1,
                        frame_len: None,
                    }
                );
                assert_eq!(
                    decoder
                        .push_slice_async(&remaining[progress.consumed as usize..])
                        .await
                        .unwrap(),
                    DecodeProgress {
                        consumed: 3,
                        written: 1,
                        frame_len: Some(1),
                    }
                );
                assert_eq!(decoder.dest().bytes(), &[7, 8, 9]);
                assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
            });
        }

        #[test]
        fn full_blocks_do_not_insert_extra_zeros() {
            for run_len in [253usize, 254] {
                let mut encoded = std::vec![run_len as u8 + 1];
                encoded.extend(std::iter::repeat_n(0x55, run_len));
                encoded.extend_from_slice(&[2, 0x66, 0]);
                let mut expected = std::vec![0x55; run_len];
                if run_len == 253 {
                    expected.push(0);
                }
                expected.push(0x66);
                for split in 0..=encoded.len() {
                    for use_reader in [false, true] {
                        block_on(async {
                            let mut decoder = api::CobsDecoderAsync::new(
                                TestWriter::new(expected.len() + 4),
                            );
                            let mut written = 0;
                            let mut frame_len = None;
                            for chunk in [&encoded[..split], &encoded[split..]] {
                                let progress = if use_reader {
                                    let mut reader = chunk;
                                    let progress = decoder
                                        .push_async(&mut reader)
                                        .await
                                        .unwrap();
                                    assert!(reader.is_empty());
                                    progress
                                } else {
                                    decoder.push_slice_async(chunk).await.unwrap()
                                };
                                assert_eq!(progress.consumed, chunk.len() as u64);
                                written += progress.written;
                                if let Some(len) = progress.frame_len {
                                    assert!(frame_len.replace(len).is_none());
                                }
                            }
                            assert_eq!(written, expected.len() as u64);
                            assert_eq!(frame_len, Some(expected.len() as u64));
                            assert_eq!(decoder.dest().bytes(), expected.as_slice());
                            assert_eq!(
                                &decoder.dest().storage()[expected.len()..],
                                &[0x80; 4],
                            );
                            assert_eq!(
                                decoder.finish_frame(),
                                Err(CompletionError::NoFrame),
                            );
                        });
                    }
                }
            }
        }

        #[test]
        fn completion_does_not_touch_destination() {
            for finish in [false, true] {
                block_on(async {
                    let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                    let _ = decoder.push_slice_async(&[2, 7]).await.unwrap();
                    decoder.dest_mut().set_position(6);
                    let bytes = decoder.dest().bytes().to_vec();
                    let storage = decoder.dest().storage().to_vec();
                    let position = decoder.dest().position();
                    let calls = decoder.dest().io_calls();
                    assert_eq!(decoder.check_complete(), Ok(()));
                    assert_eq!(decoder.dest_mut().bytes(), bytes.as_slice());
                    if finish {
                        assert_eq!(decoder.finish_frame(), Ok(1));
                        assert_eq!(decoder.check_complete(), Ok(()));
                        assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                    }
                    assert_eq!(decoder.dest().bytes(), bytes.as_slice());
                    assert_eq!(decoder.dest().storage(), storage.as_slice());
                    assert_eq!(decoder.dest().position(), position);
                    assert_eq!(decoder.dest().io_calls(), calls);
                    let dest = decoder.into_inner();
                    assert_eq!(dest.bytes(), bytes.as_slice());
                    assert_eq!(dest.storage(), storage.as_slice());
                    assert_eq!(dest.position(), position);
                    assert_eq!(dest.io_calls(), calls);
                });
            }
        }

        #[test]
        fn external_writes_are_not_frame_progress() {
            for terminated in [false, true] {
                block_on(async {
                    let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                    assert_eq!(
                        decoder.push_slice_async(&[2, 7]).await.unwrap(),
                        DecodeProgress {
                            consumed: 2,
                            written: 1,
                            frame_len: None,
                        }
                    );
                    assert_eq!(decoder.dest_mut().write_bytes(b"X").unwrap(), 1);
                    let mut source: &[u8] = if terminated {
                        &[2, 8, 0]
                    } else {
                        &[2, 8]
                    };
                    assert_eq!(
                        decoder.push_async(&mut source).await.unwrap(),
                        DecodeProgress {
                            consumed: if terminated { 3 } else { 2 },
                            written: 2,
                            frame_len: if terminated { Some(3) } else { None },
                        }
                    );
                    assert!(source.is_empty());
                    if !terminated {
                        assert_eq!(decoder.finish_frame(), Ok(3));
                    }
                    assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
                    assert_eq!(decoder.dest().bytes(), &[7, b'X', 0, 8]);
                    assert_eq!(decoder.dest().position(), 4);
                    assert_eq!(&decoder.dest().storage()[4..], &[0x80; 4]);
                });
            }
        }

        #[test]
        fn cancelled_zero_write_requires_recovery() {
            use core::{future::Future, task::Context};
            let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
            let _ = block_on(decoder.push_slice_async(&[2, b'A'])).unwrap();
            let after_bytes = decoder.dest().accepted_bytes();
            decoder.dest_mut().set_script([Event {
                after_bytes,
                action: Action::PendingForever,
            }]);
            let mut source: &[u8] = &[2, b'B', 0, 2, b'C', 0];
            {
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                let mut future = core::pin::pin!(decoder.push_async(&mut source));
                assert!(future.as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(source, &[b'B', 0, 2, b'C', 0]);
            assert_eq!(decoder.dest().bytes(), b"A");
            assert_eq!(decoder.check_complete(), Err(CompletionError::InvalidState));
            assert_eq!(decoder.finish_frame(), Err(CompletionError::InvalidState));
            let calls = decoder.dest().io_calls();
            let remaining = source;
            assert!(matches!(
                block_on(decoder.push_async(&mut source)),
                Err(DecodeError::Poisoned)
            ));
            assert!(matches!(
                block_on(decoder.push_slice_async(&[])),
                Err(DecodeError::Poisoned)
            ));
            assert_eq!(source, remaining);
            assert_eq!(decoder.dest().io_calls(), calls);
            assert_eq!(
                block_on(decoder.discard_frame_async(&mut source)).unwrap(),
                1
            );
            assert_eq!(source, &[2, b'C', 0]);
            assert_eq!(decoder.dest().bytes(), b"A");
            assert_eq!(decoder.dest().io_calls(), calls);
            assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
            decoder.dest_mut().clear_script();
            assert_eq!(
                block_on(decoder.push_async(&mut source)).unwrap(),
                DecodeProgress {
                    consumed: 3,
                    written: 1,
                    frame_len: Some(1),
                }
            );
            assert!(source.is_empty());
            assert_eq!(decoder.dest().bytes(), b"AC");
            assert_eq!(&decoder.dest().storage()[2..], &[0x80; 6]);
        }

        #[test]
        fn failed_discard_preserves_count_until_successful_retry() {
            block_on(async {
                let mut decoder = api::CobsDecoderAsync::new(TestWriter::new(8));
                let _ = decoder.push_slice_async(&[4, 7]).await.unwrap();
                let calls = decoder.dest().io_calls();
                let mut incomplete: &[u8] = &[8, 9];
                assert!(matches!(
                    decoder.discard_frame_async(&mut incomplete).await,
                    Err(DecodeError::UnexpectedSourceEof)
                ));
                assert!(incomplete.is_empty());
                assert_eq!(
                    decoder.check_complete(),
                    Err(CompletionError::InvalidState)
                );
                assert!(matches!(
                    decoder.push_slice_async(&[]).await,
                    Err(DecodeError::Poisoned)
                ));
                let mut remaining: &[u8] = &[0, 2, 10, 0];
                assert_eq!(
                    decoder.discard_frame_async(&mut remaining).await.unwrap(),
                    1
                );
                assert_eq!(remaining, &[2, 10, 0]);
                assert_eq!(decoder.dest().bytes(), &[7]);
                assert_eq!(decoder.dest().io_calls(), calls);
                let progress = decoder.push_async(&mut remaining).await.unwrap();
                assert_eq!(
                    progress,
                    DecodeProgress {
                        consumed: 3,
                        written: 1,
                        frame_len: Some(1),
                    }
                );
                assert!(remaining.is_empty());
                assert_eq!(decoder.dest().bytes(), &[7, 10]);
            });
        }

        #[test]
        fn decode_malformed() {
            block_on(async {
                let malformed_buf: [u8; 32] = [
                    68, 69, 65, 68, 66, 69, 69, 70, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                ];
                let mut dest_buf: [u8; 32] = [0x80; 32];
                let mut source = malformed_buf.as_slice();
                let progress = match api::decode_to_slice_async(&mut source, &mut dest_buf).await {
                    Err(DecodeError::InvalidFrame(progress)) => progress,
                    other => panic!("expected invalid framing, got {other:?}"),
                };
                assert_eq!(
                    progress,
                    DecodeProgress {
                        consumed: 9,
                        written: 7,
                        frame_len: None,
                    }
                );
                assert_eq!(source, &malformed_buf[9..]);
                assert_eq!(&dest_buf[..7], &malformed_buf[1..8]);
                assert_eq!(&dest_buf[7..], &[0x80; 25]);
            });
        }

        #[test]
        fn decode_malformed2() {
            block_on(async {
                let malformed_buf: [u8; 8] = [68, 69, 65, 68, 66, 69, 69, 70];
                let mut dest_buf = [0x80; 32];
                let mut source = malformed_buf.as_slice();
                assert!(matches!(
                    api::decode_to_slice_async(&mut source, &mut dest_buf).await,
                    Err(DecodeError::UnexpectedSourceEof)
                ));
                assert!(source.is_empty());
                assert_eq!(&dest_buf[..7], &malformed_buf[1..]);
                assert_eq!(&dest_buf[7..], &[0x80; 25]);
            });
        }

        #[test]
        fn decode_malformed3() {
            block_on(async {
                let malformed_buf: [u8; 2] = [3, 100];
                let mut dest_buf = [0x80; 32];
                let mut source = malformed_buf.as_slice();
                assert!(matches!(
                    api::decode_to_slice_async(&mut source, &mut dest_buf).await,
                    Err(DecodeError::UnexpectedSourceEof)
                ));
                assert!(source.is_empty());
                assert_eq!(&dest_buf[..1], &[100]);
                assert_eq!(&dest_buf[1..], &[0x80; 31]);
            });
        }

        #[test]
        fn decode_empty() {
            block_on(async {
                let mut source: &[u8] = &[];
                assert!(matches!(
                    api::decode_to_slice_async(&mut source, &mut []).await,
                    Err(DecodeError::EmptyFrame)
                ));
                assert!(source.is_empty());
            });
        }

        #[test]
        fn decode_nothing() {
            block_on(async {
                let mut source: &[u8] = &[1];
                assert_eq!(
                    api::decode_to_slice_async(&mut source, &mut [])
                        .await
                        .unwrap(),
                    0
                );
                assert!(source.is_empty());
            });
        }

        #[test]
        fn dest_returns_correctly() {
            block_on(async {
                let mut output_buffer = TestWriter::new(32);
                let expected = core::ptr::from_ref(&output_buffer);
                let decoder = api::CobsDecoderAsync::new(&mut output_buffer);
                assert_eq!(expected, core::ptr::from_ref(&**decoder.dest()));
            });
        }

        #[test]
        fn dest_mut_returns_correctly() {
            block_on(async {
                let mut output_buffer = TestWriter::new(32);
                let expected = core::ptr::from_mut(&mut output_buffer);
                let mut decoder = api::CobsDecoderAsync::new(&mut output_buffer);
                assert_eq!(expected, core::ptr::from_mut(&mut **decoder.dest_mut()));
            });
        }

        #[test]
        fn into_inner_returns_correctly() {
            block_on(async {
                let mut output_buffer = TestWriter::new(32);
                let expected = core::ptr::from_mut(&mut output_buffer);
                let decoder = api::CobsDecoderAsync::new(&mut output_buffer);
                assert_eq!(expected, core::ptr::from_mut(&mut *decoder.into_inner()));
            });
        }

        #[test]
        fn pending_once_resumes_the_same_push() {
            use core::{
                future::Future,
                task::{Context, Poll},
            };
            use std::sync::Arc;
            let mut writer = TestWriter::new(8);
            writer.set_script([Event {
                after_bytes: 1,
                action: Action::PendingOnce,
            }]);
            let mut decoder = api::CobsDecoderAsync::new(writer);
            let mut source: &[u8] = &[3, b'A', b'B', 0, 2, b'C', 0];
            let wakes = Arc::new(WakeCounter::default());
            let waker = futures::task::waker(wakes.clone());
            let mut cx = Context::from_waker(&waker);
            {
                let mut future = core::pin::pin!(decoder.push_async(&mut source));
                assert!(future.as_mut().poll(&mut cx).is_pending());
                assert_eq!(wakes.count(), 1);
                let progress = match future.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(progress)) => progress,
                    other => panic!("expected completed frame, got {other:?}"),
                };
                assert_eq!(
                    progress,
                    DecodeProgress {
                        consumed: 4,
                        written: 2,
                        frame_len: Some(2),
                    }
                );
            }
            assert_eq!(wakes.count(), 1);
            assert_eq!(source, &[2, b'C', 0]);
            assert_eq!(decoder.dest().bytes(), b"AB");
            assert_eq!(decoder.dest().position(), 2);
            assert_eq!(decoder.dest().accepted_bytes(), 2);
            assert_eq!(decoder.dest().io_calls(), 3);
            assert_eq!(&decoder.dest().storage()[2..], &[0x80; 6]);
            assert_eq!(decoder.check_complete(), Ok(()));
            assert_eq!(decoder.finish_frame(), Err(CompletionError::NoFrame));
        }

        #[test]
        fn injected_write_failure_preserves_acknowledged_prefix() {
            block_on(async {
                let mut writer = TestWriter::new(8);
                writer.set_script([Event {
                    after_bytes: 1,
                    action: Action::Fail,
                }]);
                let mut decoder = api::CobsDecoderAsync::new(writer);
                let mut source: &[u8] = &[3, b'A', b'B', 0, 2, b'C', 0];
                assert!(matches!(
                    decoder.push_async(&mut source).await,
                    Err(DecodeError::Destination(_))
                ));
                assert_eq!(source, &[0, 2, b'C', 0]);
                assert_eq!(decoder.dest().bytes(), b"A");
                assert_eq!(decoder.dest().accepted_bytes(), 1);
                assert_eq!(decoder.dest().io_calls(), 2);
                assert_eq!(
                    decoder.check_complete(),
                    Err(CompletionError::InvalidState)
                );
                assert_eq!(
                    decoder.finish_frame(),
                    Err(CompletionError::InvalidState)
                );
                assert!(matches!(
                    decoder.push_async(&mut source).await,
                    Err(DecodeError::Poisoned)
                ));
                assert!(matches!(
                    decoder.push_slice_async(&[]).await,
                    Err(DecodeError::Poisoned)
                ));
                assert_eq!(source, &[0, 2, b'C', 0]);
                assert_eq!(decoder.dest().io_calls(), 2);
                assert_eq!(decoder.discard_frame_async(&mut source).await.unwrap(), 1);
                assert_eq!(source, &[2, b'C', 0]);
                assert_eq!(decoder.dest().bytes(), b"A");
                assert_eq!(decoder.dest().io_calls(), 2);
                assert_eq!(
                    decoder.push_async(&mut source).await.unwrap(),
                    DecodeProgress {
                        consumed: 3,
                        written: 1,
                        frame_len: Some(1),
                    }
                );
                assert!(source.is_empty());
                assert_eq!(decoder.dest().bytes(), b"AC");
                assert_eq!(decoder.dest().accepted_bytes(), 2);
                assert_eq!(decoder.dest().io_calls(), 3);
                assert_eq!(&decoder.dest().storage()[2..], &[0x80; 6]);
            });
        }
    };
}

pub(crate) use decoder_tests;
