use std::{vec, vec::Vec};

use crate::sync::{
    DecodedFrame, decode_to_slice, encode_from_slice, encode_from_slice_including_sentinels,
};
use crate::{DecodeError, DecodeProgress, SeekableError, max_encoding_length};

/// Straightforward COBS encoder used as an independent oracle.
fn reference_encode(data: &[u8]) -> Vec<u8> {
    let mut out = vec![0];
    let mut code_idx = 0;
    let mut code = 1u8;
    for (i, &byte) in data.iter().enumerate() {
        if byte == 0 {
            out[code_idx] = code;
            code_idx = out.len();
            out.push(0);
            code = 1;
        } else {
            out.push(byte);
            code += 1;
            if code == 0xFF {
                out[code_idx] = code;
                code = 1;
                if i + 1 == data.len() {
                    return out;
                }
                code_idx = out.len();
                out.push(0);
            }
        }
    }
    out[code_idx] = code;
    out
}

fn encode_vec(data: &[u8]) -> Vec<u8> {
    let mut buf = vec![0xAA; max_encoding_length(data.len())];
    let len = encode_from_slice(data, &mut buf).unwrap();
    buf.truncate(len);
    buf
}

fn assert_round_trip(data: &[u8]) {
    let encoded = encode_vec(data);
    assert_eq!(encoded, reference_encode(data), "payload {data:?}");
    assert!(encoded.len() <= max_encoding_length(data.len()));
    assert!(!encoded.contains(&0));

    let mut decoded = vec![0x55; data.len()];
    assert_eq!(
        decode_to_slice(&encoded, &mut decoded),
        Ok(DecodedFrame {
            len: data.len(),
            consumed: encoded.len(),
        })
    );
    assert_eq!(decoded, data);

    let mut framed = vec![0xAA; encoded.len() + 2];
    assert_eq!(
        encode_from_slice_including_sentinels(data, &mut framed),
        Ok(encoded.len() + 2)
    );
    assert_eq!(framed[0], 0);
    assert_eq!(&framed[1..=encoded.len()], encoded.as_slice());
    assert_eq!(framed[encoded.len() + 1], 0);
}

/// Deterministic xorshift generator so failures are reproducible.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn payload(&mut self, len: usize, zero_one_in: u64) -> Vec<u8> {
        (0..len)
            .map(|_| {
                let value = self.next();
                if value.is_multiple_of(zero_one_in) {
                    0
                } else {
                    (value >> 8) as u8 | 1
                }
            })
            .collect()
    }
}

fn nonzero(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 255) as u8 + 1).collect()
}

#[test]
fn empty_payload() {
    assert_eq!(encode_vec(&[]), [1]);
    let mut framed = [0xAA; 3];
    assert_eq!(
        encode_from_slice_including_sentinels(&[], &mut framed),
        Ok(3)
    );
    assert_eq!(framed, [0, 1, 0]);

    let mut dest = [0x55; 2];
    for (source, consumed) in [(&[1][..], 1), (&[1, 0], 2), (&[0, 1, 0], 3)] {
        assert_eq!(
            decode_to_slice(source, &mut dest),
            Ok(DecodedFrame { len: 0, consumed })
        );
        assert_eq!(dest, [0x55; 2]);
    }
    assert_eq!(
        decode_to_slice(&[1, 0], &mut []),
        Ok(DecodedFrame {
            len: 0,
            consumed: 2
        })
    );
}

#[test]
fn embedded_zeros() {
    let cases: [(&[u8], &[u8]); 6] = [
        (&[0], &[1, 1]),
        (&[0, 0], &[1, 1, 1]),
        (&[7, 0, 8], &[2, 7, 2, 8]),
        (&[0, 7], &[1, 2, 7]),
        (&[7, 0], &[2, 7, 1]),
        (&[7, 0, 0, 8], &[2, 7, 1, 2, 8]),
    ];
    for (payload, expected) in cases {
        assert_eq!(encode_vec(payload), expected, "payload {payload:?}");
        assert_round_trip(payload);
    }
}

#[test]
fn block_boundaries() {
    for len in [1, 253, 254, 255, 256, 507, 508, 509, 762, 1000] {
        let data = nonzero(len);
        assert_round_trip(&data);

        let mut with_zero = data.clone();
        with_zero.push(0);
        assert_round_trip(&with_zero);

        let mut leading_zero = vec![0];
        leading_zero.extend_from_slice(&data);
        assert_round_trip(&leading_zero);
    }

    let full = encode_vec(&nonzero(254));
    assert_eq!(full.len(), 255);
    assert_eq!(full[0], 0xFF);

    let over = encode_vec(&nonzero(255));
    assert_eq!(over.len(), 257);
    assert_eq!((over[0], over[255]), (0xFF, 2));

    let full_then_zero = encode_vec(&[nonzero(254), vec![0]].concat());
    assert_eq!(full_then_zero.len(), 257);
    assert_eq!(&full_then_zero[255..], &[1, 1]);
}

#[test]
fn randomized_round_trips_match_reference() {
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    for len in (0..600).chain([1016, 2048, 4099]) {
        for zero_one_in in [2, 16, 300, u64::MAX] {
            assert_round_trip(&rng.payload(len, zero_one_in));
        }
    }
}

#[test]
fn encode_reports_insufficient_capacity() {
    for len in 0..=600 {
        let data = nonzero(len);
        let required = max_encoding_length(len);
        let mut exact = vec![0; required];
        assert_eq!(encode_from_slice(&data, &mut exact), Ok(required));

        let mut short = vec![0; required - 1];
        assert_eq!(
            encode_from_slice(&data, &mut short),
            Err(SeekableError::OutOfBounds),
            "length {len}"
        );

        let mut framed_short = vec![0; required + 1];
        assert_eq!(
            encode_from_slice_including_sentinels(&data, &mut framed_short),
            Err(SeekableError::OutOfBounds),
            "length {len}"
        );
    }
    assert_eq!(
        encode_from_slice(&[], &mut []),
        Err(SeekableError::OutOfBounds)
    );
    assert_eq!(
        encode_from_slice_including_sentinels(&[], &mut []),
        Err(SeekableError::OutOfBounds)
    );
}

#[test]
fn decode_skips_padding_and_stops_at_delimiter() {
    let source = [0, 0, 2, 9, 0, 2, 7, 0];
    let mut dest = [0x55; 3];
    let frame = decode_to_slice(&source, &mut dest).unwrap();
    assert_eq!(
        frame,
        DecodedFrame {
            len: 1,
            consumed: 5
        }
    );
    assert_eq!(dest, [9, 0x55, 0x55]);

    let frame = decode_to_slice(&source[frame.consumed..], &mut dest).unwrap();
    assert_eq!(
        frame,
        DecodedFrame {
            len: 1,
            consumed: 3
        }
    );
    assert_eq!(dest[0], 7);
}

#[test]
fn decode_rejects_malformed_frames() {
    let source = [3, 1, 0, 2, 9, 0];
    let mut dest = [0x55; 4];
    let progress = DecodeProgress {
        consumed: 3,
        written: 1,
        frame_len: None,
    };
    assert_eq!(
        decode_to_slice(&source, &mut dest),
        Err(DecodeError::InvalidFrame(progress))
    );
    let next = &source[progress.consumed as usize..];
    assert_eq!(
        decode_to_slice(next, &mut dest),
        Ok(DecodedFrame {
            len: 1,
            consumed: 3
        })
    );
    assert_eq!(dest[0], 9);

    assert_eq!(
        decode_to_slice(&[0, 0, 3, 0], &mut dest),
        Err(DecodeError::InvalidFrame(DecodeProgress {
            consumed: 4,
            written: 0,
            frame_len: None,
        }))
    );
}

#[test]
fn decode_rejects_truncated_and_empty_input() {
    let mut dest = [0; 300];
    assert_eq!(
        decode_to_slice(&[3, 1], &mut dest),
        Err(DecodeError::UnexpectedSourceEof)
    );
    let truncated_full_block = &encode_vec(&nonzero(254))[..200];
    assert_eq!(
        decode_to_slice(truncated_full_block, &mut dest),
        Err(DecodeError::UnexpectedSourceEof)
    );
    assert_eq!(
        decode_to_slice(&[], &mut dest),
        Err(DecodeError::EmptyFrame)
    );
    assert_eq!(
        decode_to_slice(&[0, 0], &mut dest),
        Err(DecodeError::EmptyFrame)
    );

    // Truncation at a block boundary is structurally indistinguishable from
    // a complete undelimited frame, as documented.
    let frame = decode_to_slice(&[2, 9], &mut dest).unwrap();
    assert_eq!(
        frame,
        DecodedFrame {
            len: 1,
            consumed: 2
        }
    );
}

#[test]
fn decode_reports_insufficient_capacity() {
    let mut dest = [0x55; 2];
    assert_eq!(
        decode_to_slice(&[2, 7, 2, 8, 0], &mut dest),
        Err(DecodeError::Destination(SeekableError::OutOfBounds))
    );
    assert_eq!(dest, [7, 0]);

    for len in [254, 255, 508] {
        let data = nonzero(len);
        let encoded = encode_vec(&data);
        let mut exact = vec![0; len];
        assert_eq!(decode_to_slice(&encoded, &mut exact).unwrap().len, len);
        let mut short = vec![0; len - 1];
        assert_eq!(
            decode_to_slice(&encoded, &mut short),
            Err(DecodeError::Destination(SeekableError::OutOfBounds))
        );
    }
}

#[cfg(feature = "embedded-io")]
#[test]
fn matches_embedded_backend() {
    let mut rng = XorShift(7);
    for len in [0, 1, 253, 254, 255, 509, 1200] {
        let data = rng.payload(len, 20);
        let mut async_buf = vec![0; max_encoding_length(len)];
        let async_len = {
            let mut writer = &mut async_buf[..];
            futures::executor::block_on(crate::embedded::encode_from_slice_async(
                &data,
                &mut writer,
            ))
            .unwrap() as usize
        };
        assert_eq!(&async_buf[..async_len], encode_vec(&data).as_slice());
    }
}

#[cfg(feature = "tokio")]
#[test]
fn matches_tokio_backend() {
    let mut rng = XorShift(11);
    for len in [0, 1, 253, 254, 255, 509, 1200] {
        let data = rng.payload(len, 20);
        let mut async_buf = Vec::new();
        futures::executor::block_on(crate::tokio::encode_from_slice_async(&data, &mut async_buf))
            .unwrap();
        assert_eq!(async_buf, encode_vec(&data));
    }
}
