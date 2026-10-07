use cobs_io_async::{max_encoding_length, sync};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::RngExt;
use std::hint::black_box;

const SIZES: [usize; 7] = [16, 256, 4096, 65536, 262144, 1048576, 4194304];

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    for size in SIZES {
        let data: Vec<u8> = rand::rng().random_iter().take(size).collect();
        let mut buffer = vec![0u8; max_encoding_length(size)];

        // Validate the workload before timing it.
        let encoded_len = sync::encode_from_slice(&data, &mut buffer).expect("encoding failed");
        let mut decoded = vec![0u8; size];
        let frame =
            sync::decode_to_slice(&buffer[..encoded_len], &mut decoded).expect("decoding failed");
        assert_eq!(frame.len, size);
        assert_eq!(decoded, data);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let written = sync::encode_from_slice(
                    black_box(data.as_slice()),
                    black_box(buffer.as_mut_slice()),
                )
                .expect("encoding failed");

                black_box(&buffer[..written]);
                black_box(written);
            });
        });
    }

    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    for size in SIZES {
        let data: Vec<u8> = rand::rng().random_iter().take(size).collect();
        let mut encoded = vec![0u8; max_encoding_length(size)];
        let encoded_len = sync::encode_from_slice(&data, &mut encoded).expect("encoding failed");
        encoded.truncate(encoded_len);

        let mut output = vec![0u8; size];
        let frame = sync::decode_to_slice(&encoded, &mut output).expect("decoding failed");
        assert_eq!(frame.len, size);
        assert_eq!(output, data);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &encoded, |b, encoded| {
            b.iter(|| {
                let frame = sync::decode_to_slice(
                    black_box(encoded.as_slice()),
                    black_box(output.as_mut_slice()),
                )
                .expect("decoding failed");

                black_box(&output[..frame.len]);
                black_box(frame.len);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
