#[cfg(feature = "embedded-io")]
use cobs_io_async::embedded::{
    CobsDecoderAsync, decode_to_slice_async, decode_to_slice_buffered_async,
    encode_from_slice_async,
};
#[cfg(all(feature = "tokio", not(feature = "embedded-io")))]
use cobs_io_async::tokio::{
    CobsDecoderAsync, decode_to_slice_async, decode_to_slice_buffered_async,
    encode_from_slice_async,
};
use cobs_io_async::{SeekableError, max_encoding_length, sync};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(feature = "embedded-io")]
use embedded_io_async::{BufRead, ErrorType, Read, Seek, SeekFrom, Write};
use futures::executor::block_on;
use rand::RngExt;
use std::hint::black_box;
#[cfg(feature = "tokio")]
use tokio::io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

const SIZES: [usize; 7] = [16, 256, 4096, 65536, 262144, 1048576, 4194304];

/// Window size offered per fill by [`CountingReader`], similar to a typical
/// `BufReader` capacity.
const READER_WINDOW: usize = 4096;

/// In-memory reader that offers at most [`READER_WINDOW`] bytes per call and
/// counts read and fill calls, to show how many I/O calls each decoder makes.
struct CountingReader<'a> {
    data: &'a [u8],
    calls: usize,
}

impl CountingReader<'_> {
    fn read_into(&mut self, buf: &mut [u8]) -> usize {
        self.calls += 1;
        let n = buf.len().min(READER_WINDOW).min(self.data.len());
        buf[..n].copy_from_slice(&self.data[..n]);
        self.data = &self.data[n..];
        n
    }

    fn window(&mut self) -> &[u8] {
        self.calls += 1;
        &self.data[..self.data.len().min(READER_WINDOW)]
    }
}

#[cfg(feature = "embedded-io")]
impl ErrorType for CountingReader<'_> {
    type Error = core::convert::Infallible;
}

#[cfg(feature = "embedded-io")]
impl Read for CountingReader<'_> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        Ok(self.read_into(buf))
    }
}

#[cfg(feature = "embedded-io")]
impl BufRead for CountingReader<'_> {
    async fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        Ok(self.window())
    }

    fn consume(&mut self, amt: usize) {
        self.data = &self.data[amt..];
    }
}

#[cfg(feature = "tokio")]
impl AsyncRead for CountingReader<'_> {
    fn poll_read(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let n = this.read_into(buf.initialize_unfilled());
        buf.advance(n);
        core::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl AsyncBufRead for CountingReader<'_> {
    fn poll_fill_buf(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<&[u8]>> {
        core::task::Poll::Ready(Ok(self.get_mut().window()))
    }

    fn consume(self: core::pin::Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        this.data = &this.data[amt..];
    }
}

struct SliceWriter<'a> {
    buffer: &'a mut [u8],
    position: usize,
}

#[cfg(feature = "embedded-io")]
impl ErrorType for SliceWriter<'_> {
    type Error = SeekableError;
}

#[cfg(feature = "embedded-io")]
impl Write for SliceWriter<'_> {
    async fn write(&mut self, bytes: &[u8]) -> Result<usize, Self::Error> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or(SeekableError::OutOfBounds)?;

        let destination = self
            .buffer
            .get_mut(self.position..end)
            .ok_or(SeekableError::OutOfBounds)?;

        destination.copy_from_slice(bytes);
        self.position = end;
        Ok(bytes.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(feature = "embedded-io")]
impl Seek for SliceWriter<'_> {
    async fn seek(&mut self, from: SeekFrom) -> Result<u64, Self::Error> {
        let position = match from {
            SeekFrom::Start(position) => i128::from(position),
            SeekFrom::End(offset) => self.buffer.len() as i128 + i128::from(offset),
            SeekFrom::Current(offset) => self.position as i128 + i128::from(offset),
        };

        if !(0..=self.buffer.len() as i128).contains(&position) {
            return Err(SeekableError::OutOfBounds);
        }

        self.position = position as usize;
        Ok(self.position as u64)
    }
}

#[cfg(feature = "tokio")]
impl AsyncWrite for SliceWriter<'_> {
    fn poll_write(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
        buf: &[u8],
    ) -> core::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let end = this.position.checked_add(buf.len()).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, SeekableError::OutOfBounds)
        })?;

        let destination = this.buffer.get_mut(this.position..end).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, SeekableError::OutOfBounds)
        })?;

        destination.copy_from_slice(buf);
        this.position = end;
        core::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        core::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        core::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl AsyncSeek for SliceWriter<'_> {
    fn start_seek(self: core::pin::Pin<&mut Self>, pos: std::io::SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        let position = match pos {
            std::io::SeekFrom::Start(position) => i128::from(position),
            std::io::SeekFrom::End(offset) => this.buffer.len() as i128 + i128::from(offset),
            std::io::SeekFrom::Current(offset) => this.position as i128 + i128::from(offset),
        };

        if !(0..=this.buffer.len() as i128).contains(&position) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                SeekableError::OutOfBounds,
            ));
        }

        this.position = position as usize;
        Ok(())
    }
    fn poll_complete(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<u64>> {
        core::task::Poll::Ready(Ok(self.position as u64))
    }
}

fn encode_into(source: &[u8], destination: &mut [u8]) -> usize {
    let mut writer = SliceWriter {
        buffer: destination,
        position: 0,
    };

    let written =
        block_on(encode_from_slice_async(source, &mut writer)).expect("benchmark encoding failed");

    usize::try_from(written).expect("encoded length does not fit usize")
}

fn decode_into(source: &[u8], destination: &mut [u8]) -> usize {
    let mut reader = source;

    let written = block_on(decode_to_slice_async(&mut reader, destination))
        .expect("benchmark decoding failed");

    usize::try_from(written).expect("decoded length does not fit usize")
}

fn decode_buffered_into(source: &[u8], destination: &mut [u8]) -> usize {
    let mut reader = source;

    let written = block_on(decode_to_slice_buffered_async(&mut reader, destination))
        .expect("benchmark buffered decoding failed");

    usize::try_from(written).expect("decoded length does not fit usize")
}

type DecodeFn = fn(&[u8], &mut [u8]) -> usize;

/// Decodes the undelimited body with a stateful decoder, finishing the frame
/// at EOF.
fn decode_push_into(source: &[u8], destination: &mut [u8], buffered: bool) -> usize {
    let mut reader = source;
    let mut decoder = CobsDecoderAsync::new(SliceWriter {
        buffer: destination,
        position: 0,
    });
    let progress = if buffered {
        block_on(decoder.push_buffered_async(&mut reader))
    } else {
        block_on(decoder.push_async(&mut reader))
    }
    .expect("benchmark push failed");
    assert_eq!(progress.consumed as usize, source.len());
    let written = decoder.finish_frame().expect("benchmark frame incomplete");

    usize::try_from(written).expect("decoded length does not fit usize")
}

fn decode_sync_into(source: &[u8], destination: &mut [u8]) -> usize {
    sync::decode_to_slice(source, destination)
        .expect("benchmark sync decoding failed")
        .len
}

/// Returns the read calls made by the unbuffered and buffered decoders when
/// the source offers at most [`READER_WINDOW`] bytes per call.
fn count_decode_calls(encoded: &[u8], destination: &mut [u8]) -> (usize, usize) {
    let mut reader = CountingReader {
        data: encoded,
        calls: 0,
    };
    block_on(decode_to_slice_async(&mut reader, destination)).expect("counting decode failed");
    let unbuffered = reader.calls;

    let mut reader = CountingReader {
        data: encoded,
        calls: 0,
    };
    block_on(decode_to_slice_buffered_async(&mut reader, destination))
        .expect("counting buffered decode failed");
    (unbuffered, reader.calls)
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_input_seekable");

    for size in SIZES {
        let data: Vec<u8> = rand::rng().random_iter().take(size).collect();
        let mut buffer = vec![0u8; max_encoding_length(size)];

        // Validate the workload before timing it.
        let encoded_len = encode_into(&data, &mut buffer);
        let mut decoded = vec![0u8; size];
        assert_eq!(decode_into(&buffer[..encoded_len], &mut decoded), size);
        assert_eq!(decoded, data);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let written =
                    encode_into(black_box(data.as_slice()), black_box(buffer.as_mut_slice()));

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

        let encoded_len = encode_into(&data, &mut encoded);
        encoded.truncate(encoded_len);

        let variants: [(&str, DecodeFn); 5] = [
            ("async_read", decode_into),
            ("async_buf_read", decode_buffered_into),
            ("sync", decode_sync_into),
            ("push_async", |source, destination| {
                decode_push_into(source, destination, false)
            }),
            ("push_buffered_async", |source, destination| {
                decode_push_into(source, destination, true)
            }),
        ];

        // Validate every variant before timing it.
        let mut output = vec![0u8; size];
        for (name, decode) in variants {
            output.fill(0);
            assert_eq!(decode(&encoded, &mut output), size, "{name}");
            assert_eq!(output, data, "{name}");
        }

        let (unbuffered_calls, buffered_calls) = count_decode_calls(&encoded, &mut output);
        assert!(buffered_calls <= unbuffered_calls);
        println!(
            "decode/{size}: {unbuffered_calls} read calls unbuffered, \
             {buffered_calls} fill calls buffered ({READER_WINDOW}-byte window)"
        );

        group.throughput(Throughput::Bytes(size as u64));
        for (name, decode) in variants {
            group.bench_with_input(BenchmarkId::new(name, size), &encoded, |b, encoded| {
                b.iter(|| {
                    let written = decode(
                        black_box(encoded.as_slice()),
                        black_box(output.as_mut_slice()),
                    );

                    black_box(&output[..written]);
                    black_box(written);
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
