[![Crates.io](https://img.shields.io/crates/v/cobs-io-async)](https://crates.io/crates/cobs-io-async)
[![docs.rs](https://img.shields.io/docsrs/cobs-io-async)](https://docs.rs/cobs-io-async)
[![ci](https://github.com/thives/cobs-io-async.rs/actions/workflows/ci.yml/badge.svg)](https://github.com/thives/cobs-io-async.rs/actions/workflows/ci.yml)

# cobs-io-async

> [!WARNING]
> This crate is in early development. The API is not yet stable and may change.

> [!CAUTION]
> This crate is not yet production-ready. It has not been widely tested and may contain bugs.

Asynchronous [Consistent Overhead Byte Stuffing (COBS)](https://en.wikipedia.org/wiki/Consistent_Overhead_Byte_Stuffing)
encoding and decoding for embedded and Tokio I/O.

COBS transforms a payload into an encoded body containing no zero bytes,
allowing zero to mark frame boundaries in a byte stream. This crate uses
zero as its fixed delimiter; payload zeros are encoded as data.

- Independent `embedded-io-async` and Tokio backends.
- `no_std` support through the embedded backend.
- One-shot helpers and stateful, incremental codecs.
- No heap allocation for normal codec processing.
- Explicit frame completion and recovery after interrupted operations.

The package name is `cobs-io-async`; its Rust import name is `cobs_io_async`.

[API documentation](https://docs.rs/cobs-io-async/latest/cobs_io_async/)

## Installation

`tokio` and `serde` features are enabled by default. Select the backend matching your I/O
types.

### Embedded I/O

```toml
[dependencies]
cobs-io-async = { version = "0.1", default-features = false, features = ["embedded-io"] }
```

Use `cobs_io_async::embedded` with the traits from `embedded_io_async`.
This backend supports `no_std` and does not require a particular executor.

### Tokio

```toml
[dependencies]
cobs-io-async = { version = "0.1", features = ["tokio"] }
```

Use `cobs_io_async::tokio` with Tokio's asynchronous I/O traits.
Enable any additional Tokio features required by your application in its
own Tokio dependency.

The crate does not create a runtime or spawn tasks. Runtime requirements
depend on the supplied I/O types.

Both backends may be enabled together. Their APIs remain in separate
modules.

## Quick start

This example encodes a payload with surrounding delimiters, then decodes it
into a fixed-size buffer.

It uses the embedded backend and `futures` as a host-side executor. To run it,
also add this under `[dependencies]`:

```toml
futures = "0.3"
```

```rust
use cobs_io_async::{
    embedded::{
        decode_to_slice_async,
        encode_from_slice_including_sentinels_async,
    },
    max_encoding_length,
};

fn main() {
    futures::executor::block_on(async {
        let payload = [7, 0, 8];
        let mut encoded = [0u8; max_encoding_length(3) + 2];

        let encoded_len = {
            let mut writer = &mut encoded[..];
            encode_from_slice_including_sentinels_async(&payload, &mut writer)
                .await
                .unwrap()
        };

        assert_eq!(
            &encoded[..encoded_len as usize],
            &[0, 2, 7, 2, 8, 0],
        );

        let mut reader = &encoded[..encoded_len as usize];
        let mut decoded = [0u8; 3];

        let decoded_len = decode_to_slice_async(&mut reader, &mut decoded)
            .await
            .unwrap();

        assert_eq!(decoded_len, payload.len() as u64);
        assert_eq!(decoded, payload);
        assert!(reader.is_empty());
    });
}
```

For this in-memory example, the Tokio backend can be used by selecting the
`tokio` feature and changing the import from `embedded` to `tokio`.
Slice I/O does not itself require a Tokio runtime.

`futures` is only the executor chosen for this example, not a requirement of
the codec.

## Choosing an API

Both backend modules expose the same entry-point names:

| API | Purpose |
|---|---|
| `encode_from_slice_async` | Encode one complete payload without delimiters. |
| `encode_from_slice_including_sentinels_async` | Encode one complete payload with leading and trailing zero delimiters. |
| `CobsEncoderAsync` | Combine successive payload chunks into one encoded body; finish with `finalize_async`. |
| `decode_to_slice_async` | Decode one frame into a caller-provided slice. |
| `CobsDecoderAsync` | Retain decoding state across successive input chunks. |

Incremental encoding requires a seekable destination because it backpatches
earlier code bytes. One-shot slice encoding writes sequentially and does not
require destination seeking. Decoding never requires seeking.

The crate root exposes shared error types, `DecodeProgress`,
`max_encoding_overhead`, and `max_encoding_length`, even when no backend is
enabled.

`embedded_io_async::Write` is implemented for the embedded backend's `CobsEncoderAsync` and `CobsDecoderAsync`.

See the module documentation for backend-specific bounds and error behavior:

- [Embedded backend](https://docs.rs/cobs-io-async/latest/cobs_io_async/embedded/)
- [Tokio backend](https://docs.rs/cobs-io-async/latest/cobs_io_async/tokio/)

## Framing and buffer sizes

An empty payload encodes as `[1]`, or `[0, 1, 0]` with surrounding delimiters.
Zeros outside an active frame are padding, so `[0]` is not a completed empty
frame.

Use `max_encoding_length(payload_len)` for an upper bound on an undelimited
body's size. Reserve two additional bytes for surrounding delimiters.
Use checked arithmetic when calculating sizes from potentially large lengths.

Decoder capacity is based on the decoded payload, not the encoded length.
Successful one-shot decoding leaves the unused destination tail unchanged.

### Input exhaustion is not frame completion

A stateful decoder push stops when input is exhausted or when a delimiter
completes or invalidates the active frame.

- Exhausting an input chunk does not finish the frame.
- `check_complete` checks structural completeness without finishing it.
- `finish_frame` declares an independently known undelimited boundary.
- `DecodeProgress::frame_len == Some(n)` reports a frame completed by a
  delimiter during that push, including `Some(0)` for an empty payload.

The one-shot `decode_to_slice_async` helper also accepts structurally complete
EOF after a frame has started. Structural completeness cannot detect
truncation exactly at a COBS block boundary. Use the stateful decoder when
the protocol requires explicit delimiter completion.

## Errors, cancellation, and recovery

Codec operations do not explicitly flush or truncate destinations. Errors
and cancellation may leave input consumed and output partially written;
operations are not transactional.

Dropping an unpolled future has no effect. Cancelling a polled, unfinished
stateful operation leaves the encoder or decoder poisoned.

- Encoder `reset_async` abandons the current body and establishes a new
  output boundary.
- Decoder `discard_frame_async` consumes input through the next zero
  delimiter and clears poisoning only on success.
- Recovery does not roll back output or resume the abandoned frame.

`DecodeError::InvalidFrame` is different from an I/O failure: the bad frame's
delimiter has already been consumed and framing reset. Another push can
process the next frame without discarding first.

A cancelled read may already have consumed the intended recovery delimiter.
Consult the recovery-method documentation before retrying.

Normal codec processing does not allocate, but user-provided I/O, executors,
and backend error construction may allocate.

## Features

| Feature | Effect |
|---|---|
| `embedded-io` | Enables the embedded backend; supports `no_std`. |
| `tokio` | Enables the Tokio backend and this crate's `std` feature. |
| `std` | Enables standard-library integration for the embedded I/O dependency when it is also enabled. Does not select a backend. |
| `serde` | Adds serialization and deserialization for progress and error types, subject to generic parameter bounds. |
| `defmt` | Adds compact diagnostic formatting for supported types. |

There is no separate `alloc` feature. Encoder and decoder state is not
serializable.

## Protocol limitations

COBS provides framing, not integrity or authenticity. A structurally valid
frame may still contain corrupted or truncated application data.

Use an appropriate checksum, authentication mechanism, or independently
known message length when your protocol requires it.

## Development

The manifest declares Rust 1.87 as the minimum supported version.

Project recipes use [just](https://github.com/casey/just):

```sh
just build
just test
just docs
```

`just test` requires [cargo-nextest](https://nexte.st/).
The documentation recipe requires a nightly Rust toolchain.

Library tests can also be run directly with Cargo:

```sh
cargo test --lib --no-default-features
cargo test --lib --no-default-features --features embedded-io
cargo test --lib --no-default-features --features tokio
cargo test --lib --all-features
```

## License

Licensed under either of:

- Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license
  ([LICENSE-MIT](LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
