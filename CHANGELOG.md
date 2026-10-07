Change Log
=======

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

# [Unreleased]

### Added

- `Reliable`, a runtime-independent reliable byte-stream connection over COBS
  framing. It implements the new `Transport` trait using stop-and-wait
  acknowledgments, CRC-32 integrity checking, retransmission and receive
  backpressure, and drives both directions from every poll method.
- `Transport`, a poll-based byte-stream trait (`poll_read`, `poll_write`,
  `poll_flush`), and `Timer`, a monotonic clock with deadline wakeups. Users
  implement both for their platform.
- `Config` (session identifier, retransmission timeout, retry limit),
  `ConfigError` and `ConnectionError`.
- `Reliable::poll_progress` to maintain a connection while no read, write or
  flush is pending.
- `sync` module with `encode_from_slice`, `encode_from_slice_including_sentinels`,
  and `decode_to_slice` for in-memory encoding and decoding.

### Changed

- **Breaking:** the crate is now a transport layer rather than an asynchronous
  codec. The default features are empty and the crate is `no_std`.
- `DecodeError` no longer has a `Poisoned` variant, and
  `DecodeProgress` now describes only the progress carried by
  `DecodeError::InvalidFrame`.

### Removed

- **Breaking:** the `embedded` and `tokio` modules and the `embedded-io`,
  `tokio` and `std` features, together with their encoders, decoders and
  I/O trait implementations. Runtime adapters are expected to be implemented
  by users against `Transport` and `Timer`.
- `CompletionError`, `EncodeError`, `CodecError`, `DEFAULT_BUF_SIZE`, and the
  `embedded_io_async` error implementations.
- The `futures` dev-dependency.

# [v0.1.0] 2026-09-14

- Initial release of the crate.
