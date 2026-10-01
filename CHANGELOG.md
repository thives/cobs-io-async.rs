Change Log
=======

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

# [Unreleased]

### Added

- `sync` module with `encode_from_slice`, `encode_from_slice_including_sentinels`,
  and `decode_to_slice` for in-memory encoding and decoding. Available without
  any backend feature, including in `no_std` builds.
- `decode_to_slice_buffered_async` in the `embedded` and `tokio` backends,
  which batches reads from `BufRead` / `AsyncBufRead` sources while consuming
  input only through the frame delimiter.
- `CobsDecoderAsync::push_buffered_async` (and the `CobsDecoderSliceAsync`
  wrapper), which batches reads from buffered sources and writes decoded
  output in batches, consuming a frame's delimiter only after its output is
  acknowledged.

### Changed

- `DecodeProgress` and the shared error types are now available with
  `default-features = false`, as previously documented.

### Fixed

- Documentation now states that `tokio` and `serde` are enabled by default,
  and dependency examples reference the published `0.0.2` release instead of `0.1`.

# [v0.1.0] 2026-09-14

- Initial release of the crate.
