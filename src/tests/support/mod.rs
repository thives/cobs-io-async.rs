#[cfg(any(feature = "embedded-io", feature = "tokio"))]
pub(super) mod encoder_suite;

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
mod writer;

#[cfg(feature = "embedded-io")]
pub(super) mod embedded_writer;

#[cfg(feature = "tokio")]
pub(super) mod tokio_writer;

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
pub(super) mod decoder_suite;

#[cfg(any(feature = "embedded-io", feature = "tokio"))]
pub(super) use writer::{Action, Event, WakeCounter};
