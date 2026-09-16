use core::convert::Infallible;

/// A decoding or delimiter-recovery failure.
///
/// Generic parameters preserve the source and destination error types.
/// Match their variants to inspect them: the current [`core::error::Error`]
/// implementation does not forward `source()`.
///
/// Errors do not roll back input consumption or partial output.
/// Only [`InvalidFrame`](Self::InvalidFrame) carries progress information.
///
/// Implementations such as `Clone`, `Copy`, serialization, and compact formatting
/// depend on the corresponding traits being implemented by both generic error parameters.
///
/// Error formatting is diagnostic output, not a stable serialization format.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DecodeError<SourceError, DestinationError> {
    /// Reading the source failed.
    ///
    /// Stateful decoding or recovery remains poisoned.
    Source(SourceError),

    /// Writing decoded output failed.
    ///
    /// The corresponding encoded byte may already have been consumed.
    /// Stateful decoding remains poisoned.
    Destination(DestinationError),

    /// EOF occurred before the current COBS block was complete, or before
    /// delimiter-based discard encountered a zero.
    ///
    /// Ordinary streaming pushes do not report this merely because their
    /// input chunk ended.
    UnexpectedSourceEof,

    /// One-shot decoding reached EOF without starting an encoded frame.
    ///
    /// Empty input and zero padding alone produce this error. A valid
    /// encoding of an empty payload, such as `[1]`, does not.
    EmptyFrame,

    /// A zero delimiter arrived before the current COBS block had enough data.
    ///
    /// Progress is local to the failing call, includes the delimiter, and has
    /// `frame_len: None`. Previously written partial output is not removed.
    ///
    /// The delimiter has already been consumed and frame state reset.
    /// Another push can process the next frame without discarding first.
    InvalidFrame(crate::DecodeProgress),

    /// A push was attempted after a failed or interrupted operation left
    /// the decoder unable to continue safely.
    ///
    /// See the selected backends' `CobsDecoderAsync::discard_frame_async` for recovery limitations.
    Poisoned,
}

impl<S: core::error::Error, D: core::error::Error> core::error::Error for DecodeError<S, D> {}

#[cfg(feature = "embedded-io")]
impl<S, D> embedded_io_async::Error for DecodeError<S, D>
where
    S: embedded_io_async::Error,
    D: embedded_io_async::Error,
{
    fn kind(&self) -> embedded_io_async::ErrorKind {
        match self {
            DecodeError::Source(e) => e.kind(),
            DecodeError::Destination(e) => e.kind(),
            _ => embedded_io_async::ErrorKind::Other,
        }
    }
}

impl<S, D> core::fmt::Display for DecodeError<S, D>
where
    S: core::error::Error,
    D: core::error::Error,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DecodeError::Source(e) => write!(f, "Source error: {}", e),
            DecodeError::Destination(e) => write!(f, "Destination error: {}", e),
            DecodeError::UnexpectedSourceEof => write!(f, "Unexpected source EOF"),
            DecodeError::EmptyFrame => write!(f, "Empty frame"),
            DecodeError::InvalidFrame(progress) => {
                write!(f, "Invalid frame after decoding {:?}", progress)
            }
            DecodeError::Poisoned => write!(f, "Decoder is poisoned due to previous error"),
        }
    }
}

#[cfg(feature = "defmt")]
impl<S, D> defmt::Format for DecodeError<S, D>
where
    D: defmt::Format,
    S: defmt::Format,
{
    fn format(&self, f: defmt::Formatter<'_>) {
        match self {
            DecodeError::Source(e) => defmt::write!(f, "Source error: {}", e),
            DecodeError::Destination(e) => defmt::write!(f, "Destination error: {}", e),
            DecodeError::UnexpectedSourceEof => defmt::write!(f, "Unexpected source EOF"),
            DecodeError::EmptyFrame => defmt::write!(f, "Empty frame"),
            DecodeError::InvalidFrame(progress) => {
                defmt::write!(f, "Invalid frame after decoding {:?}", progress)
            }
            DecodeError::Poisoned => defmt::write!(f, "Decoder is poisoned due to previous error"),
        }
    }
}

/// Failure to validate or explicitly finish an undelimited frame.
///
/// These errors do not change decoder state. They implement
/// [`core::error::Error`] without requiring `std`.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompletionError {
    /// The current COBS block still requires encoded data bytes.
    ///
    /// Contains the total decoded bytes acknowledged as written for the
    /// current frame across all preceding pushes.
    IncompleteFrame(u64),

    /// The decoder is poisoned by an earlier failed or interrupted operation.
    InvalidState,

    /// No frame is active.
    ///
    /// Returned by `finish_frame`, not by `check_complete`. This includes
    /// the state immediately after successful explicit or delimiter-based
    /// completion. It is distinct from an active empty-payload frame.
    NoFrame,
}

impl core::error::Error for CompletionError {}

impl core::fmt::Display for CompletionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CompletionError::IncompleteFrame(written) => {
                write!(f, "Incomplete frame after decoding {} bytes", written)
            }
            CompletionError::InvalidState => {
                write!(f, "Invalid state encountered during decoding")
            }
            CompletionError::NoFrame => {
                write!(f, "No encoded frame has started")
            }
        }
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for CompletionError {
    fn format(&self, f: defmt::Formatter<'_>) {
        match self {
            CompletionError::IncompleteFrame(written) => {
                defmt::write!(f, "Incomplete frame after decoding {} bytes", written)
            }
            CompletionError::InvalidState => {
                defmt::write!(f, "Invalid state encountered during decoding")
            }
            CompletionError::NoFrame => {
                defmt::write!(f, "No encoded frame has started")
            }
        }
    }
}

/// An encoding failure.
///
/// `SourceError` and `DestinationError` preserve the underlying I/O error
/// types. Match the corresponding variants to inspect them: the current
/// [`core::error::Error`] implementation does not forward `source()`.
///
/// An error does not imply that no input was consumed or output written.
/// Consult the failed operation's documentation for poisoning and recovery.
///
/// Implementations such as `Clone`, `Copy`, serialization, and compact formatting
/// depend on the corresponding traits being implemented by both generic error parameters.
///
/// Error formatting is diagnostic output, not a stable serialization format.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EncodeError<SourceError, DestinationError> {
    /// Reading or seeking the source failed.
    Source(SourceError),

    /// Querying, seeking, or writing the destination failed.
    Destination(DestinationError),

    /// Internal seekable-source encoding reached EOF while expecting a
    /// previously observed source zero.
    ///
    /// Not currently emitted by the exported encoding entry points.
    UnexpectedSourceEof,

    /// Internal seekable-source encoding observed input inconsistent with
    /// an earlier scan.
    ///
    /// Not currently emitted by the exported encoding entry points.
    /// This variant does not imply exhaustive source-mutation detection.
    SourceChanged,

    /// A streaming encoder's nonempty destination write returned `Ok(0)`.
    ///
    /// The slice encoding helpers do not currently normalize zero writes
    /// into this variant.
    WriteZero,

    /// The streaming encoder cannot continue after an earlier failed,
    /// panicked, or cancelled operation.
    ///
    /// A successful reset permits a new frame, but does not resume or repair
    /// the abandoned frame.
    Poisoned,

    /// Input was pushed after successful finalization.
    ///
    /// Reset before starting another frame. Repeated finalization itself
    /// succeeds and returns the previous length.
    AlreadyFinalized,

    /// A required encoded offset or destination position exceeds `u64::MAX`.
    PositionOverflow,
}

impl<S: core::error::Error, D: core::error::Error> core::error::Error for EncodeError<S, D> {}

#[cfg(feature = "embedded-io")]
impl<S, D> embedded_io_async::Error for EncodeError<S, D>
where
    S: embedded_io_async::Error,
    D: embedded_io_async::Error,
{
    fn kind(&self) -> embedded_io_async::ErrorKind {
        match self {
            EncodeError::Source(e) => e.kind(),
            EncodeError::Destination(e) => e.kind(),
            _ => embedded_io_async::ErrorKind::Other,
        }
    }
}

impl<S, D> core::fmt::Display for EncodeError<S, D>
where
    S: core::error::Error,
    D: core::error::Error,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            EncodeError::Source(e) => write!(f, "Source error: {}", e),
            EncodeError::Destination(e) => write!(f, "Destination error: {}", e),
            EncodeError::UnexpectedSourceEof => write!(f, "Unexpected source EOF"),
            EncodeError::SourceChanged => write!(f, "Source changed"),
            EncodeError::WriteZero => write!(f, "Write zero"),
            EncodeError::Poisoned => write!(
                f,
                "Encoder cannot continue after a failed or interrupted operation"
            ),
            EncodeError::AlreadyFinalized => write!(f, "Encoder is already finalized"),
            EncodeError::PositionOverflow => write!(f, "Encoded position exceeds u64::MAX"),
        }
    }
}

#[cfg(feature = "defmt")]
impl<S, D> defmt::Format for EncodeError<S, D>
where
    D: defmt::Format,
    S: defmt::Format,
{
    fn format(&self, f: defmt::Formatter<'_>) {
        match self {
            EncodeError::Source(e) => defmt::write!(f, "Source error: {}", e),
            EncodeError::Destination(e) => defmt::write!(f, "Destination error: {}", e),
            EncodeError::UnexpectedSourceEof => defmt::write!(f, "Unexpected source EOF"),
            EncodeError::SourceChanged => defmt::write!(f, "Source changed"),
            EncodeError::WriteZero => defmt::write!(f, "Write zero"),
            EncodeError::Poisoned => defmt::write!(
                f,
                "Encoder cannot continue after a failed or interrupted operation"
            ),
            EncodeError::AlreadyFinalized => defmt::write!(f, "Encoder is already finalized"),
            EncodeError::PositionOverflow => defmt::write!(f, "Encoded position exceeds u64::MAX"),
        }
    }
}

/// A bounds error involving a slice-backed buffer.
///
/// Embedded one-shot decoding returns this as its destination error when
/// the output slice cannot hold the next decoded byte. Tokio represents
/// the corresponding failure as a `std::io::Error` wrapping this value.
///
/// Some slice-push APIs retain this as their source-error parameter even
/// though they perform no source I/O and cannot emit a source error.
///
/// Implements [`core::error::Error`] without requiring `std`.
/// With the `embedded-io` feature, also implements
/// `embedded_io_async::Error`, reporting `ErrorKind::Other`.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SeekableError {
    /// An operation would access a position or write bytes outside the
    /// backing slice's supported bounds.
    OutOfBounds,
}

#[cfg(feature = "embedded-io")]
impl embedded_io_async::Error for SeekableError {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        embedded_io_async::ErrorKind::Other
    }
}

impl core::error::Error for SeekableError {}
impl core::fmt::Display for SeekableError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SeekableError::OutOfBounds => write!(f, "Out of bounds"),
        }
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for SeekableError {
    fn format(&self, f: defmt::Formatter<'_>) {
        match self {
            SeekableError::OutOfBounds => defmt::write!(f, "Out of bounds"),
        }
    }
}

impl<D> From<EncodeError<Infallible, D>> for EncodeError<SeekableError, D> {
    fn from(e: EncodeError<Infallible, D>) -> Self {
        match e {
            EncodeError::Source(_) => unreachable!(),
            EncodeError::Destination(d) => EncodeError::Destination(d),
            EncodeError::UnexpectedSourceEof => EncodeError::UnexpectedSourceEof,
            EncodeError::SourceChanged => EncodeError::SourceChanged,
            EncodeError::WriteZero => EncodeError::WriteZero,
            EncodeError::Poisoned => EncodeError::Poisoned,
            EncodeError::AlreadyFinalized => EncodeError::AlreadyFinalized,
            EncodeError::PositionOverflow => EncodeError::PositionOverflow,
        }
    }
}
