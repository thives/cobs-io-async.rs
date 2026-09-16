use crate::error::*;

#[derive(Clone, Debug)]
pub(crate) struct EncoderState {
    code_idx: u64,
    code: u8,
}

pub(crate) enum PushResult {
    AddSingle(u8),
    ModifyFromStartAndSkip((u64, u8)),
    ModifyFromStartAndPushAndSkip((u64, u8, u8)),
}

impl Default for EncoderState {
    fn default() -> Self {
        Self {
            code_idx: 0,
            code: 1,
        }
    }
}

impl EncoderState {
    pub(crate) fn push(&mut self, data: u8) -> Option<PushResult> {
        let result = if data == 0 {
            PushResult::ModifyFromStartAndSkip((self.code_idx, self.code))
        } else {
            self.code += 1;
            if self.code == u8::MAX {
                PushResult::ModifyFromStartAndPushAndSkip((self.code_idx, self.code, data))
            } else {
                return Some(PushResult::AddSingle(data));
            }
        };
        self.code_idx = self.code_idx.checked_add(u64::from(self.code))?;
        self.code = 1;
        Some(result)
    }

    pub(crate) fn finalize(&self) -> (u64, u8) {
        (self.code_idx, self.code)
    }
}

impl core::fmt::Display for EncoderState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "EncoderState {{ code_idx: {}, code: {} }}",
            self.code_idx, self.code
        )
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for EncoderState {
    fn format(&self, f: defmt::Formatter<'_>) {
        defmt::write!(
            f,
            "EncoderState {{ code_idx: {}, code: {} }}",
            self.code_idx,
            self.code
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub(crate) enum Phase {
    New,
    Ready,
    Poisoned,
    Finished,
}

#[derive(Debug)]
pub(crate) struct EncoderCore {
    pub(crate) dest_idx: u64,
    pub(crate) start_idx: u64,
    pub(crate) state: EncoderState,
    pub(crate) needs_code_placeholder: bool,
    pub(crate) phase: Phase,
    pub(crate) start_known: bool,
}

impl EncoderCore {
    pub(crate) fn new() -> Self {
        Self {
            dest_idx: 0,
            start_idx: 0,
            state: EncoderState::default(),
            needs_code_placeholder: false,
            phase: Phase::New,
            start_known: false,
        }
    }

    pub(crate) fn begin_operation<S, D>(&mut self) -> Result<bool, EncodeError<S, D>> {
        let initialize = match self.phase {
            Phase::New => true,
            Phase::Ready => false,
            Phase::Poisoned => return Err(EncodeError::Poisoned),
            Phase::Finished => return Err(EncodeError::AlreadyFinalized),
        };
        self.phase = Phase::Poisoned;
        Ok(initialize)
    }

    pub(crate) fn complete_reset(&mut self, next_start: u64) {
        *self = Self {
            start_idx: next_start,
            start_known: true,
            ..Self::new()
        };
    }
}

impl core::fmt::Display for EncoderCore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{{ dest_idx: {}, start_idx: {}, state: {:?}, needs_code_placeholder: {}, phase: {:?}, start_known: {} }}",
            self.dest_idx,
            self.start_idx,
            self.state,
            self.needs_code_placeholder,
            self.phase,
            self.start_known
        )
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for EncoderCore {
    fn format(&self, f: defmt::Formatter<'_>) {
        defmt::write!(
            f,
            "{{ dest_idx: {}, start_idx: {}, state: {:?}, needs_code_placeholder: {}, phase: {:?}, start_known: {} }}",
            self.dest_idx,
            self.start_idx,
            self.state,
            self.needs_code_placeholder,
            self.phase,
            self.start_known
        )
    }
}

pub(crate) struct Block {
    pub(crate) payload_len: u64,
    pub(crate) following: Following,
}

pub(crate) enum Following {
    /// The input ended immediately after this block.
    Eof,
    /// A sentinel follows this block.
    Sentinel,
    /// This was a full 254-byte block and at least one more input byte exists.
    MoreInput,
}

impl Block {
    pub(crate) fn code(&self) -> u8 {
        if self.payload_len == 254 {
            0xFF
        } else {
            self.payload_len as u8 + 1
        }
    }
}

#[derive(Debug)]
pub(crate) struct InputSeekable<'a> {
    buf: &'a [u8],
    idx: usize,
}

impl<'a> InputSeekable<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, idx: 0 }
    }
}

#[cfg(feature = "embedded-io")]
impl embedded_io_async::ErrorType for InputSeekable<'_> {
    type Error = SeekableError;
}

#[cfg(feature = "embedded-io")]
impl embedded_io_async::Read for InputSeekable<'_> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, SeekableError> {
        let len = buf.len().min(self.buf.len() - self.idx);
        buf[..len].copy_from_slice(&self.buf[self.idx..(self.idx + len)]);
        self.idx += len;
        Ok(len)
    }
}

#[cfg(feature = "tokio")]
impl ::tokio::io::AsyncRead for InputSeekable<'_> {
    fn poll_read(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
        buf: &mut ::tokio::io::ReadBuf<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let len = buf.remaining().min(this.buf.len() - this.idx);
        buf.put_slice(&this.buf[this.idx..this.idx + len]);
        this.idx += len;
        core::task::Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "embedded-io")]
impl embedded_io_async::Seek for InputSeekable<'_> {
    async fn seek(&mut self, pos: embedded_io_async::SeekFrom) -> Result<u64, SeekableError> {
        match pos {
            embedded_io_async::SeekFrom::Start(offset) => {
                if offset > self.buf.len() as u64 {
                    return Err(SeekableError::OutOfBounds);
                }
                self.idx = offset as usize;
            }
            embedded_io_async::SeekFrom::End(offset) => {
                let new_idx = self.buf.len() as i64 + offset;
                if new_idx > self.buf.len() as i64 {
                    return Err(SeekableError::OutOfBounds);
                }
                self.idx = new_idx as usize;
            }
            embedded_io_async::SeekFrom::Current(offset) => {
                let new_idx = self.idx as i64 + offset;
                if new_idx > self.buf.len() as i64 {
                    return Err(SeekableError::OutOfBounds);
                }
                self.idx = new_idx as usize;
            }
        }
        Ok(self.idx as u64)
    }
}

#[cfg(feature = "tokio")]
impl ::tokio::io::AsyncSeek for InputSeekable<'_> {
    fn start_seek(self: core::pin::Pin<&mut Self>, pos: std::io::SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();
        match pos {
            std::io::SeekFrom::Start(offset) => {
                if offset > this.buf.len() as u64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        SeekableError::OutOfBounds,
                    ));
                }
                this.idx = offset as usize;
            }
            std::io::SeekFrom::End(offset) => {
                let new_idx = this.buf.len() as i64 + offset;
                if new_idx > this.buf.len() as i64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        SeekableError::OutOfBounds,
                    ));
                }
                this.idx = new_idx as usize;
            }
            std::io::SeekFrom::Current(offset) => {
                let new_idx = this.idx as i64 + offset;
                if new_idx > this.buf.len() as i64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        SeekableError::OutOfBounds,
                    ));
                }
                this.idx = new_idx as usize;
            }
        }
        Ok(())
    }
    fn poll_complete(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<u64>> {
        core::task::Poll::Ready(Ok(self.idx as u64))
    }
}

macro_rules! define_encoder {
    (
        $D:ident: [$($writer_bounds:tt)+],
        $S:ident: [$($reader_bounds:tt)+],
        source_seek: $source_seek:ident,
        dest_nonseek: [$($writer_nonseek_bounds:tt)+],
        errors: [$source_error:ty, $dest_error:ty],
        slice_source_error: $slice_source_error:ty,
        seek_from: $seek_from:path $(,)?
    ) => {
        use $crate::codec::encode::{EncoderCore, Phase, PushResult, Following, Block, InputSeekable};
        use $crate::EncodeError;
        use ::core::convert::Infallible;
        use core::marker::PhantomData;

        /// Incrementally encodes one undelimited COBS body into a seekable writer.
        ///
        /// Owns the destination `D`; pass a mutable reference to borrow an existing
        /// writer. Encoder state requires no heap allocation, although the supplied
        /// writer may allocate.
        ///
        /// Encoding operations require `embedded_io_async::Write + Seek` for the
        /// embedded backend, or `tokio::io::AsyncWrite + AsyncSeek + Unpin` for Tokio.
        ///
        /// # Framing
        ///
        /// Successive pushes concatenate payload bytes into one body without adding
        /// frame delimiters. Payload zeros are encoded as data.
        ///
        /// Call [`finalize_async`](Self::finalize_async) explicitly to complete the
        /// body. An empty payload produces `[1]`. A terminal full block of 254 nonzero
        /// payload bytes does not receive a redundant trailing code byte.
        ///
        /// To encode a complete slice without requiring destination seeking, use
        /// [`encode_from_slice_async`]. For a complete encoding surrounded by zero
        /// delimiters, use [`encode_from_slice_including_sentinels_async`].
        ///
        /// # Destination behavior
        ///
        /// The initial frame position is captured lazily when an operation first
        /// needs it. Encoding reserves and backpatches code bytes, so intermediate
        /// output is not necessarily a valid completed COBS body.
        ///
        /// Writes begin at the captured position and may overwrite existing data.
        /// No operation truncates or explicitly flushes the destination. Dropping
        /// the encoder does not finalize it.
        ///
        /// The destination must honor seeks for subsequent writes and permit
        /// overwriting previously written code bytes. Append-mode files are
        /// unsuitable even if they implement the required traits.
        ///
        /// Use a one-shot slice helper when the destination supports only
        /// sequential output.
        ///
        /// # Errors and recovery
        ///
        /// I/O failures, position overflow, and zero-progress writes leave an active
        /// encoding operation poisoned. Further pushes and finalization reject with
        /// [`Poisoned`](crate::EncodeError::Poisoned).
        ///
        /// [`reset_async`](Self::reset_async) abandons the current body and establishes
        /// a new output boundary. It does not repair the abandoned body, remove
        /// partial output, or undo source consumption.
        ///
        /// After successful finalization, further pushes reject with
        /// [`AlreadyFinalized`](crate::EncodeError::AlreadyFinalized) until reset
        /// succeeds. Repeated finalization succeeds without performing I/O.
        ///
        /// # Cancellation
        ///
        /// Dropping an unpolled future performs no I/O and changes no state.
        /// Cancelling a polled, unfinished encoding or reset operation leaves the
        /// encoder poisoned. Input consumption and output are not rolled back.
        ///
        /// # Formatting
        ///
        /// Implements `Debug` when `D: Debug`. Its output includes the destination
        /// and internal encoder state and is intended for diagnostics, not as a
        /// stable serialization format.
        #[derive(Debug)]
        pub struct CobsEncoderAsync<$D> {
            dest: $D,
            core: EncoderCore,
        }

        impl<$D> CobsEncoderAsync<$D> {
            /// Creates an encoder owning `dest`, without performing I/O.
            ///
            /// Pass a mutable reference to borrow an existing destination.
            ///
            /// Construction does not capture the destination's position, reserve a code
            /// byte, clear existing contents, or flush. The initial frame position is
            /// captured lazily by the first polled operation that successfully queries it.
            ///
            /// Moving the destination cursor before that capture changes where the
            /// initial frame begins.
            ///
            /// Construction does not require `D` to implement an I/O trait.
            pub fn new(dest: $D) -> Self {
                Self {
                    dest,
                    core: EncoderCore::new(),
                }
            }

            /// Returns a shared reference to the destination.
            ///
            /// Performs no I/O, finalization, or state change. The destination remains
            /// accessible while the encoder is unfinished, finalized, or poisoned.
            ///
            /// Output from an unfinished encoder may contain placeholder code bytes
            /// and need not yet be a valid completed COBS body.
            pub fn dest(&self) -> &$D {
                &self.dest
            }

            /// Returns mutable access to the destination without changing encoder state.
            ///
            /// External writes, truncation, repositioning, or replacement do not
            /// synchronize encoder bookkeeping or clear poisoning.
            ///
            /// Moving the cursor before the initial frame position is captured changes
            /// that initial position. Once established, encoding and reset use stored
            /// absolute offsets; moving the cursor does not relocate the frame,
            /// including after a successful reset.
            ///
            /// Preserve the destination contents and position semantics required by the
            /// encoder. Modifying or replacing previously encoded data can invalidate
            /// the resulting body without being detected.
            ///
            /// This accessor performs no I/O. It may be used to explicitly flush the
            /// destination through its own I/O interface.
            pub fn dest_mut(&mut self) -> &mut $D {
                &mut self.dest
            }

            /// Consumes the encoder and returns its destination without performing I/O.
            ///
            /// Does not finalize the body, add a delimiter, reposition the destination,
            /// or flush. May be called in any encoder state.
            ///
            /// Existing output is retained, but unfinished or poisoned encoding state
            /// is lost. Call [`finalize_async`](Self::finalize_async) first when the
            /// current body needs successful completion.
            pub fn into_inner(self) -> $D {
                self.dest
            }
        }

        impl<$D> CobsEncoderAsync<$D>
        where
            $D: $($writer_bounds)+,
        {
            async fn ensure_start<E>(
                &mut self,
            ) -> Result<(), EncodeError<E, $dest_error>> {
                if !self.core.start_known {
                    let position = self.dest
                        .stream_position()
                        .await
                        .map_err(EncodeError::Destination)?;
                    self.core.start_idx = position;
                    self.core.start_known = true;
                }
                Ok(())
            }

            async fn begin_push<E>(
                &mut self,
            ) -> Result<(), EncodeError<E, $dest_error>> {
                if self.core.begin_operation::<E, $dest_error>()? {
                    self.ensure_start::<E>().await?;
                    self.append::<E>(0).await?;
                }
                Ok(())
            }

            async fn write_at<E>(
                &mut self,
                idx: u64,
                byte: u8,
            ) -> Result<(), EncodeError<E, $dest_error>> {
                let target = self.core.start_idx
                    .checked_add(idx)
                    .ok_or(EncodeError::PositionOverflow)?;
                target
                    .checked_add(1)
                    .ok_or(EncodeError::PositionOverflow)?;
                self.dest
                    .seek(<$seek_from>::Start(target))
                    .await
                    .map_err(EncodeError::Destination)?;
                let written = self.dest
                    .write(&[byte])
                    .await
                    .map_err(EncodeError::Destination)?;
                if written == 0 {
                    return Err(EncodeError::WriteZero);
                }
                Ok(())
            }

            async fn append<E>(
                &mut self,
                byte: u8,
            ) -> Result<(), EncodeError<E, $dest_error>> {
                let next_idx = self.core.dest_idx
                    .checked_add(1)
                    .ok_or(EncodeError::PositionOverflow)?;
                self.write_at::<E>(self.core.dest_idx, byte).await?;
                self.core.dest_idx = next_idx;
                Ok(())
            }

            async fn push_byte<E>(
                &mut self,
                byte: u8,
            ) -> Result<(), EncodeError<E, $dest_error>> {
                if self.core.needs_code_placeholder {
                    self.append::<E>(0).await?;
                    self.core.needs_code_placeholder = false;
                }
                let action = self.core.state
                    .push(byte)
                    .ok_or(EncodeError::PositionOverflow)?;
                match action {
                    PushResult::AddSingle(byte) => {
                        self.append::<E>(byte).await?;
                    }
                    PushResult::ModifyFromStartAndSkip((idx, code)) => {
                        self.write_at::<E>(idx, code).await?;
                        self.append::<E>(0).await?;
                    }
                    PushResult::ModifyFromStartAndPushAndSkip(
                        (idx, code, byte),
                    ) => {
                        self.write_at::<E>(idx, code).await?;
                        self.append::<E>(byte).await?;
                        self.core.needs_code_placeholder = true;
                    }
                }
                Ok(())
            }

            /// Appends payload read from `source` to the current COBS body.
            ///
            /// Reads from the source's current position until a nonempty read returns
            /// `Ok(0)`. Input exhaustion ends this push, not the body. Subsequent pushes
            /// may continue with the same or a different source.
            ///
            /// The source needs only the backend's asynchronous read trait; Tokio
            /// additionally requires `Unpin`. Source seeking is not required.
            ///
            /// Payload zeros are encoded as data. No frame delimiter is added.
            ///
            /// # Initialization and completion
            ///
            /// The first push captures the initial destination position if necessary
            /// and reserves a code-byte placeholder, even if the source immediately
            /// returns EOF.
            ///
            /// A successful push leaves the body open. Call
            /// [`finalize_async`](Self::finalize_async) to complete it.
            ///
            /// # Errors
            ///
            /// - [`Poisoned`](crate::EncodeError::Poisoned): rejected before I/O because
            ///   an earlier operation left the encoder poisoned.
            /// - [`AlreadyFinalized`](crate::EncodeError::AlreadyFinalized): rejected
            ///   before I/O because the current body has already been finalized.
            /// - [`Source`](crate::EncodeError::Source): reading the source failed.
            /// - [`Destination`](crate::EncodeError::Destination): querying, seeking,
            ///   or writing the destination failed.
            /// - [`WriteZero`](crate::EncodeError::WriteZero): a nonempty destination
            ///   write returned zero.
            /// - [`PositionOverflow`](crate::EncodeError::PositionOverflow): a required
            ///   encoded position cannot be represented.
            ///
            /// Source, destination, zero-write, and position-overflow failures leave the
            /// encoder poisoned. Errors provide no progress count or reliable retry
            /// offset. Input may already have been consumed and output modified.
            ///
            /// # Cancellation
            ///
            /// Dropping an unpolled future has no effect. Cancelling a polled, unfinished
            /// push leaves the encoder poisoned and does not roll back I/O.
            ///
            /// See [`reset_async`](Self::reset_async) for recovery.
            pub async fn push_async<$S>(
                &mut self,
                source: &mut $S,
            ) -> Result<(), EncodeError<$source_error, $dest_error>>
            where
                $S: $($reader_bounds)+,
            {
                self.begin_push::<$source_error>().await?;
                let mut byte = [0u8; 1];
                loop {
                    let n = source
                        .read(&mut byte)
                        .await
                        .map_err(EncodeError::Source)?;
                    if n == 0 {
                        break;
                    }
                    self.push_byte::<$source_error>(byte[0]).await?;
                }
                self.core.phase = Phase::Ready;
                Ok(())
            }

            /// Appends every byte of `source` to the current COBS body.
            ///
            /// Successive pushes concatenate payload chunks without introducing frame
            /// boundaries. Payload zeros are encoded as data. Call
            /// [`finalize_async`](Self::finalize_async) to complete the body.
            ///
            /// An empty slice still checks the encoder state and initializes a new body
            /// if necessary. It does not finalize the body or add a delimiter.
            ///
            /// # Errors
            ///
            /// Uses the destination, state, zero-write, and position-overflow error
            /// behavior of [`push_async`](Self::push_async).
            ///
            /// Performs no source I/O and never returns a source error, despite
            /// [`SeekableError`](crate::SeekableError) appearing as the source-error
            /// parameter in the return type.
            ///
            /// A poisoned or finalized encoder rejects before I/O, including for an
            /// empty slice. Other operation failures leave the encoder poisoned.
            ///
            /// # Cancellation
            ///
            /// Dropping an unpolled future has no effect. Cancelling a polled, unfinished
            /// call leaves the encoder poisoned and may leave partial output.
            ///
            /// Neither failure nor cancellation provides the number of payload bytes
            /// processed. Replaying the slice is not a safe way to resume the body.
            /// Use [`reset_async`](Self::reset_async) to establish a new boundary.
            pub async fn push_slice_async(
                &mut self,
                source: &[u8],
            ) -> Result<
                (),
                EncodeError<$crate::SeekableError, $dest_error>,
            > {
                self.begin_push::<$crate::SeekableError>().await?;
                for &byte in source {
                    self.push_byte::<$crate::SeekableError>(byte).await?;
                }
                self.core.phase = Phase::Ready;
                Ok(())
            }

            /// Completes the current undelimited COBS body.
            ///
            /// Returns the total encoded body length across all pushes. The length
            /// excludes pre-existing destination contents and delimiters written by
            /// [`reset_async`](Self::reset_async).
            ///
            /// Finalizing without any payload produces `[1]`. A terminal full block
            /// of 254 nonzero payload bytes does not receive a redundant trailing
            /// code byte.
            ///
            /// On the first successful completion, positions the destination immediately
            /// after the encoded body. Does not write a frame delimiter, truncate,
            /// or explicitly flush.
            ///
            /// # Repeated calls
            ///
            /// After successful finalization, repeated calls return the cached length
            /// without performing I/O. In particular, they do not restore a destination
            /// cursor moved through [`dest_mut`](Self::dest_mut).
            ///
            /// Further pushes, including empty pushes, return
            /// [`AlreadyFinalized`](crate::EncodeError::AlreadyFinalized) until
            /// [`reset_async`](Self::reset_async) succeeds.
            ///
            /// # Errors
            ///
            /// Returns [`Poisoned`](crate::EncodeError::Poisoned) before I/O when the
            /// encoder cannot continue.
            ///
            /// Otherwise, destination failures, zero-progress writes, and unrepresentable
            /// positions return [`Destination`](crate::EncodeError::Destination),
            /// [`WriteZero`](crate::EncodeError::WriteZero), or
            /// [`PositionOverflow`](crate::EncodeError::PositionOverflow), respectively,
            /// and leave the encoder poisoned.
            ///
            /// Performs no source I/O; the source-error parameter is `Infallible`.
            /// Repeated successful finalization does not return `AlreadyFinalized`.
            ///
            /// # Cancellation
            ///
            /// Dropping an unpolled future has no effect. Cancelling an unfinished
            /// finalization operation leaves the encoder poisoned and may leave
            /// partially updated code bytes or a changed destination cursor.
            ///
            /// No rollback is performed.
            pub async fn finalize_async(
                &mut self,
            ) -> Result<u64, EncodeError<Infallible, $dest_error>> {
                if self.core.phase == Phase::Finished {
                    return Ok(self.core.dest_idx);
                }
                self.begin_push::<Infallible>().await?;
                if !self.core.needs_code_placeholder {
                    let (idx, code) = self.core.state.finalize();
                    self.write_at::<Infallible>(idx, code).await?;
                }
                let end = self.core.start_idx
                    .checked_add(self.core.dest_idx)
                    .ok_or(EncodeError::PositionOverflow)?;
                self.dest
                    .seek(<$seek_from>::Start(end))
                    .await
                    .map_err(EncodeError::Destination)?;
                self.core.phase = Phase::Finished;
                Ok(self.core.dest_idx)
            }

            /// Abandons the current body and establishes a new output frame boundary.
            ///
            /// Writes a zero delimiter at the stored frame start plus the encoded
            /// length acknowledged by successful append writes. This position is based
            /// on encoder bookkeeping, not the destination's current cursor or the
            /// total number of writes used to backpatch code bytes.
            ///
            /// On success, starts a fresh body immediately after that delimiter and
            /// clears poisoning. The new body's initial code-byte placeholder is
            /// reserved by a subsequent push or finalization.
            ///
            /// # Recovery behavior
            ///
            /// May be called on a new, active, finalized, or poisoned encoder.
            /// On a new encoder, captures the current destination position and writes
            /// the delimiter there.
            ///
            /// Does not finalize or repair the abandoned body, rewind its source,
            /// truncate leftover output, or explicitly flush. The abandoned output
            /// may be malformed or may resemble a valid shorter frame; applications
            /// must not assume reset provides integrity protection.
            ///
            /// Successful resets are not idempotent. Each additional reset writes
            /// another zero boundary.
            ///
            /// # Errors
            ///
            /// Destination failures, zero-progress writes, and unrepresentable positions
            /// return [`Destination`](crate::EncodeError::Destination),
            /// [`WriteZero`](crate::EncodeError::WriteZero), or
            /// [`PositionOverflow`](crate::EncodeError::PositionOverflow), respectively.
            ///
            /// Failure leaves the encoder poisoned. Reset may be attempted again if the
            /// destination remains usable. This method does not reject merely because
            /// the encoder is poisoned or finalized.
            ///
            /// Performs no source I/O; the source-error parameter is `Infallible`.
            ///
            /// # Cancellation
            ///
            /// Dropping an unpolled future has no effect. Cancelling after the reset
            /// operation has begun leaves the encoder poisoned and may leave destination
            /// changes. Recovery does not roll back acknowledged or unacknowledged
            /// effects of previous I/O.
            pub async fn reset_async(
                &mut self,
            ) -> Result<(), EncodeError<Infallible, $dest_error>> {
                self.core.phase = Phase::Poisoned;
                self.ensure_start::<Infallible>().await?;
                let next_start = self.core.start_idx
                    .checked_add(self.core.dest_idx)
                    .and_then(|boundary| boundary.checked_add(1))
                    .ok_or(EncodeError::PositionOverflow)?;
                self.write_at::<Infallible>(self.core.dest_idx, 0).await?;
                self.core.complete_reset(next_start);
                Ok(())
            }
        }

        async fn encode_async<$S, $D>(
            source: &mut $S,
            dest: &mut $D,
        ) -> Result<u64, EncodeError<$source_error, $dest_error>>
        where
            $S: $source_seek + $($reader_bounds)+,
            $D: $($writer_nonseek_bounds)+,
        {
            let mut written: u64 = 0;
            loop {
                let block = scan_block::<$S, $D>(source).await?;
                dest.write_all(&[block.code()])
                    .await
                    .map_err(EncodeError::Destination)?;
                written += 1;
                copy_payload(source, dest, block.payload_len).await?;
                written += block.payload_len;
                match block.following {
                    Following::Eof => break,
                    Following::Sentinel => {
                        consume_sentinel::<$S, $D>(source).await?;
                    }
                    Following::MoreInput => {}
                }
            }
            Ok(written)
        }

        async fn encode_including_sentinels_async<$S, $D>(
            source: &mut $S,
            dest: &mut $D,
        ) -> Result<u64, EncodeError<$source_error, $dest_error>>
        where
            $S: $source_seek + $($reader_bounds)+,
            $D: $($writer_nonseek_bounds)+,
        {
            let _ = PhantomData::<fn() -> $D>;
            dest.write_all(&[0]).await.map_err(EncodeError::Destination)?;
            let encoded_len = encode_async(source, dest).await?;
            dest.write_all(&[0]).await.map_err(EncodeError::Destination)?;
            Ok(encoded_len + 2)
        }

        /// Encodes the entire `source` slice as one complete, undelimited COBS body.
        ///
        /// Writes sequentially from the destination's current position. The embedded
        /// backend requires `embedded_io_async::Write`; Tokio requires
        /// `tokio::io::AsyncWrite + Unpin`. Destination seeking is not required.
        ///
        /// Payload zeros are encoded as data. Empty input produces `[1]`.
        /// A terminal full block of 254 nonzero payload bytes does not receive a
        /// redundant trailing code byte.
        ///
        /// # Return value
        ///
        /// Returns the encoded body length, excluding existing destination contents.
        /// No separate finalization is required.
        ///
        /// Each call completes one undelimited body, but concatenating the outputs
        /// does not preserve frame boundaries. Separate bodies with zero delimiters
        /// or another independently known boundary.
        ///
        /// Use [`CobsEncoderAsync`] to combine payload chunks into one body, or
        /// [`encode_from_slice_including_sentinels_async`] for separately delimited
        /// encodings.
        ///
        /// See [`max_encoding_length`](crate::max_encoding_length) for buffer sizing.
        /// This function does not seek the destination, truncate it, or explicitly
        /// flush it.
        ///
        /// # Errors
        ///
        /// Destination errors are wrapped in
        /// [`Destination`](crate::EncodeError::Destination).
        ///
        /// The internal immutable slice source does not produce source errors.
        /// Its declared error type is [`SeekableError`](crate::SeekableError) for
        /// the embedded backend and `std::io::Error` for Tokio.
        ///
        /// # Zero-progress writes
        ///
        /// The embedded backend uses `embedded_io_async::Write::write_all`, whose
        /// default implementation panics if a nonempty write returns `Ok(0)`.
        ///
        /// Tokio's `AsyncWriteExt::write_all` reports
        /// `std::io::ErrorKind::WriteZero`, wrapped as a destination error.
        /// This helper does not normalize that failure into
        /// [`EncodeError::WriteZero`](crate::EncodeError::WriteZero).
        ///
        /// # Cancellation
        ///
        /// Dropping an unpolled future performs no I/O. Failure or cancellation after
        /// polling can leave partial output; no rollback, progress count, or resumable
        /// encoder state is provided.
        ///
        /// Retrying starts the entire encoding again at the destination's then-current
        /// position, not at the point where the previous operation stopped.
        pub async fn encode_from_slice_async<$D>(
            source: &[u8],
            dest: &mut $D,
        ) -> Result<u64, EncodeError<$slice_source_error, $dest_error>>
        where
            $D: $($writer_nonseek_bounds)+,
        {
            let _ = PhantomData::<fn() -> $D>;
            let mut input_buffer = InputSeekable::new(source);
            encode_async(&mut input_buffer, dest).await
        }

        /// Encodes one complete payload with a leading and trailing zero delimiter.
        ///
        /// On success, writes `[0, encoded_body..., 0]`. Empty input produces
        /// `[0, 1, 0]`. Payload zeros remain data and do not split the payload into
        /// multiple frames.
        ///
        /// Writes sequentially from the destination's current position. The embedded
        /// backend requires `embedded_io_async::Write`; Tokio requires
        /// `tokio::io::AsyncWrite + Unpin`. Destination seeking is not required.
        ///
        /// # Return value and capacity
        ///
        /// Returns the encoded body length plus two for the delimiters, excluding
        /// pre-existing destination contents.
        ///
        /// Reserve [`max_encoding_length`](crate::max_encoding_length) plus two bytes
        /// when using a fixed-size buffer. Use checked arithmetic when calculating
        /// capacity from potentially large lengths.
        ///
        /// Each call produces a separate framed encoding. No separate finalization
        /// is required. The function does not seek the destination, truncate it,
        /// or explicitly flush it.
        ///
        /// # Errors and zero-progress writes
        ///
        /// Body and delimiter writes use the same `write_all` behavior documented
        /// on [`encode_from_slice_async`].
        ///
        /// Destination errors are wrapped in
        /// [`Destination`](crate::EncodeError::Destination).
        /// Embedded's default `write_all` panics on a nonempty zero-progress write;
        /// Tokio reports a destination error with `std::io::ErrorKind::WriteZero`.
        ///
        /// The internal immutable slice source does not produce source errors,
        /// although a source-error parameter remains part of the return type.
        ///
        /// # Cancellation
        ///
        /// Dropping an unpolled future performs no I/O. Failure or cancellation after
        /// polling may leave a delimiter or body partially written.
        ///
        /// No rollback, progress count, or resumable state is provided. Retrying
        /// starts another complete encoding at the destination's then-current
        /// position and does not repair the interrupted frame.
        pub async fn encode_from_slice_including_sentinels_async<$D>(
            source: &[u8],
            dest: &mut $D,
        ) -> Result<u64, EncodeError<$slice_source_error, $dest_error>>
        where
            $D: $($writer_nonseek_bounds)+,
        {
            let _ = PhantomData::<fn() -> $D>;
            let mut input_buffer = InputSeekable::new(source);
            encode_including_sentinels_async(&mut input_buffer, dest).await
        }

        async fn scan_block<$S, $D>(source: &mut $S) -> Result<Block, EncodeError<$source_error, $dest_error>>
        where
            $S: $source_seek + $($reader_bounds)+,
            $D: $($writer_nonseek_bounds)+,
        {
            let _ = PhantomData::<fn() -> $D>;
            let start = source
                .stream_position()
                .await
                .map_err(EncodeError::Source)?;
            let mut byte = [0u8; 1];
            let mut payload_len = 0;
            let following = loop {
                let n = source.read(&mut byte).await.map_err(EncodeError::Source)?;
                if n == 0 {
                    break Following::Eof;
                }
                if byte[0] == 0 {
                    break Following::Sentinel;
                }
                payload_len += 1;
                if payload_len == 254 {
                    let n = source.read(&mut byte).await.map_err(EncodeError::Source)?;
                    break if n == 0 {
                        Following::Eof
                    } else {
                        Following::MoreInput
                    };
                }
            };
            source
                .seek(SeekFrom::Start(start))
                .await
                .map_err(EncodeError::Source)?;
            Ok(Block {
                payload_len,
                following,
            })
        }

        async fn copy_payload<$S, $D>(
            source: &mut $S,
            dest: &mut $D,
            len: u64,
        ) -> Result<(), EncodeError<$source_error, $dest_error>>
        where
            $S: $($reader_bounds)+,
            $D: $($writer_nonseek_bounds)+,
        {
            let _ = PhantomData::<fn() -> $D>;
            const BUF_SIZE: usize = 32;
            let mut byte = [0u8; BUF_SIZE];
            let mut remaining = len;
            loop {
                if remaining == 0 {
                    break;
                }
                let n = source
                    .read(&mut byte[..(remaining.min(BUF_SIZE as u64) as usize)])
                    .await
                    .map_err(EncodeError::Source)?;
                if n == 0 {
                    return Err(EncodeError::SourceChanged);
                }
                dest.write_all(&byte[..n])
                    .await
                    .map_err(EncodeError::Destination)?;
                remaining -= n as u64;
            }
            Ok(())
        }

        async fn consume_sentinel<$S, $D>(source: &mut $S) -> Result<(), EncodeError<$source_error, $dest_error>>
        where
            $S: $($reader_bounds)+,
            $D: $($writer_nonseek_bounds)+,
        {
            let _ = PhantomData::<fn() -> $D>;
            let mut byte = [0u8; 1];
            let n = source.read(&mut byte).await.map_err(EncodeError::Source)?;
            if n == 0 {
                return Err(EncodeError::UnexpectedSourceEof);
            }
            if byte[0] != 0 {
                return Err(EncodeError::SourceChanged);
            }
            Ok(())
        }

    };
}

pub(crate) use define_encoder;
