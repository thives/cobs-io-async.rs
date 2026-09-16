use crate::error::*;

/// Progress from a successful decoder push or an invalid-frame error.
///
/// [`consumed`](Self::consumed) and [`written`](Self::written) describe the
/// current call. [`frame_len`](Self::frame_len), when present, includes output
/// from earlier pushes belonging to the same completed frame.
///
/// Source and destination I/O errors do not include this progress, and
/// cancellation returns no progress value. These counts do not provide
/// rollback or a safe retry offset after an interrupted operation.
///
/// The default value contains zero counts and `frame_len: None`. It does not
/// indicate whether a frame is active: an empty push can return default
/// progress while retaining an incomplete or structurally complete frame.
///
/// The `serde` feature enables serialization and deserialization. The `defmt`
/// feature enables compact diagnostic formatting.
#[must_use = "Inspect consumed input and frame completion before processing the next chunk"]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct DecodeProgress {
    /// Number of encoded bytes processed during this call.
    ///
    /// Includes code bytes, encoded payload, ignored zero padding, and a delimiter
    /// completing or invalidating an active frame. Excludes bytes belonging to
    /// subsequent frames.
    ///
    /// For `push_slice_async`, successful results and
    /// [`InvalidFrame`](crate::DecodeError::InvalidFrame) errors identify the
    /// processed prefix. The remaining input starts at
    /// `source[consumed as usize..]`.
    ///
    /// For reader-based operations, the source has already advanced past the
    /// consumed bytes; do not skip them again.
    pub consumed: u64,

    /// Number of decoded bytes acknowledged by successful destination writes
    /// during this call.
    ///
    /// Includes reconstructed payload zeros. Excludes code bytes, delimiters,
    /// padding, output from earlier calls, and external writes through the
    /// decoder's `dest_mut` accessor.
    ///
    /// Acknowledged writes do not imply flushing. Output written before an
    /// invalid-frame error remains in the destination.
    pub written: u64,

    /// Cumulative decoded length of a frame completed by a delimiter during
    /// this call.
    ///
    /// Includes acknowledged output from earlier pushes belonging to that frame.
    /// `Some(0)` represents a successfully completed empty frame.
    ///
    /// `None` means that this call did not successfully complete a frame by
    /// delimiter. It does not distinguish idle state, input exhaustion, an
    /// incomplete frame, or invalid framing.
    ///
    /// Explicit completion through `finish_frame` returns its length separately
    /// and does not modify previously returned progress values.
    pub frame_len: Option<u64>,
}

#[derive(Clone, Debug)]
struct DecoderState {
    next_sentinel_idx: u8,
    next_byte_is_sentinel_or_sentinel_index: bool,
    current_block_is_full: bool,
}

enum PushResult {
    AddSingle,
    WriteSentinel,
    InvalidFrame,
    EndOfFrame,
    Skip,
}

impl DecoderState {
    fn new() -> Self {
        Self {
            next_sentinel_idx: 0,
            next_byte_is_sentinel_or_sentinel_index: true,
            current_block_is_full: false,
        }
    }
    #[inline]
    fn push(&mut self, byte: u8) -> PushResult {
        if byte == 0 {
            self.next_byte_is_sentinel_or_sentinel_index = true;
            if self.next_sentinel_idx > 1 {
                self.next_sentinel_idx = 0;
                return PushResult::InvalidFrame;
            }
            self.next_sentinel_idx = 0;
            return PushResult::EndOfFrame;
        }
        if self.next_byte_is_sentinel_or_sentinel_index {
            let ret = if self.next_sentinel_idx == 0 || self.current_block_is_full {
                PushResult::Skip
            } else {
                PushResult::WriteSentinel
            };
            self.current_block_is_full = byte == 0xFF;
            self.next_sentinel_idx = byte;
            self.next_byte_is_sentinel_or_sentinel_index = byte <= 1;
            ret
        } else {
            self.next_sentinel_idx -= 1;
            if self.next_sentinel_idx <= 1 {
                self.next_byte_is_sentinel_or_sentinel_index = true;
            }
            PushResult::AddSingle
        }
    }
}

impl core::fmt::Display for DecoderState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "DecoderState {{ next_sentinel_idx: {}, next_byte_is_sentinel_or_sentinel_index: {}, current_block_is_full: {} }}",
            self.next_sentinel_idx,
            self.next_byte_is_sentinel_or_sentinel_index,
            self.current_block_is_full
        )
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for DecoderState {
    fn format(&self, f: defmt::Formatter<'_>) {
        defmt::write!(
            f,
            "DecoderState {{ next_sentinel_idx: {}, next_byte_is_sentinel_or_sentinel_index: {}, current_block_is_full: {} }}",
            self.next_sentinel_idx,
            self.next_byte_is_sentinel_or_sentinel_index,
            self.current_block_is_full
        )
    }
}

pub(crate) enum DecodeAction {
    Skip,
    Write(u8),
    FrameComplete(u64),
    InvalidFrame,
}

#[derive(Debug)]
pub(crate) struct DecoderCore {
    state: DecoderState,
    written: u64,
    frame_started: bool,
    poisoned: bool,
}

impl DecoderCore {
    pub(crate) fn new() -> Self {
        Self {
            state: DecoderState::new(),
            written: 0,
            frame_started: false,
            poisoned: false,
        }
    }

    pub(crate) fn begin_push(&mut self) -> bool {
        if self.poisoned {
            return false;
        }
        self.poisoned = true;
        true
    }

    pub(crate) fn finish_push(&mut self) {
        self.poisoned = false;
    }

    #[inline]
    pub(crate) fn accept_byte(&mut self, byte: u8) -> DecodeAction {
        if !self.frame_started && byte == 0 {
            return DecodeAction::Skip;
        }
        self.frame_started = true;
        match self.state.push(byte) {
            PushResult::AddSingle => DecodeAction::Write(byte),
            PushResult::WriteSentinel => DecodeAction::Write(0),
            PushResult::Skip => DecodeAction::Skip,
            PushResult::InvalidFrame => {
                self.reset_frame();
                DecodeAction::InvalidFrame
            }
            PushResult::EndOfFrame => DecodeAction::FrameComplete(self.reset_frame()),
        }
    }

    #[inline]
    pub(crate) fn acknowledge_write(&mut self) {
        self.written += 1;
    }

    fn reset_frame(&mut self) -> u64 {
        self.state = DecoderState::new();
        self.frame_started = false;
        core::mem::take(&mut self.written)
    }

    pub(crate) fn check_complete(&self) -> Result<(), CompletionError> {
        if self.poisoned {
            return Err(CompletionError::InvalidState);
        }
        if self.state.next_sentinel_idx > 1 {
            return Err(CompletionError::IncompleteFrame(self.written));
        }
        Ok(())
    }

    pub(crate) fn finish_frame(&mut self) -> Result<u64, CompletionError> {
        self.check_complete()?;
        if !self.frame_started {
            return Err(CompletionError::NoFrame);
        }
        Ok(self.reset_frame())
    }

    pub(crate) fn begin_discard(&mut self) {
        self.poisoned = true;
    }

    pub(crate) fn finish_discard(&mut self) -> u64 {
        let written = self.reset_frame();
        self.poisoned = false;
        written
    }
}

impl core::fmt::Display for DecoderCore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{{ state: {:?}, frame written: {}, frame_started: {}, poisoned: {} }}",
            self.state, self.written, self.frame_started, self.poisoned
        )
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for DecoderCore {
    fn format(&self, f: defmt::Formatter<'_>) {
        defmt::write!(
            f,
            "{{ state: {:?}, frame written: {}, frame_started: {}, poisoned: {} }}",
            self.state,
            self.written,
            self.frame_started,
            self.poisoned
        )
    }
}

macro_rules! define_decoder {
    (
        $D:ident: [$($writer_bounds:tt)+],
        $S:ident: [$($reader_bounds:tt)+],
        errors: [$source_error:ty, $dest_error:ty],
    ) => {
        use $crate::codec::decode::{DecodeAction, DecoderCore};
        use $crate::{DecodeError, DecodeProgress};

        /// Incrementally decodes zero-delimited COBS frames into an asynchronous writer.
        ///
        /// Owns the destination `D`; pass a mutable reference to borrow an existing
        /// writer. Decoder state requires no heap allocation, although the supplied
        /// writer may allocate.
        ///
        /// Push methods require `embedded_io_async::Write` for the embedded backend,
        /// or `tokio::io::AsyncWrite + Unpin` for the Tokio backend. The destination
        /// does not need to support seeking.
        ///
        /// # Framing
        ///
        /// A nonzero byte starts a frame. Zeros while idle are ignored as padding.
        /// `[1, 0]` completes a valid empty frame.
        ///
        /// Successive pushes continue the current frame. Each push stops at input
        /// exhaustion or the first delimiter completing or invalidating an active
        /// frame. The delimiter is consumed; subsequent input is not requested by
        /// the decoder.
        ///
        /// Input exhaustion does not complete a frame. Use
        /// [`finish_frame`](Self::finish_frame) only when the application independently
        /// knows the boundary of an undelimited frame.
        /// [`check_complete`](Self::check_complete) checks structural completeness
        /// without establishing that boundary.
        ///
        /// A full COBS block contains 254 nonzero payload bytes and does not insert
        /// a zero before the next block. Shorter blocks reconstruct a zero only when
        /// another nonzero code byte follows, not at a delimiter or explicit finish.
        ///
        /// Completion resets framing and frame-length accounting without clearing,
        /// repositioning, truncating, or flushing the destination.
        ///
        /// # Errors and recovery
        ///
        /// Output is written before whole-frame validation. Errors can therefore
        /// leave partial decoded output in the destination.
        ///
        /// Source or destination failures poison the decoder. Further pushes reject
        /// with [`Poisoned`](crate::DecodeError::Poisoned) before performing I/O.
        /// [`discard_frame_async`](Self::discard_frame_async) can establish a new
        /// input boundary and clear poisoning.
        ///
        /// The rejected frame's output remains in the destination. The attached
        /// progress describes only the failing call: `written` excludes earlier
        /// pushes, and `frame_len` is `None`.
        ///
        /// Track earlier pushes separately if the application needs to identify
        /// all output belonging to a rejected frame. Discarding afterward cannot
        /// recover its cumulative length and may skip the next frame.
        ///
        /// An [`InvalidFrame`](crate::DecodeError::InvalidFrame) error is different:
        /// the invalidating delimiter has already been consumed and framing reset.
        /// Another push may process the next frame immediately.
        ///
        /// # Cancellation
        ///
        /// Dropping an unpolled future performs no I/O and changes no decoder state.
        /// Cancelling a polled, unfinished push or discard leaves the decoder
        /// poisoned. Input consumption and output are not rolled back, even when
        /// individual I/O operations are cancellation-safe.
        ///
        /// # Formatting
        ///
        /// `Debug` is available when `D: Debug` and includes the destination.
        /// `Display` and, with the `defmt` feature, `defmt::Format` describe internal
        /// decoder state without requiring the destination to implement formatting.
        ///
        /// Formatting does not validate or complete a frame. Its representation is
        /// intended for diagnostics, not as a stable serialization format.
        #[derive(Debug)]
        pub struct CobsDecoderAsync<$D> {
            dest: $D,
            core: DecoderCore,
        }

        impl<$D> CobsDecoderAsync<$D> {
            /// Creates an idle decoder owning `dest`.
            ///
            /// Pass a mutable reference to borrow an existing destination.
            ///
            /// Performs no I/O and does not clear, reposition, or flush the destination.
            /// Framing and acknowledged-output accounting start empty, regardless of
            /// existing destination contents.
            ///
            /// Initially, [`check_complete`](Self::check_complete) returns `Ok(())` and
            /// [`finish_frame`](Self::finish_frame) returns
            /// [`NoFrame`](crate::CompletionError::NoFrame).
            ///
            /// Construction does not require `D` to implement an I/O trait.
            pub fn new(dest: $D) -> Self {
                Self {
                    dest,
                    core: DecoderCore::new(),
                }
            }

            /// Returns a shared reference to the destination.
            ///
            /// Performs no I/O, validation, or frame completion and does not change
            /// decoder state. The destination remains accessible while the decoder is
            /// incomplete or poisoned.
            pub fn dest(&self) -> &$D {
                &self.dest
            }

            /// Returns mutable access to the destination without changing decoder state.
            ///
            /// External writes, repositioning, truncation, or replacement do not adjust
            /// frame accounting or clear poisoning. Reported decoded lengths may therefore
            /// differ from the destination's current contents after such changes.
            ///
            /// This accessor performs no I/O or validation. It may be used to explicitly
            /// flush the destination through its own I/O interface.
            pub fn dest_mut(&mut self) -> &mut $D {
                &mut self.dest
            }

            /// Consumes the decoder and returns its destination without performing I/O.
            ///
            /// Does not validate or finish a frame, write additional output, or flush.
            /// May be called with an incomplete or poisoned decoder; its framing state
            /// and accounting are then lost, while existing output remains intact.
            ///
            /// Call [`finish_frame`](Self::finish_frame) first when an independently known
            /// undelimited frame boundary needs validation.
            pub fn into_inner(self) -> $D {
                self.dest
            }

            /// Checks whether the current COBS block is structurally complete.
            ///
            /// Returns `Ok(())` for an idle decoder or a complete block boundary.
            /// Performs no I/O and changes no state.
            ///
            /// Success does not finish a frame or prove that the intended message was
            /// not truncated at a block boundary. Further input may continue the same
            /// frame. Use [`finish_frame`](Self::finish_frame) to declare an independently
            /// known undelimited boundary.
            ///
            /// # Errors
            ///
            /// Returns [`InvalidState`](crate::CompletionError::InvalidState) if poisoned.
            /// Otherwise, returns [`IncompleteFrame`](crate::CompletionError::IncompleteFrame)
            /// when the current block requires more payload bytes. Its count is the
            /// cumulative acknowledged decoded output for the current frame.
            ///
            /// Never returns [`NoFrame`](crate::CompletionError::NoFrame).
            pub fn check_complete(&self) -> Result<(), $crate::CompletionError> {
                self.core.check_complete()
            }

            /// Declares an independently known end to the current undelimited frame.
            ///
            /// On success, returns the cumulative decoded bytes acknowledged across all
            /// pushes belonging to the frame, then resets framing and frame accounting.
            /// A frame consisting of `[1]` finishes successfully with length zero.
            ///
            /// Performs no I/O: it does not consume a delimiter, write an additional zero,
            /// reposition the destination, or flush. Existing output remains intact.
            ///
            /// Structural completeness does not prove that the message was not truncated
            /// at a COBS block boundary. The application must establish the boundary
            /// independently.
            ///
            /// Do not finish a frame again after a push has already completed it by
            /// delimiter. Unlike repeated encoder finalization, repeated decoder finish
            /// does not return a cached length.
            ///
            /// # Errors
            ///
            /// Returns, in precedence order:
            ///
            /// - [`InvalidState`](crate::CompletionError::InvalidState) if poisoned.
            /// - [`IncompleteFrame`](crate::CompletionError::IncompleteFrame) if the
            ///   current block requires more payload bytes. The attached count is
            ///   cumulative acknowledged output for the current frame.
            /// - [`NoFrame`](crate::CompletionError::NoFrame) if no frame is active.
            ///
            /// All errors leave state unchanged. An incomplete frame can be continued
            /// by supplying more input.
            pub fn finish_frame(&mut self) -> Result<u64, $crate::CompletionError>
            {
                self.core.finish_frame()
            }

            /// Discards encoded input through the next zero delimiter.
            ///
            /// Starts at the source's current position and stops immediately after the
            /// first zero, even if no frame is active. Discarded input is neither decoded
            /// nor validated.
            ///
            /// This operation is permitted while poisoned. On success, it resets framing
            /// and accounting and clears poisoning.
            ///
            /// Performs no destination I/O and requires no destination I/O trait.
            /// Does not rewind, truncate, or flush the destination, or undo partial output.
            ///
            /// # Return value
            ///
            /// Returns the cumulative decoded bytes previously acknowledged as written
            /// for the abandoned frame.
            ///
            /// This is not the number of encoded bytes discarded by this call, the
            /// hypothetical decoded size of that input, or the destination's total length.
            /// An idle decoder returns zero after finding a delimiter.
            ///
            /// Interrupted writes can have unacknowledged side effects, so the returned
            /// count may be smaller than the actual destination changes.
            ///
            /// # Recovery limitations
            ///
            /// Use this operation only when the next unread delimiter is the intended
            /// synchronization boundary. A cancelled read may already have consumed
            /// that delimiter, causing discard to skip through a later frame.
            ///
            /// An [`InvalidFrame`](crate::DecodeError::InvalidFrame) result already consumed
            /// the invalid frame's delimiter and reset framing. Do not discard again
            /// merely because that error occurred.
            ///
            /// After a failed or cancelled `push_slice_async`, the caller's slice has
            /// not been advanced and no consumed-prefix offset is available. This
            /// method cannot infer the correct unread suffix. Recovery must arrange
            /// that the next zero read is the intended synchronization delimiter.
            ///
            /// # Errors
            ///
            /// Returns [`Source`](crate::DecodeError::Source) for a source I/O failure or
            /// [`UnexpectedSourceEof`](crate::DecodeError::UnexpectedSourceEof) if input
            /// ends before a delimiter.
            ///
            /// Failure leaves the decoder poisoned and preserves its acknowledged
            /// frame-output count for a later recovery attempt. No destination error
            /// is possible; the destination-error parameter is `Infallible`.
            ///
            /// # Cancellation
            ///
            /// Dropping an unpolled future has no effect. Cancelling after polling leaves
            /// the decoder poisoned and does not roll back input consumption.
            /// Only successful recovery clears poisoning.
            pub async fn discard_frame_async<$S>(
                &mut self,
                source: &mut $S,
            ) -> Result<u64, DecodeError<$source_error, ::core::convert::Infallible>>
            where
                $S: $($reader_bounds)+,
            {
                self.core.begin_discard();
                let mut byte = [0u8; 1];
                loop {
                    let n = source
                        .read(&mut byte)
                        .await
                        .map_err(DecodeError::Source)?;
                    if n == 0 {
                        return Err(DecodeError::UnexpectedSourceEof);
                    }
                    if byte[0] == 0 {
                        return Ok(self.core.finish_discard());
                    }
                }
            }
        }

        impl<$D> CobsDecoderAsync<$D>
        where
            $D: $($writer_bounds)+,
        {
            /// Decodes input into the destination until input exhaustion or a frame boundary.
            ///
            /// Continues the current frame from the source's current position. The source
            /// needs only the backend's asynchronous read trait; Tokio additionally
            /// requires `Unpin`. Sources do not need to support seeking.
            ///
            /// A read returning `Ok(0)` ends this call successfully, even if the current
            /// COBS block is incomplete. It neither finishes nor resets the frame.
            ///
            /// Leading zero padding is consumed without producing output. A delimiter
            /// completing or invalidating an active frame is consumed, but no subsequent
            /// encoded byte is requested by the decoder.
            ///
            /// # Progress
            ///
            /// Returns [`DecodeProgress`](crate::DecodeProgress) containing this call's
            /// consumed and acknowledged-written counts. When a delimiter completes a
            /// frame, `frame_len` contains its cumulative decoded length across all pushes.
            /// `[1, 0]` reports `Some(0)`.
            ///
            /// Completion resets framing and accounting, not the destination position.
            /// No explicit flush is performed.
            ///
            /// # Errors
            ///
            /// - [`Poisoned`](crate::DecodeError::Poisoned): rejected before I/O because
            ///   an earlier operation left the decoder poisoned.
            /// - [`Source`](crate::DecodeError::Source) or
            ///   [`Destination`](crate::DecodeError::Destination): I/O failed and the
            ///   decoder remains poisoned.
            /// - [`InvalidFrame`](crate::DecodeError::InvalidFrame): a premature delimiter
            ///   invalidated the frame. Includes per-call progress; the delimiter has
            ///   already been consumed and framing reset.
            ///
            /// Input exhaustion does not produce `EmptyFrame` or `UnexpectedSourceEof`.
            /// Partial output is retained. I/O errors do not include progress or a
            /// reliable retry offset.
            ///
            /// # Zero-progress writes
            ///
            /// The embedded backend uses `embedded_io_async::Write::write_all`, whose
            /// default implementation panics if a nonempty write returns `Ok(0)`.
            /// If unwinding occurs, the decoder remains poisoned.
            ///
            /// Tokio's `AsyncWriteExt::write_all` instead reports
            /// `std::io::ErrorKind::WriteZero`, wrapped as a destination error.
            ///
            /// # Cancellation
            ///
            /// Dropping an unpolled future has no effect. Cancelling a polled, unfinished
            /// call leaves the decoder poisoned. The source and destination may already
            /// have changed, including unacknowledged effects of interrupted I/O.
            ///
            /// See [`discard_frame_async`](Self::discard_frame_async) for recovery limits.
            pub async fn push_async<$S>(
                &mut self,
                source: &mut $S,
            ) -> Result<DecodeProgress, DecodeError<$source_error, $dest_error>>
            where
                $S: $($reader_bounds)+,
            {
                if !self.core.begin_push() {
                    return Err(DecodeError::Poisoned);
                }
                let mut progress = DecodeProgress::default();
                let mut byte = [0u8; 1];
                loop {
                    let n = source
                        .read(&mut byte)
                        .await
                        .map_err(DecodeError::Source)?;
                    if n == 0 {
                        self.core.finish_push();
                        return Ok(progress);
                    }
                    if self
                        .process_byte::<$source_error>(byte[0], &mut progress)
                        .await?
                    {
                        return Ok(progress);
                    }
                }
            }

            /// Decodes `source` as a chunk of the current encoded stream.
            ///
            /// Directly processes the slice, stopping at its end or the first delimiter
            /// completing or invalidating an active frame. Leading zero padding is
            /// ignored but counted as consumed.
            ///
            /// An empty slice returns default progress when the decoder is not poisoned.
            /// It neither starts nor finishes a frame. Slice exhaustion does not finish
            /// even a structurally complete frame; use
            /// [`finish_frame`](Self::finish_frame) only for an independently known boundary.
            ///
            /// # Remaining input
            ///
            /// On success or [`InvalidFrame`](crate::DecodeError::InvalidFrame), retain
            /// `&source[progress.consumed as usize..]` for subsequent processing.
            /// The consumed prefix includes any delimiter that completed or invalidated
            /// the frame.
            ///
            /// After a failed or cancelled `push_slice_async`, the caller's slice has
            /// not been advanced and no consumed-prefix offset is available. This
            /// method cannot infer the correct unread suffix. Recovery must arrange
            /// that the next zero read is the intended synchronization delimiter.
            ///
            /// # Errors
            ///
            /// Uses the framing and destination-error behavior of
            /// [`push_async`](Self::push_async). A poisoned decoder rejects before I/O,
            /// including for an empty slice.
            ///
            /// Performs no source I/O and never returns a source error, despite
            /// [`SeekableError`](crate::SeekableError) appearing as the source-error
            /// parameter in the return type.
            ///
            /// # Cancellation and write behavior
            ///
            /// Uses the zero-progress-write and poisoning behavior documented on
            /// [`push_async`](Self::push_async).
            ///
            /// Destination failure or cancellation provides no consumed-prefix offset.
            /// Do not assume that none or all of the slice was processed, or that
            /// replaying the slice is safe. Partial destination output is retained.
            ///
            /// Dropping an unpolled future performs no I/O and changes no state.
            pub async fn push_slice_async(
                &mut self,
                source: &[u8],
            ) -> Result<DecodeProgress, DecodeError<$crate::SeekableError, $dest_error>> {
                if !self.core.begin_push() {
                    return Err(DecodeError::Poisoned);
                }
                let mut progress = DecodeProgress::default();
                for &byte in source {
                    if self
                        .process_byte::<$crate::SeekableError>(byte, &mut progress)
                        .await?
                    {
                        return Ok(progress);
                    }
                }
                self.core.finish_push();
                Ok(progress)
            }

            async fn process_byte<E>(
                &mut self,
                byte: u8,
                progress: &mut DecodeProgress,
            ) -> Result<bool, DecodeError<E, $dest_error>> {
                progress.consumed += 1;
                match self.core.accept_byte(byte) {
                    DecodeAction::Skip => {}
                    DecodeAction::Write(value) => {
                        self.dest
                            .write_all(&[value])
                            .await
                            .map_err(DecodeError::Destination)?;
                        self.core.acknowledge_write();
                        progress.written += 1;
                    }
                    DecodeAction::FrameComplete(len) => {
                        progress.frame_len = Some(len);
                        self.core.finish_push();
                        return Ok(true);
                    }
                    DecodeAction::InvalidFrame => {
                        self.core.finish_push();
                        return Err(DecodeError::InvalidFrame(*progress));
                    }
                }
                Ok(false)
            }
        }

        impl<$D> core::fmt::Display for CobsDecoderAsync<$D> {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, "{}", self.core)
            }
        }

        #[cfg(feature = "defmt")]
        impl<$D> defmt::Format for CobsDecoderAsync<$D> {
            fn format(&self, f: defmt::Formatter<'_>) {
                defmt::write!(f, "{}", self.core)
            }
        }
    }
}

pub(crate) use define_decoder;

#[cfg(test)]
mod tests {
    #[test]
    fn reset_works() {
        use super::{DecodeAction, DecoderCore};
        use crate::CompletionError;
        let mut core = DecoderCore::new();
        assert!(core.begin_push());
        for byte in [3, 1, 2, 4, 4, 5] {
            match core.accept_byte(byte) {
                DecodeAction::Skip => {}
                DecodeAction::Write(_) => core.acknowledge_write(),
                _ => panic!("unexpected frame boundary"),
            }
        }
        core.finish_push();
        assert!(core.frame_started);
        assert_eq!(
            core.check_complete(),
            Err(CompletionError::IncompleteFrame(5))
        );
        let prev_written = core.reset_frame();
        assert_eq!(prev_written, 5);
        assert_eq!(core.written, 0);
        assert!(!core.frame_started);
        assert_eq!(core.state.next_sentinel_idx, 0);
        assert!(core.state.next_byte_is_sentinel_or_sentinel_index);
        assert!(!core.state.current_block_is_full);
        assert_eq!(core.check_complete(), Ok(()));
        assert_eq!(core.finish_frame(), Err(CompletionError::NoFrame));
        assert_eq!(core.reset_frame(), 0);
        assert!(core.begin_push());
        assert!(matches!(core.accept_byte(2), DecodeAction::Skip));
        assert!(matches!(core.accept_byte(9), DecodeAction::Write(9)));
        core.acknowledge_write();
        assert!(matches!(
            core.accept_byte(0),
            DecodeAction::FrameComplete(1)
        ));
        core.finish_push();
        assert_eq!(core.written, 0);
        assert_eq!(core.finish_frame(), Err(CompletionError::NoFrame));
    }

    #[test]
    fn discard_counts_only_acknowledged_output() {
        use super::{DecodeAction, DecoderCore};
        use crate::CompletionError;
        let mut core = DecoderCore::new();
        assert!(core.begin_push());
        assert!(matches!(core.accept_byte(3), DecodeAction::Skip));
        assert!(matches!(core.accept_byte(7), DecodeAction::Write(7)));
        core.acknowledge_write();
        assert!(matches!(core.accept_byte(8), DecodeAction::Write(8)));
        assert_eq!(core.check_complete(), Err(CompletionError::InvalidState));
        core.begin_discard();
        assert_eq!(core.finish_discard(), 1);
        assert_eq!(core.check_complete(), Ok(()));
        assert_eq!(core.finish_frame(), Err(CompletionError::NoFrame));
    }
}
