use core::task::{Context, Poll};
use std::{
    collections::VecDeque,
    io::SeekFrom,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    vec::Vec,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TestError {
    OutOfBounds,
    Injected,
}

impl core::fmt::Display for TestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OutOfBounds => f.write_str("test writer operation is out of bounds"),
            Self::Injected => f.write_str("injected test writer failure"),
        }
    }
}

impl core::error::Error for TestError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    PendingOnce,
    PendingForever,
    Fail,
    Zero,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Event {
    pub(crate) after_bytes: usize,
    pub(crate) action: Action,
}

#[derive(Debug)]
pub(crate) struct TestWriter {
    storage: Vec<u8>,
    len: usize,
    cursor: usize,
    io_calls: usize,
    write_limit: usize,
    pending_writes: bool,
    zero_writes: bool,
    accepted_bytes: usize,
    script: VecDeque<Event>,
}

impl TestWriter {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            storage: std::vec![0x80; capacity],
            len: 0,
            cursor: 0,
            io_calls: 0,
            write_limit: capacity,
            pending_writes: false,
            zero_writes: false,
            accepted_bytes: 0,
            script: VecDeque::new(),
        }
    }

    pub(crate) fn accepted_bytes(&self) -> usize {
        self.accepted_bytes
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.storage[..self.len]
    }

    pub(crate) fn storage(&self) -> &[u8] {
        &self.storage
    }

    pub(crate) fn position(&self) -> u64 {
        self.cursor as u64
    }

    pub(crate) fn set_position(&mut self, position: u64) {
        let cursor = usize::try_from(position).expect("position does not fit usize");
        assert!(cursor <= self.storage.len(), "position exceeds capacity");
        self.cursor = cursor;
    }

    pub(crate) fn io_calls(&self) -> usize {
        self.io_calls
    }

    pub(crate) fn record_io(&mut self) {
        self.io_calls += 1;
    }

    pub(crate) fn set_write_limit(&mut self, limit: usize) {
        assert!(limit <= self.storage.len(), "write limit exceeds capacity");
        self.write_limit = limit;
    }

    pub(crate) fn set_pending_writes(&mut self, pending: bool) {
        self.pending_writes = pending;
    }

    pub(crate) fn set_zero_writes(&mut self, zero: bool) {
        self.zero_writes = zero;
    }

    pub(crate) fn set_script(&mut self, events: impl IntoIterator<Item = Event>) {
        let script: VecDeque<_> = events.into_iter().collect();
        let mut previous = self.accepted_bytes;
        for event in &script {
            assert!(
                event.after_bytes >= previous,
                "script must be ordered and must not precede accepted_bytes",
            );
            previous = event.after_bytes;
        }
        self.script = script;
    }

    pub(crate) fn clear_script(&mut self) {
        self.script.clear();
    }

    pub(crate) fn poll_write_bytes(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<Result<usize, TestError>> {
        self.record_io();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.pending_writes {
            return Poll::Pending;
        }
        if self.zero_writes {
            return Poll::Ready(Ok(0));
        }
        let mut count = bytes.len();

        if let Some(event) = self.script.front().copied() {
            if event.after_bytes == self.accepted_bytes {
                if !matches!(event.action, Action::PendingForever) {
                    self.script.pop_front();
                }
                return match event.action {
                    Action::PendingOnce => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Action::PendingForever => Poll::Pending,
                    Action::Fail => Poll::Ready(Err(TestError::Injected)),
                    Action::Zero => Poll::Ready(Ok(0)),
                };
            }
            debug_assert!(event.after_bytes > self.accepted_bytes);
            count = count.min(event.after_bytes - self.accepted_bytes);
        }
        Poll::Ready(self.write_ready(&bytes[..count]))
    }

    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize, TestError> {
        assert!(
            self.script.is_empty(),
            "use the async writer interface while a script is installed",
        );
        self.record_io();
        if bytes.is_empty() || self.zero_writes {
            return Ok(0);
        }
        self.write_ready(bytes)
    }

    pub(crate) fn seek_to(&mut self, from: SeekFrom) -> Result<u64, TestError> {
        self.record_io();
        let (base, offset) = match from {
            SeekFrom::Start(position) => (0, i128::from(position)),
            SeekFrom::End(offset) => (self.len as i128, i128::from(offset)),
            SeekFrom::Current(offset) => (self.cursor as i128, i128::from(offset)),
        };
        let position = base
            .checked_add(offset)
            .filter(|&position| position >= 0 && position <= self.storage.len() as i128)
            .ok_or(TestError::OutOfBounds)?;
        self.cursor = position as usize;
        Ok(self.position())
    }

    fn write_ready(&mut self, bytes: &[u8]) -> Result<usize, TestError> {
        let end = self
            .cursor
            .checked_add(bytes.len())
            .ok_or(TestError::OutOfBounds)?;
        if end > self.storage.len() || end > self.write_limit {
            return Err(TestError::OutOfBounds);
        }
        let accepted_bytes = self
            .accepted_bytes
            .checked_add(bytes.len())
            .expect("test writer accepted byte count overflow");
        self.storage[self.cursor..end].copy_from_slice(bytes);
        self.cursor = end;
        self.len = self.len.max(end);
        self.accepted_bytes = accepted_bytes;
        Ok(bytes.len())
    }
}

#[derive(Default)]
pub(crate) struct WakeCounter(AtomicUsize);

impl WakeCounter {
    pub(crate) fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl futures::task::ArcWake for WakeCounter {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_preserve_logical_length_and_spare_capacity() {
        let mut writer = TestWriter::new(8);
        assert!(writer.bytes().is_empty());
        assert_eq!(writer.storage(), &[0x80; 8]);
        assert_eq!(writer.position(), 0);
        assert_eq!(writer.io_calls(), 0);
        writer.set_position(2);
        assert!(writer.bytes().is_empty());
        assert_eq!(writer.io_calls(), 0);
        assert_eq!(writer.write_bytes(b"abcd"), Ok(4));
        writer.set_position(3);
        assert_eq!(writer.write_bytes(b"ZZ"), Ok(2));
        assert_eq!(writer.position(), 5);
        assert_eq!(writer.bytes(), &[0x80, 0x80, b'a', b'Z', b'Z', b'd']);
        assert_eq!(&writer.storage()[6..], &[0x80; 2]);
        assert_eq!(writer.io_calls(), 2);
    }

    #[test]
    fn failed_writes_leave_storage_and_cursor_unchanged() {
        let mut writer = TestWriter::new(8);
        writer.write_bytes(b"abcd").unwrap();
        writer.set_write_limit(5);
        let before = writer.storage().to_vec();
        assert_eq!(writer.write_bytes(b"XY"), Err(TestError::OutOfBounds));
        assert_eq!(writer.storage(), before.as_slice());
        assert_eq!(writer.bytes(), b"abcd");
        assert_eq!(writer.position(), 4);
        assert_eq!(writer.write_bytes(b"e"), Ok(1));
        assert_eq!(writer.write_bytes(b"f"), Err(TestError::OutOfBounds));
        assert_eq!(writer.position(), 5);
        assert_eq!(writer.bytes(), b"abcde");
        writer.set_write_limit(8);
        assert_eq!(writer.write_bytes(b"f"), Ok(1));
        assert_eq!(writer.bytes(), b"abcdef");
        writer.set_position(8);
        assert_eq!(writer.write_bytes(b"x"), Err(TestError::OutOfBounds));
        assert_eq!(writer.position(), 8);
        assert_eq!(writer.bytes(), b"abcdef");
    }

    #[test]
    fn empty_and_zero_writes_have_no_storage_effect() {
        let mut writer = TestWriter::new(4);
        writer.set_position(3);
        writer.set_write_limit(0);
        assert_eq!(writer.write_bytes(b""), Ok(0));
        writer.set_zero_writes(true);
        assert_eq!(writer.write_bytes(b"too long"), Ok(0));
        writer.set_zero_writes(false);
        assert_eq!(writer.write_bytes(b"x"), Err(TestError::OutOfBounds));
        assert!(writer.bytes().is_empty());
        assert_eq!(writer.storage(), &[0x80; 4]);
        assert_eq!(writer.position(), 3);
        assert_eq!(writer.io_calls(), 3);
    }

    #[test]
    fn seeks_are_checked_and_do_not_extend_length() {
        let mut writer = TestWriter::new(8);
        writer.write_bytes(b"abcd").unwrap();
        assert_eq!(writer.seek_to(SeekFrom::End(2)), Ok(6));
        assert_eq!(writer.bytes(), b"abcd");
        assert_eq!(writer.seek_to(SeekFrom::Current(-5)), Ok(1));
        let before = writer.storage().to_vec();
        for from in [
            SeekFrom::Start(9),
            SeekFrom::Start(u64::MAX),
            SeekFrom::Current(-2),
            SeekFrom::Current(i64::MIN),
            SeekFrom::Current(i64::MAX),
            SeekFrom::End(-5),
            SeekFrom::End(i64::MIN),
            SeekFrom::End(i64::MAX),
        ] {
            let calls = writer.io_calls();
            assert_eq!(writer.seek_to(from), Err(TestError::OutOfBounds));
            assert_eq!(writer.position(), 1);
            assert_eq!(writer.bytes(), b"abcd");
            assert_eq!(writer.storage(), before.as_slice());
            assert_eq!(writer.io_calls(), calls + 1);
        }
        assert_eq!(writer.seek_to(SeekFrom::End(-4)), Ok(0));
        assert_eq!(writer.seek_to(SeekFrom::Start(8)), Ok(8));
        assert_eq!(writer.seek_to(SeekFrom::End(0)), Ok(4));
        assert_eq!(writer.bytes(), b"abcd");
    }

    #[test]
    fn script_controls_progress_without_losing_data() {
        let mut writer = TestWriter::new(8);
        writer.set_script([
            Event {
                after_bytes: 2,
                action: Action::PendingOnce,
            },
            Event {
                after_bytes: 2,
                action: Action::Fail,
            },
            Event {
                after_bytes: 2,
                action: Action::Zero,
            },
            Event {
                after_bytes: 2,
                action: Action::PendingForever,
            },
        ]);
        let wakes = Arc::new(WakeCounter::default());
        let waker = futures::task::waker(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            writer.poll_write_bytes(&mut cx, b"abcde"),
            Poll::Ready(Ok(2)),
        );
        assert_eq!(writer.bytes(), b"ab");
        assert_eq!(writer.accepted_bytes(), 2);
        let before = writer.storage().to_vec();
        assert_eq!(writer.poll_write_bytes(&mut cx, b""), Poll::Ready(Ok(0)),);
        assert_eq!(writer.script.len(), 4);
        assert_eq!(wakes.count(), 0);
        assert!(writer.poll_write_bytes(&mut cx, b"cde").is_pending());
        assert_eq!(wakes.count(), 1);
        assert_eq!(
            writer.poll_write_bytes(&mut cx, b"cde"),
            Poll::Ready(Err(TestError::Injected)),
        );
        assert_eq!(writer.poll_write_bytes(&mut cx, b"cde"), Poll::Ready(Ok(0)),);
        for _ in 0..2 {
            assert!(writer.poll_write_bytes(&mut cx, b"cde").is_pending());
        }
        assert_eq!(writer.script.len(), 1);
        assert_eq!(wakes.count(), 1);
        assert_eq!(writer.storage(), before.as_slice());
        assert_eq!(writer.position(), 2);
        assert_eq!(writer.accepted_bytes(), 2);
        assert_eq!(writer.io_calls(), 7);
        writer.clear_script();
        assert_eq!(writer.poll_write_bytes(&mut cx, b"cde"), Poll::Ready(Ok(3)),);
        assert_eq!(writer.bytes(), b"abcde");
        assert_eq!(&writer.storage()[5..], &[0x80; 3]);
        assert_eq!(writer.position(), 5);
        assert_eq!(writer.accepted_bytes(), 5);
        assert_eq!(writer.io_calls(), 8);
    }

    #[test]
    fn script_counts_overwrites_and_preserves_atomic_bounds_failures() {
        let mut writer = TestWriter::new(8);
        assert_eq!(writer.write_bytes(b"AB"), Ok(2));
        assert_eq!(writer.seek_to(SeekFrom::Start(0)), Ok(0));
        assert_eq!(writer.write_bytes(b"Z"), Ok(1));
        assert_eq!(writer.bytes(), b"ZB");
        assert_eq!(writer.position(), 1);
        assert_eq!(writer.accepted_bytes(), 3);
        writer.set_script([Event {
            after_bytes: 5,
            action: Action::Fail,
        }]);
        writer.set_write_limit(2);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let before = writer.storage().to_vec();
        assert_eq!(
            writer.poll_write_bytes(&mut cx, b"xyz"),
            Poll::Ready(Err(TestError::OutOfBounds)),
        );
        assert_eq!(writer.storage(), before.as_slice());
        assert_eq!(writer.accepted_bytes(), 3);
        assert_eq!(writer.position(), 1);
        assert_eq!(writer.script.len(), 1);
        writer.set_write_limit(8);
        assert_eq!(writer.poll_write_bytes(&mut cx, b"xyz"), Poll::Ready(Ok(2)),);
        assert_eq!(writer.bytes(), b"Zxy");
        assert_eq!(writer.accepted_bytes(), 5);
        assert_eq!(
            writer.poll_write_bytes(&mut cx, b"z"),
            Poll::Ready(Err(TestError::Injected)),
        );
        assert_eq!(writer.bytes(), b"Zxy");
        assert_eq!(writer.position(), 3);
        assert_eq!(writer.accepted_bytes(), 5);
    }
}
