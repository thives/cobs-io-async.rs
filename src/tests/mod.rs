mod decoding;
mod encoding;
mod support;

#[test]
fn public_value_types_support_copy_equality_and_hashing() {
    use crate::{CompletionError, DecodeError, DecodeProgress, EncodeError, SeekableError};

    fn assert_traits<T: Copy + Eq + core::hash::Hash>() {}

    assert_traits::<SeekableError>();
    assert_traits::<CompletionError>();
    assert_traits::<DecodeProgress>();
    assert_traits::<EncodeError<SeekableError, SeekableError>>();
    assert_traits::<DecodeError<SeekableError, SeekableError>>();
}
