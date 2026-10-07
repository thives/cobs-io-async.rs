# Project description
This is an implementation of async COBS in Rust. This is a layer in a network stack for an unreliable point to point link.

# Rules
- Only add documentation when asked specifically. 
- Do not edit code and add documentation at the same time, either or.
- Do not add any additional dependencies or libraries. Removing is allowed.
- When reviewing be strict and direct.
- This is typically the lowest layer so input will be byte streams.
- Do not make commits

# Architecture

The poll-based IO trait looks like this:
```rust
trait Transport {
    type Error;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Self::Error>>;

    fn poll_flush(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>>;
}

# API

The API of this layer is a runtime independent interface that implements the Transport trait. It will have an internal buffer for incoming and outgoing packets, and it will handle retransmissions, acknowledgments, and timeouts.
This crate does not provide adaptors for any specific runtime, but it is expected that the user will implement their own adaptors for their specific runtime. The API will be designed to be as simple as possible, while still providing the necessary functionality for reliable communication over an unreliable link.
