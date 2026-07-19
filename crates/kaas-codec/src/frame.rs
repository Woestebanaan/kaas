//! Length-prefixed request/response framing over async I/O.
//!
//! Every Kafka frame is `[size:i32_be][body:size_bytes]`. The `i32` is the
//! body size only — it does not include itself. Frame size > [`MAX_FRAME_SIZE`]
//! (or the configured ceiling) is rejected without buffering, so a malicious
//! peer cannot drive the broker OOM with a spoofed prefix.

use bytes::{Bytes, BytesMut};
use thiserror::Error;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Default ceiling on a single frame's body. 100 MiB matches the v0.1 broker's
/// implicit limit (any larger value would have to be configured anyway).
pub const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FrameError {
    /// Peer closed the connection cleanly before sending the next frame.
    #[error("peer disconnected")]
    Disconnected,

    /// The size prefix declared a body larger than this connection accepts.
    #[error("frame too large: {got} > {max}")]
    TooLarge { got: usize, max: usize },

    /// The size prefix was negative.
    #[error("negative frame size: {0}")]
    NegativeSize(i32),

    /// An underlying I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Stateful frame reader. Construct once per connection so [`MAX_FRAME_SIZE`]
/// overrides are remembered across reads.
#[derive(Debug)]
pub struct FrameReader {
    max: usize,
}

impl Default for FrameReader {
    fn default() -> Self {
        Self::new(MAX_FRAME_SIZE)
    }
}

impl FrameReader {
    pub fn new(max: usize) -> Self {
        Self { max }
    }

    pub async fn read<R: AsyncRead + Unpin>(&self, r: &mut R) -> Result<Bytes, FrameError> {
        read_frame_with_limit(r, self.max).await
    }
}

/// One-shot frame read using the default size ceiling.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Bytes, FrameError> {
    read_frame_with_limit(r, MAX_FRAME_SIZE).await
}

async fn read_frame_with_limit<R: AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
) -> Result<Bytes, FrameError> {
    let mut sz = [0u8; 4];
    match r.read_exact(&mut sz).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(FrameError::Disconnected),
        Err(e) => return Err(FrameError::Io(e)),
    }
    let n = i32::from_be_bytes(sz);
    if n < 0 {
        return Err(FrameError::NegativeSize(n));
    }
    let n_us = usize::try_from(n).map_err(|_| FrameError::TooLarge { got: 0, max })?;
    if n_us > max {
        return Err(FrameError::TooLarge { got: n_us, max });
    }
    let mut body = BytesMut::with_capacity(n_us);
    body.resize(n_us, 0);
    r.read_exact(&mut body).await?;
    Ok(body.freeze())
}

/// Write a single frame: the i32 size prefix is computed from `body.len()`
/// and prepended. Errors if the body length doesn't fit i32 (theoretical;
/// real bodies are bounded by [`MAX_FRAME_SIZE`]).
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, body: &[u8]) -> io::Result<()> {
    let n = i32::try_from(body.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame body > i32::MAX"))?;
    w.write_all(&n.to_be_bytes()).await?;
    w.write_all(body).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn roundtrip_single_frame() {
        let (mut a, mut b) = duplex(64);
        let payload = b"hello kafka";
        tokio::spawn(async move {
            write_frame(&mut a, payload).await.unwrap();
        });
        let got = read_frame(&mut b).await.unwrap();
        assert_eq!(&got[..], payload);
    }

    #[tokio::test]
    async fn roundtrip_multiple_frames() {
        let (mut a, mut b) = duplex(64);
        let p1: &[u8] = &[0, 1, 2, 3];
        let p2: &[u8] = b"second";
        tokio::spawn(async move {
            write_frame(&mut a, p1).await.unwrap();
            write_frame(&mut a, p2).await.unwrap();
        });
        let g1 = read_frame(&mut b).await.unwrap();
        let g2 = read_frame(&mut b).await.unwrap();
        assert_eq!(&g1[..], p1);
        assert_eq!(&g2[..], p2);
    }

    #[tokio::test]
    async fn disconnect_before_size_prefix() {
        let (a, mut b) = duplex(64);
        drop(a);
        let err = read_frame(&mut b).await.unwrap_err();
        assert!(matches!(err, FrameError::Disconnected));
    }

    #[tokio::test]
    async fn negative_size_rejected() {
        let (mut a, mut b) = duplex(64);
        tokio::spawn(async move {
            a.write_all(&(-1i32).to_be_bytes()).await.unwrap();
        });
        let err = read_frame(&mut b).await.unwrap_err();
        assert!(matches!(err, FrameError::NegativeSize(-1)));
    }

    #[tokio::test]
    async fn oversized_rejected_without_buffering() {
        let (mut a, mut b) = duplex(64);
        tokio::spawn(async move {
            let oversize = i32::try_from(MAX_FRAME_SIZE).unwrap().saturating_add(1);
            a.write_all(&oversize.to_be_bytes()).await.unwrap();
        });
        let err = read_frame(&mut b).await.unwrap_err();
        assert!(matches!(err, FrameError::TooLarge { .. }));
    }
}
