use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Decode error: {0}")]
    Decode(#[from] FrameDecodeError),
    #[error("No more stream IDs available")]
    NoMoreStreamIds,
    #[error("Connection is closed")]
    Closed,
    #[error("Maximum number of active streams")]
    TooManyStreams,
}

#[derive(Debug, thiserror::Error)]
pub enum FrameDecodeError {
    #[error("Unknown version: {0}")]
    Version(u8),
    #[error("Frame length is too large, expected at most {1}, got {0}")]
    TooLarge(usize, usize),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<futures::channel::mpsc::SendError> for ConnectionError {
    fn from(_: futures::channel::mpsc::SendError) -> Self {
        ConnectionError::Closed
    }
}

impl From<futures::channel::oneshot::Canceled> for ConnectionError {
    fn from(_: futures::channel::oneshot::Canceled) -> Self {
        ConnectionError::Closed
    }
}
