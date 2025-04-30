#[derive(Debug, thiserror::Error)]
pub enum HeaderDecodeError {
    #[error("Invalid header version({0})")]
    Version(u8),
    #[error("Invalid header flags")]
    Flags,
}

#[derive(Debug, thiserror::Error)]
pub enum FrameDecodeError {
    #[error(transparent)]
    Header(#[from] HeaderDecodeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("frame body is too large ({0})")]
    FrameTooLarge(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Decode(#[from] FrameDecodeError),
    #[error("number of stream ids has been exhausted")]
    NoMoreStreamIds,
    #[error("maximum number of streams reached")]
    TooManyStreams,
    #[error("connection is closed")]
    Closed,
}
