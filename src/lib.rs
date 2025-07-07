mod error;
mod frame;
mod tagged_stream;

pub(crate) mod connection;

const MAX_ACK_BACKLOG: usize = 256;

pub use crate::frame::header::StreamId;
pub use connection::{Connection, Endpoint, Stream};
pub use error::{ConnectionError, FrameDecodeError};

pub type Result<T> = std::result::Result<T, ConnectionError>;

#[derive(Debug, Clone)]
pub struct Config {
    max_active_streams: usize,
    read_after_close: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_active_streams: 512,
            read_after_close: true,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Config::default()
    }

    pub fn set_max_active_streams(&mut self, max_active_streams: usize) -> &mut Self {
        self.max_active_streams = max_active_streams;
        self
    }

    pub fn set_read_after_close(&mut self, read_after_close: bool) -> &mut Self {
        self.read_after_close = read_after_close;
        self
    }
}
