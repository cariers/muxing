mod connection;
mod error;
mod frame;

pub use connection::{Connection, Stream};
pub use error::{ConnectionError, FrameDecodeError, HeaderDecodeError};
pub use frame::StreamId;

pub const DEFAULT_FRAME_SIZE: u32 = 8 * 1024;
pub const HEADER_SIZE: usize = 12;

pub type Result<T> = std::result::Result<T, ConnectionError>;

#[derive(Debug, Clone)]
pub struct Config {
    max_num_streams: usize,
    max_frame_size: u32,
    max_ack_backlog: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_num_streams: 512,
            max_ack_backlog: 512,
            max_frame_size: DEFAULT_FRAME_SIZE,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_num_streams(&mut self, max_num_streams: usize) -> &Self {
        self.max_num_streams = max_num_streams;
        self
    }

    pub fn max_frame_size(&mut self, max_frame_size: u32) -> &Self {
        self.max_frame_size = max_frame_size;
        self
    }
    pub fn max_ack_backlog(&mut self, max_ack_backlog: usize) -> &Self {
        self.max_ack_backlog = max_ack_backlog;
        self
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Endpoint {
    Client,
    Server,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = Config::new();
        assert_eq!(config.max_num_streams, 512);
        assert_eq!(config.max_frame_size, DEFAULT_FRAME_SIZE);
        assert_eq!(config.max_ack_backlog, 512);

        let mut config = Config::new();
        config.max_num_streams(1024);
        config.max_frame_size(16 * 1024);
        config.max_ack_backlog(1024);

        assert_eq!(config.max_num_streams, 1024);
        assert_eq!(config.max_frame_size, 16 * 1024);
        assert_eq!(config.max_ack_backlog, 1024);
    }
}
