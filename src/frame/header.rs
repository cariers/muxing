use std::fmt;

use crate::FrameDecodeError;

pub const HEADER_SIZE: usize = 12;
pub const VERSION: u8 = 0x1;

const CONNECTION_ID: StreamId = StreamId::new(0);

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Flags(u8);

pub const SYN: Flags = Flags(1);
pub const ACK: Flags = Flags(2);
pub const FIN: Flags = Flags(4);
pub const RST: Flags = Flags(8);

impl Flags {
    pub fn contains(self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId(u32);

impl StreamId {
    pub(crate) const fn new(val: u32) -> Self {
        StreamId(val)
    }

    pub fn is_server(self) -> bool {
        self.0 % 2 == 0
    }

    pub fn is_client(self) -> bool {
        !self.is_server()
    }

    pub fn val(self) -> u32 {
        self.0
    }

    pub fn next_id(self) -> Self {
        StreamId(self.0 + 1)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl nohash_hasher::IsEnabled for StreamId {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    version: u8,
    flags: Flags,
    reserve: u16,
    stream_id: StreamId,
    length: u32,
}

impl Header {
    pub fn data(id: StreamId, length: u32) -> Self {
        Header {
            version: VERSION,
            flags: Flags(0),
            reserve: 0,
            stream_id: id,
            length,
        }
    }

    pub fn go_way(code: u16) -> Self {
        Header {
            version: VERSION,
            flags: Flags(0),
            reserve: code,
            stream_id: StreamId::new(0),
            length: 0,
        }
    }

    pub fn flags(&self) -> Flags {
        self.flags
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn length(&self) -> u32 {
        self.length
    }

    pub fn rst(&mut self) {
        self.flags.0 |= RST.0
    }

    pub fn syn(&mut self) {
        self.flags.0 |= SYN.0
    }

    pub fn fin(&mut self) {
        self.flags.0 |= FIN.0
    }

    pub fn ack(&mut self) {
        self.flags.0 |= ACK.0
    }

    pub fn is_termination(&self) -> bool {
        self.stream_id == CONNECTION_ID && self.reserve > 0
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Header ( version: {}, flags: {:?}, reserve: {}, stream_id: {}, length: {} )",
            self.version, self.flags, self.reserve, self.stream_id.0, self.length
        )
    }
}

pub fn encode(hdr: &Header) -> [u8; HEADER_SIZE] {
    let mut buf = [0; HEADER_SIZE];
    buf[0] = hdr.version;
    buf[1] = hdr.flags.0;
    buf[2..4].copy_from_slice(&hdr.reserve.to_be_bytes());
    buf[4..8].copy_from_slice(&hdr.stream_id.0.to_be_bytes());
    buf[8..HEADER_SIZE].copy_from_slice(&hdr.length.to_be_bytes());
    buf
}

pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Header, FrameDecodeError> {
    let version = buf[0];
    if version != VERSION {
        return Err(FrameDecodeError::Version(version));
    }
    let flags = Flags(buf[1]);
    let reserve = u16::from_be_bytes([buf[2], buf[3]]);
    let stream_id = StreamId(u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]));
    let length = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);

    Ok(Header {
        version,
        flags,
        reserve,
        stream_id,
        length,
    })
}
