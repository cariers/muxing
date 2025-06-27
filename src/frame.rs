use asynchronous_codec::{Decoder, Encoder};
use bitflags::bitflags;
use bytes::{Buf, Bytes, BytesMut};
use std::{fmt, io};

use crate::{FrameDecodeError, HEADER_SIZE, HeaderDecodeError};

pub const CONNECTION_ID: StreamId = StreamId(0);

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId(u32);

impl StreamId {
    pub(crate) fn new(val: u32) -> Self {
        StreamId(val)
    }

    pub fn is_server(self) -> bool {
        self.0 % 2 == 0
    }

    pub fn is_client(self) -> bool {
        !self.is_server()
    }

    pub fn is_connection(self) -> bool {
        self == CONNECTION_ID
    }

    pub fn val(self) -> u32 {
        self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Flags: u8 {
        const FIN = 0x1;
        const RST = 0x8;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    version: u8,
    flags: Flags,
    reserved: u16,
    stream_id: StreamId,
    length: u32,
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "(Header {} (len {}) (flags {:?}))",
            self.stream_id, self.length, self.flags
        )
    }
}

impl Header {
    pub fn new(id: StreamId, length: u32) -> Self {
        Header {
            version: 0x1,
            flags: Flags::empty(),
            reserved: 0,
            stream_id: id,
            length,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn fin(&mut self) {
        self.flags.insert(Flags::FIN);
    }

    pub fn rst(&mut self) {
        self.flags.insert(Flags::RST);
    }

    pub fn flags(&self) -> Flags {
        self.flags
    }
}

pub fn encode_header(header: &Header) -> [u8; HEADER_SIZE] {
    let mut buf = [0; HEADER_SIZE];
    buf[0] = header.version;
    buf[1] = header.flags.bits();
    buf[2..4].copy_from_slice(&header.reserved.to_be_bytes());
    buf[4..8].copy_from_slice(&header.stream_id.0.to_be_bytes());
    buf[8..HEADER_SIZE].copy_from_slice(&header.length.to_be_bytes());
    buf
}

pub fn decode_header(buf: &[u8; HEADER_SIZE]) -> Result<Header, HeaderDecodeError> {
    let version = buf[0];
    if version != 0 {
        return Err(HeaderDecodeError::Version(version));
    }
    let flags = Flags::from_bits(buf[1]).ok_or(HeaderDecodeError::Flags)?;

    let hdr = Header {
        version,
        flags,
        reserved: u16::from_be_bytes([buf[2], buf[3]]),
        stream_id: StreamId(u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]])),
        length: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
    };

    Ok(hdr)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    header: Header,
    data: Bytes,
}

impl Frame {
    pub fn new(header: Header, data: Bytes) -> Self {
        Frame { header, data }
    }

    pub fn new_data(id: StreamId, data: Bytes) -> Self {
        Frame::new(Header::new(id, data.len() as u32), data)
    }

    pub fn new_empty(id: StreamId) -> Self {
        Frame::new(Header::new(id, 0), Bytes::new())
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }

    pub fn into_data(self) -> Bytes {
        self.data
    }

    pub fn into_pair(self) -> (Header, Bytes) {
        (self.header, self.data)
    }

    pub fn new_close(id: StreamId) -> Self {
        let mut frame = Frame::new_empty(id);
        frame.header_mut().fin();
        frame
    }
}

pub struct Codec {
    max_frame_size: u32,
}

impl Codec {
    pub(crate) fn new(max_frame_size: u32) -> Self {
        Codec { max_frame_size }
    }
}

impl Decoder for Codec {
    type Item = Frame;
    type Error = FrameDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let mut header_buf = [0_u8; HEADER_SIZE];
        header_buf.copy_from_slice(&src[..HEADER_SIZE]);
        let header = decode_header(&header_buf)?;
        if header.length > self.max_frame_size {
            return Err(FrameDecodeError::FrameTooLarge(header.length as usize));
        }
        if src.len() - HEADER_SIZE < header.length as usize {
            return Ok(None);
        }
        src.advance(HEADER_SIZE); // Skip the header
        let data = src.split_to(header.length as usize).freeze();
        Ok(Some(Frame::new(header, data)))
    }
}

impl Encoder for Codec {
    type Error = io::Error;
    type Item<'a> = Frame;

    fn encode(&mut self, item: Self::Item<'_>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (header, payload) = item.into_pair();
        let header = encode_header(&header);
        dst.reserve(HEADER_SIZE + payload.len());
        dst.extend_from_slice(&header);
        dst.extend_from_slice(&payload);
        Ok(())
    }
}
