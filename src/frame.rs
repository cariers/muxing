mod io;

pub mod header;

use bytes::Bytes;
use header::{Header, StreamId};
use std::num::TryFromIntError;

pub(crate) use io::Framed;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    header: Header,
    body: Bytes,
}

impl Frame {
    pub fn new(header: Header) -> Self {
        Frame {
            header,
            body: Bytes::new(),
        }
    }

    pub fn new_data(id: StreamId, body: Bytes) -> Result<Self, TryFromIntError> {
        Ok(Frame {
            header: Header::data(id, body.len().try_into()?),
            body,
        })
    }

    pub fn new_close_stream(id: StreamId, ack: bool) -> Self {
        let mut header = Header::data(id, 0);
        header.fin();
        if ack {
            header.ack();
        }
        Frame {
            header,
            body: Bytes::new(),
        }
    }

    pub fn new_protocol_error() -> Self {
        Frame {
            header: Header::go_way(0x1),
            body: Bytes::new(),
        }
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }

    pub(crate) fn into_body(self) -> Bytes {
        self.body
    }

    pub fn is_termination(&self) -> bool {
        self.header.is_termination()
    }
}
