use crate::{
    FrameDecodeError,
    connection::ConnectionId,
    frame::{Frame, header},
};
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, Sink, Stream, ready};
use std::{
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

const MAX_FRAME_BODY_LEN: usize = 1024 * 1024; // 1 MiB

enum ReadState {
    Init,
    Header {
        offset: usize,
        buffer: [u8; header::HEADER_SIZE],
    },
    Body {
        header: header::Header,
        offset: usize,
        buffer: BytesMut,
    },
}

impl fmt::Debug for ReadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadState::Init => write!(f, "ReadState::Init"),
            ReadState::Header { offset, .. } => {
                write!(f, "ReadState::Header ( offset: {} )", offset)
            }
            ReadState::Body { header, offset, .. } => {
                write!(f, "ReadState::Body(header: {}, offset: {})", header, offset)
            }
        }
    }
}

pub(crate) struct Framed<T> {
    id: ConnectionId,
    inner: T,
    read_state: ReadState,
    write_state: WriteState,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Framed<T> {
    pub fn new(id: ConnectionId, inner: T) -> Self {
        Framed {
            id,
            inner,
            read_state: ReadState::Init,
            write_state: WriteState::Init,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for Framed<T> {
    type Item = Result<Frame, FrameDecodeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            tracing::trace!("Connection({}): read {:?}", this.id, this.read_state);
            match &mut this.read_state {
                ReadState::Init => {
                    this.read_state = ReadState::Header {
                        offset: 0,
                        buffer: [0; header::HEADER_SIZE],
                    };
                }
                ReadState::Header { offset, buffer } => {
                    if *offset == header::HEADER_SIZE {
                        let header = header::decode(buffer)?;
                        tracing::trace!("Connection({}): decoded header: {}", this.id, header);
                        let body_len = header.length() as usize;

                        if body_len > MAX_FRAME_BODY_LEN {
                            return Poll::Ready(Some(Err(FrameDecodeError::TooLarge(
                                body_len,
                                MAX_FRAME_BODY_LEN,
                            ))));
                        }
                        // 申请足够的缓冲区来存储 body，转入读取包体状态
                        this.read_state = ReadState::Body {
                            header,
                            offset: 0,
                            buffer: BytesMut::zeroed(body_len),
                        };
                        continue;
                    }
                    let buf = &mut buffer[*offset..header::HEADER_SIZE];
                    match ready!(Pin::new(&mut this.inner).poll_read(cx, buf))? {
                        0 => {
                            if *offset == 0 {
                                return Poll::Ready(None);
                            } else {
                                return Poll::Ready(Some(Err(io::Error::from(
                                    io::ErrorKind::UnexpectedEof,
                                )
                                .into())));
                            }
                        }
                        n => *offset += n,
                    }
                }
                ReadState::Body {
                    header,
                    offset,
                    buffer,
                } => {
                    let body_len = header.length() as usize;
                    if *offset == body_len {
                        // 完成读取包体，创建 Frame 实例
                        let header = header.clone();
                        let buffer = buffer.split().freeze();
                        this.read_state = ReadState::Init; // 重置状态为 Init
                        return Poll::Ready(Some(Ok(Frame {
                            header,
                            body: buffer,
                        })));
                    }
                    let buf = &mut buffer[*offset..body_len];
                    // 从流中读取数据到缓冲区
                    match ready!(Pin::new(&mut this.inner).poll_read(cx, buf))? {
                        0 => {
                            return Poll::Ready(Some(Err(io::Error::from(
                                io::ErrorKind::UnexpectedEof,
                            )
                            .into())));
                        }
                        n => *offset += n,
                    }
                }
            }
        }
    }
}

enum WriteState {
    Init,
    Header {
        header: [u8; header::HEADER_SIZE],
        buffer: Bytes,
        offset: usize,
    },
    Body {
        offset: usize,
        buffer: Bytes,
    },
}

impl fmt::Debug for WriteState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteState::Init => write!(f, "WriteState::Init"),
            WriteState::Header { offset, .. } => {
                write!(f, "WriteState::Header ( offset: {} )", offset)
            }
            WriteState::Body { offset, buffer } => {
                write!(
                    f,
                    "WriteState::Body(offset: {}, length: {})",
                    offset,
                    buffer.len()
                )
            }
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Sink<Frame> for Framed<T> {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = &mut *self;
        loop {
            match &mut this.write_state {
                WriteState::Init => {
                    // 如果是初始状态，直接返回 Poll::Ready
                    return Poll::Ready(Ok(()));
                }
                WriteState::Header {
                    header,
                    buffer,
                    offset,
                } => match Pin::new(&mut this.inner).poll_write(cx, &header[*offset..]) {
                    Poll::Ready(Ok(n)) => {
                        *offset += n;
                        if *offset == header.len() {
                            if buffer.is_empty() {
                                // 如果没有数据，直接进入 Init 状态
                                this.write_state = WriteState::Init;
                            } else {
                                // 否则进入 Body 状态
                                this.write_state = WriteState::Body {
                                    offset: 0,
                                    buffer: buffer.clone(),
                                };
                            }
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                },
                WriteState::Body { offset, buffer } => {
                    // 在 Body 状态下，继续写入数据
                    match Pin::new(&mut this.inner).poll_write(cx, &buffer[*offset..]) {
                        Poll::Ready(Ok(n)) => {
                            *offset += n;
                            if *offset == buffer.len() {
                                // 如果写入完成，重置状态为 Init
                                this.write_state = WriteState::Init;
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    }
                }
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        tracing::trace!("Connection({}): start_send {:?}", self.id, item);
        let Frame { header, body } = item;
        self.get_mut().write_state = WriteState::Header {
            header: header::encode(&header),
            buffer: body,
            offset: 0,
        };
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // 确保在调用 poll_flush 之前，已经调用了 poll_ready, 发送缓冲区
        ready!(self.as_mut().poll_ready(cx))?;
        Pin::new(&mut self.as_mut().inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tracing::trace!("Connection({}): poll_close", self.id);
        // 确保在调用 poll_close 之前，已经调用了 poll_ready, 发送缓冲区
        ready!(self.as_mut().poll_ready(cx))?;
        Pin::new(&mut self.as_mut().inner).poll_close(cx)
    }
}
