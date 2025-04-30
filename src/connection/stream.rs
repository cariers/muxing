use super::{ConnectionId, StreamCommand};
use crate::{
    Config, StreamId,
    frame::{Flags, Frame},
};
use bytes::{Buf, Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, SinkExt, channel::mpsc, ready};
use parking_lot::{Mutex, MutexGuard};
use std::{
    fmt, io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use tracing::{debug, trace};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum State {
    Open { ack: bool },
    SendClosed,
    RecvClosed,
    Closed,
}

impl State {
    pub fn can_read(self) -> bool {
        !matches!(self, State::RecvClosed | State::Closed)
    }
    pub fn can_write(self) -> bool {
        !matches!(self, State::SendClosed | State::Closed)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Flag {
    /// The stream was opened lazily, so set the initial SYN flag.
    Syn,
    /// The stream still needs acknowledgement, so set the ACK flag.
    Ack,
}

pub struct Stream {
    id: StreamId,
    flag: Option<Flag>,
    connection_id: ConnectionId,
    config: Arc<Config>,
    sender: mpsc::Sender<StreamCommand>,
    shared: Arc<Mutex<Shared>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id.val())
            .field("connection", &self.connection_id)
            .finish()
    }
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}/{})", self.connection_id, self.id)
    }
}

impl Stream {
    pub(crate) fn new_inbound(
        id: StreamId,
        connection_id: ConnectionId,
        config: Arc<Config>,
        sender: mpsc::Sender<StreamCommand>,
    ) -> Self {
        Self {
            id,
            flag: Some(Flag::Ack),
            connection_id,
            config,
            sender,
            shared: Arc::new(Mutex::new(Shared::new())),
        }
    }

    pub(crate) fn new_outbound(
        id: StreamId,
        connection_id: ConnectionId,
        config: Arc<Config>,
        sender: mpsc::Sender<StreamCommand>,
    ) -> Self {
        Self {
            id,
            flag: Some(Flag::Syn),
            connection_id,
            config,
            sender,
            shared: Arc::new(Mutex::new(Shared::new())),
        }
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn is_write_closed(&self) -> bool {
        matches!(self.shared().state(), State::SendClosed)
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.shared().state(), State::Closed)
    }

    pub(crate) fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }

    pub(crate) fn clone_shared(&self) -> Arc<Mutex<Shared>> {
        self.shared.clone()
    }

    fn write_zero_err(&self) -> io::Error {
        let msg = format!("{}/{}: connection is closed", self.connection_id, self.id);
        io::Error::new(io::ErrorKind::WriteZero, msg)
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.sender.is_closed() {
            return Poll::Ready(Ok(0));
        }
        let mut shared = self.shared();
        let mut n = 0; //读取的字节数
        while n < buf.len() {
            let len = shared.buffer.len();
            let read_len = len.min(buf.len() - n);
            buf[n..n + read_len].copy_from_slice(&shared.buffer[..read_len]);
            n += read_len;
            shared.buffer.advance(read_len);
            if n == buf.len() {
                break;
            }
        }
        if n > 0 {
            trace!("{}/{}: read {} bytes", self.connection_id, self.id, n);
            return Poll::Ready(Ok(n));
        }
        if !shared.state().can_read() {
            debug!("{}/{}: eof", self.connection_id, self.id);
            return Poll::Ready(Ok(0));
        }
        // 等待读被唤醒
        shared.reader = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self.sender.poll_ready(cx)).map_err(|_| self.write_zero_err())?;

        let payload = {
            let shared = self.shared();
            if !shared.state().can_write() {
                debug!("{}/{}: can no longer write", self.connection_id, self.id);
                return Poll::Ready(Err(self.write_zero_err()));
            }
            if buf.len() > self.config.max_frame_size as usize {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{}/{}: frame size {} exceeds max frame size {}",
                        self.connection_id,
                        self.id,
                        buf.len(),
                        self.config.max_frame_size
                    ),
                )));
            }
            Bytes::copy_from_slice(buf)
        };
        let n = payload.len();
        let mut frame = Frame::new_data(self.id, payload);
        match self.flag.take() {
            Some(Flag::Syn) => frame.header_mut().syn(),
            Some(Flag::Ack) => frame.header_mut().ack(),
            None => {}
        };
        trace!("{}/{}: write {} bytes", self.connection_id, self.id, n);
        if frame.header().flags().contains(Flags::ACK) {
            // 切换到ACK状态
            self.shared()
                .update_state(self.connection_id, self.id, State::Open { ack: true });
        }
        let cmd = StreamCommand::SendFrame(frame);
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.sender
            .poll_flush_unpin(cx)
            .map_err(|_| self.write_zero_err())
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.sender.is_closed() {
            return Poll::Ready(Ok(()));
        }
        ready!(self.sender.poll_ready(cx)).map_err(|_| self.write_zero_err())?;

        let ack = if self.flag == Some(Flag::Ack) {
            self.flag.take();
            true
        } else {
            false
        };
        trace!("{}/{}: close", self.connection_id, self.id);
        let cmd = StreamCommand::CloseStream { ack };
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;

        self.shared()
            .update_state(self.connection_id, self.id, State::SendClosed);
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    state: State,
    pub(crate) buffer: BytesMut,
    pub(crate) reader: Option<Waker>,
    pub(crate) writer: Option<Waker>,
}

impl Shared {
    fn new() -> Self {
        Self {
            state: State::Open { ack: false },
            buffer: BytesMut::new(),
            reader: None,
            writer: None,
        }
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn is_pending_ack(&self) -> bool {
        matches!(self.state(), State::Open { ack: false })
    }

    pub(crate) fn update_state(&mut self, cid: ConnectionId, sid: StreamId, next: State) -> State {
        use self::State::*;
        let current = self.state;
        match (current, next) {
            (Closed, _) => {}
            (Open { .. }, _) => self.state = next,
            (RecvClosed, Closed) => self.state = Closed,
            (RecvClosed, Open { .. }) => {}
            (RecvClosed, RecvClosed) => {}
            (RecvClosed, SendClosed) => self.state = Closed,
            (SendClosed, Closed) => self.state = Closed,
            (SendClosed, Open { .. }) => {}
            (SendClosed, RecvClosed) => self.state = Closed,
            (SendClosed, SendClosed) => {}
        }
        trace!(
            "{}/{}: update state: (from {:?} to {:?} -> {:?})",
            cid, sid, current, next, self.state
        );
        current
    }
}

#[pin_project::pin_project]
pub struct TaggedStream<K, S> {
    key: K,
    #[pin]
    inner: S,

    reported_none: bool,
}

impl<K, S> TaggedStream<K, S> {
    pub fn new(key: K, inner: S) -> Self {
        Self {
            key,
            inner,
            reported_none: false,
        }
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<K, S> futures::Stream for TaggedStream<K, S>
where
    K: Copy,
    S: futures::Stream,
{
    type Item = (K, Option<S::Item>);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.reported_none {
            return Poll::Ready(None);
        }

        match futures::ready!(this.inner.poll_next(cx)) {
            Some(item) => Poll::Ready(Some((*this.key, Some(item)))),
            None => {
                *this.reported_none = true;

                Poll::Ready(Some((*this.key, None)))
            }
        }
    }
}
