use crate::{
    Config, StreamId,
    connection::{ConnectionId, StreamCommand},
    frame::{Frame, header},
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Flag {
    None,
    Syn,
    Ack,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum State {
    Open { acknowledged: bool },
    SendClosed,
    RecvClosed,
    Closed,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Open { .. } => write!(f, "Open"),
            State::SendClosed => write!(f, "SendClosed"),
            State::RecvClosed => write!(f, "RecvClosed"),
            State::Closed => write!(f, "Closed"),
        }
    }
}

impl State {
    pub fn can_read(self) -> bool {
        matches!(self, State::Open { .. } | State::SendClosed)
    }

    pub fn can_write(self) -> bool {
        matches!(self, State::Open { .. } | State::RecvClosed)
    }
}

pub struct Stream {
    id: StreamId,
    config: Arc<Config>,
    connection_id: ConnectionId,
    sender: mpsc::Sender<StreamCommand>,
    flag: Flag,
    shared: Arc<Mutex<Shared>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id.val())
            .field("connection", &self.connection_id)
            .finish()
    }
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(Stream {}/{})", self.connection_id, self.id.val())
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
            connection_id,
            config: config.clone(),
            sender,
            flag: Flag::Ack,
            shared: Arc::new(Mutex::new(Shared::new(id, connection_id))),
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
            connection_id,
            config: config.clone(),
            sender,
            flag: Flag::Syn,
            shared: Arc::new(Mutex::new(Shared::new(id, connection_id))),
        }
    }

    pub(crate) fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }

    pub(crate) fn clone_shared(&self) -> Arc<Mutex<Shared>> {
        self.shared.clone()
    }

    fn add_flag(&mut self, header: &mut header::Header) {
        match self.flag {
            Flag::None => (),
            Flag::Syn => {
                header.syn();
                self.flag = Flag::None
            }
            Flag::Ack => {
                header.ack();
                self.flag = Flag::None
            }
        }
    }

    fn write_zero_err(&self) -> io::Error {
        io::ErrorKind::WriteZero.into()
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.shared().state, State::Closed)
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    state: State,
    id: StreamId,
    pub(crate) connection_id: ConnectionId,
    pub(crate) buffer: BytesMut,
    pub(crate) reader: Option<Waker>,
    pub(crate) writer: Option<Waker>,
}

impl Shared {
    fn new(id: StreamId, connection_id: ConnectionId) -> Self {
        Self {
            state: State::Open {
                acknowledged: false,
            },
            id,
            connection_id,
            buffer: BytesMut::new(),
            reader: None,
            writer: None,
        }
    }

    pub(crate) fn state(&self) -> State {
        self.state
    }

    pub(crate) fn update_state(&mut self, next: State) -> State {
        let current = self.state;
        match (current, next) {
            (State::Open { .. }, _) => self.state = next,
            (State::RecvClosed, State::SendClosed) => self.state = State::Closed,
            (State::SendClosed, State::RecvClosed) => self.state = State::Closed,
            (_, State::Closed) => self.state = State::Closed,
            _ => {}
        }
        tracing::trace!(
            "(Stream {}/{}) state changed from {} to {}, input: {}",
            self.connection_id,
            self.id,
            current,
            self.state,
            next,
        );
        current
    }

    pub fn is_pending_ack(&self) -> bool {
        matches!(
            self.state(),
            State::Open {
                acknowledged: false
            }
        )
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if !self.config.read_after_close && self.sender.is_closed() {
            return Poll::Ready(Ok(0));
        }
        let mut shared = self.shared();
        let mut n = shared.buffer.len().min(buf.len());
        if n > 0 {
            n = shared.buffer.len().min(buf.len());
            buf[..n].copy_from_slice(&shared.buffer[..n]);
            shared.buffer.advance(n);
            tracing::trace!("{}: read {} bytes", self, n);
            return Poll::Ready(Ok(n));
        }
        if !shared.state.can_read() {
            tracing::trace!("{}: eof", self);
            return Poll::Ready(Ok(0));
        }
        // 等待数据到来
        shared.reader = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(
            self.sender
                .poll_ready(cx)
                .map_err(|_| self.write_zero_err())?
        );
        if !self.shared().state.can_write() {
            tracing::trace!("{}: can't no logger write", self);
            return Poll::Ready(Err(self.write_zero_err()));
        }
        let n = buf.len();
        let mut frame =
            Frame::new_data(self.id, Bytes::copy_from_slice(buf)).expect("Frame creation failed");
        self.add_flag(frame.header_mut());
        if frame.header().flags().contains(header::ACK) {
            tracing::trace!("{}: ack frame", self);
            self.shared()
                .update_state(State::Open { acknowledged: true });
        }
        tracing::trace!("{}: write {} bytes, frame: {:?}", self, n, frame);
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
        if self.is_closed() {
            return Poll::Ready(Ok(()));
        }
        ready!(
            self.sender
                .poll_ready(cx)
                .map_err(|_| self.write_zero_err())?
        );
        let ack = if self.flag == Flag::Ack {
            self.flag = Flag::None;
            true
        } else {
            false
        };

        let cmd = StreamCommand::CloseStream { ack };
        tracing::trace!("{}: close stream", self);
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        self.shared().update_state(State::SendClosed);

        Poll::Ready(Ok(()))
    }
}
