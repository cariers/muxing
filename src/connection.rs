mod cleanup;
mod closing;
mod stream;

pub use stream::Stream;

use crate::{
    Config, ConnectionError, MAX_ACK_BACKLOG, Result, StreamId,
    connection::stream::State,
    frame::{
        self, Frame,
        header::{self, Header},
    },
    tagged_stream::TaggedStream,
};
use cleanup::Cleanup;
use closing::Closing;
use futures::{
    AsyncRead, AsyncWrite, FutureExt, SinkExt, StreamExt,
    channel::mpsc,
    stream::{Fuse, SelectAll},
};
use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    fmt,
    sync::Arc,
    task::{Context, Poll, Waker},
};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Endpoint {
    /// Client to server connection.
    Client,
    /// Server to client connection.
    Server,
}

#[derive(Clone, Copy)]
pub(crate) struct ConnectionId(u32);

impl ConnectionId {
    pub(crate) fn random() -> Self {
        ConnectionId(rand::random())
    }
}

impl fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "0x{:08x}", self.0)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "0x{:08x}", self.0)
    }
}

#[derive(Debug)]
pub(crate) enum StreamCommand {
    /// 发送数据帧
    SendFrame(Frame),
    /// 关闭流
    CloseStream { ack: bool },
}

enum ConnectionState<T> {
    Active(Active<T>),
    Closing(Closing<T>),
    Cleanup(Cleanup),
    Closed,
    Poisoned,
}

impl<T> fmt::Debug for ConnectionState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionState::Active(_) => write!(f, "Active"),
            ConnectionState::Closing(_) => write!(f, "Closing"),
            ConnectionState::Cleanup(_) => write!(f, "Cleanup"),
            ConnectionState::Closed => write!(f, "Closed"),
            ConnectionState::Poisoned => write!(f, "Poisoned"),
        }
    }
}

#[derive(Debug)]
pub struct Connection<T> {
    inner: ConnectionState<T>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    pub fn new(socket: T, config: Config, endpoint: Endpoint) -> Self {
        let inner = ConnectionState::Active(Active::new(socket, config, endpoint));
        Self { inner }
    }

    pub fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
                ConnectionState::Active(mut active) => match active.poll_new_outbound(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Ready(Ok(stream));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Cleanup(active.cleanup(e));
                        continue;
                    }
                },
                ConnectionState::Closing(mut inner) => match inner.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(ConnectionError::Closed));
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(inner);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut inner) => match inner.poll_unpin(cx) {
                    Poll::Ready(e) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Cleanup(inner);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closed => {
                    self.inner = ConnectionState::Closed;
                    return Poll::Ready(Err(ConnectionError::Closed));
                }
                ConnectionState::Poisoned => unreachable!(),
            }
        }
    }

    pub fn poll_next_inbound(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Stream>>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
                ConnectionState::Active(mut active) => match active.poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Ready(Some(Ok(stream)));
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Cleanup(active.cleanup(e));
                        continue;
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closing(mut closing) => match closing.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(closing);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut cleanup) => match cleanup.poll_unpin(cx) {
                    Poll::Ready(ConnectionError::Closed) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(other) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Some(Err(other)));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Cleanup(cleanup);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closed => {
                    self.inner = ConnectionState::Closed;
                    return Poll::Ready(None);
                }
                ConnectionState::Poisoned => unreachable!(),
            }
        }
    }

    pub fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
                ConnectionState::Active(active) => {
                    self.inner = ConnectionState::Closing(active.close());
                }
                ConnectionState::Closing(mut inner) => match inner.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inner = ConnectionState::Closed;
                    }
                    Poll::Ready(Err(e)) => {
                        tracing::warn!("Failure while closing connection: {}", e);
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(inner);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut cleanup) => match cleanup.poll_unpin(cx) {
                    Poll::Ready(reason) => {
                        tracing::warn!("Failure while closing connection: {}", reason);
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Cleanup(cleanup);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closed => {
                    self.inner = ConnectionState::Closed;
                    return Poll::Ready(Ok(()));
                }
                ConnectionState::Poisoned => {
                    unreachable!()
                }
            }
        }
    }
}

impl<T> Drop for Connection<T> {
    fn drop(&mut self) {
        match &mut self.inner {
            ConnectionState::Active(active) => active.drop_all_streams(),
            ConnectionState::Closing(_) => {}
            ConnectionState::Cleanup(_) => {}
            ConnectionState::Closed => {}
            ConnectionState::Poisoned => {}
        }
    }
}

struct Active<T> {
    id: ConnectionId,
    endpoint: Endpoint,
    config: Arc<Config>,
    socket: Fuse<frame::Framed<T>>,
    next_id: u32,

    streams: IntMap<StreamId, Arc<Mutex<stream::Shared>>>,
    stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
    no_streams_waker: Option<Waker>,

    pending_read_frame: Option<Frame>,
    pending_write_frame: Option<Frame>,
    new_outbound_stream_waker: Option<Waker>,
}

impl<T> fmt::Debug for Active<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.id)
            .field("endpoint", &self.endpoint)
            .field("streams", &self.streams.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl<T> fmt::Display for Active<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "(Connection {} {:?} (streams {}))",
            self.id,
            self.endpoint,
            self.streams.len()
        )
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Active<T> {
    fn new(socket: T, config: Config, endpoint: Endpoint) -> Self {
        let id = ConnectionId::random();
        tracing::debug!("Creating new connection: {} (endpoint: {:?})", id, endpoint);
        let socket = frame::Framed::new(id, socket).fuse();
        let next_id = match endpoint {
            Endpoint::Client => 1,
            Endpoint::Server => 2,
        };
        Self {
            id,
            endpoint,
            config: Arc::new(config),
            socket,
            next_id,
            streams: IntMap::default(),
            stream_receivers: SelectAll::new(),
            no_streams_waker: None,
            pending_read_frame: None,
            pending_write_frame: None,
            new_outbound_stream_waker: None,
        }
    }

    fn close(self) -> Closing<T> {
        let pending_frames = self
            .pending_write_frame
            .into_iter()
            .collect::<VecDeque<Frame>>();
        Closing::new(self.stream_receivers, pending_frames, self.socket)
    }

    fn cleanup(mut self, error: ConnectionError) -> Cleanup {
        self.drop_all_streams();
        Cleanup::new(self.stream_receivers, error)
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        loop {
            if self.socket.poll_ready_unpin(cx).is_ready() {
                if let Some(frame) = self
                    .pending_read_frame
                    .take()
                    .or_else(|| self.pending_write_frame.take())
                {
                    self.socket.start_send_unpin(frame)?;
                }
            }
            match self.socket.poll_flush_unpin(cx)? {
                Poll::Ready(()) => {}
                Poll::Pending => {}
            }
            if self.pending_write_frame.is_none() {
                match self.stream_receivers.poll_next_unpin(cx) {
                    Poll::Ready(Some((_, Some(StreamCommand::SendFrame(frame))))) => {
                        tracing::trace!(
                            "Stream({}/{}): sending: {}",
                            self.id,
                            frame.header().stream_id(),
                            frame.header()
                        );
                        self.pending_write_frame.replace(frame);
                        continue;
                    }
                    Poll::Ready(Some((id, Some(StreamCommand::CloseStream { ack })))) => {
                        tracing::trace!("Stream({}/{}): closing", self.id, id);
                        self.pending_write_frame
                            .replace(Frame::new_close_stream(id, ack));
                        continue;
                    }
                    Poll::Ready(Some((id, None))) => {
                        // 关闭流
                        if let Some(frame) = self.on_drop_stream(id) {
                            tracing::trace!(
                                "Stream({}/{}): sending: {}",
                                self.id,
                                id,
                                frame.header()
                            );
                            self.pending_write_frame.replace(frame);
                        }
                        continue;
                    }
                    Poll::Ready(None) => {
                        self.no_streams_waker = Some(cx.waker().clone());
                    }
                    Poll::Pending => {}
                }
            }
            // 轮询底层Socket以接收数据帧

            if self.pending_read_frame.is_none() {
                match self.socket.poll_next_unpin(cx) {
                    Poll::Ready(Some(frame)) => {
                        match self.on_frame(frame?)? {
                            Action::None => {}
                            Action::New(stream) => {
                                tracing::trace!("{}: new stream created", stream);
                                return Poll::Ready(Ok(stream));
                            }
                            Action::Terminate(frame) => {
                                tracing::trace!(
                                    "Connection({}): terminating connection with frame: {}",
                                    self.id,
                                    frame.header()
                                );
                                self.pending_read_frame.replace(frame);
                            }
                        };
                        continue;
                    }
                    Poll::Ready(None) => {
                        return Poll::Ready(Err(ConnectionError::Closed));
                    }
                    Poll::Pending => {}
                }
            }

            return Poll::Pending;
        }
    }

    fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        if self.streams.len() >= self.config.max_active_streams {
            tracing::error!("{}: maximum number of streams reached", self.id);
            return Poll::Ready(Err(ConnectionError::TooManyStreams));
        }
        if self.ack_backlog() >= MAX_ACK_BACKLOG {
            tracing::debug!(
                "{MAX_ACK_BACKLOG} streams waiting for ACK, registering task for wake-up until remote acknowledges at least one stream"
            );
            self.new_outbound_stream_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        tracing::trace!("Connection({}): creating new outbound stream", self.id);

        let id = self.next_stream_id()?;
        let stream = self.make_new_outbound_stream(id);
        tracing::debug!("{}: new outbound stream", stream);
        self.streams.insert(id, stream.clone_shared());
        Poll::Ready(Ok(stream))
    }

    fn make_new_outbound_stream(&mut self, id: StreamId) -> Stream {
        let config = self.config.clone();

        let (sender, receiver) = mpsc::channel(10); // 10 is an arbitrary number.
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }
        Stream::new_outbound(id, self.id, config, sender)
    }

    fn ack_backlog(&mut self) -> usize {
        self.streams
            .iter()
            .filter(|(id, _)| match self.endpoint {
                Endpoint::Client => id.is_client(),
                Endpoint::Server => id.is_server(),
            })
            .filter(|(_, s)| s.lock().is_pending_ack())
            .count()
    }

    fn next_stream_id(&mut self) -> Result<StreamId> {
        let proposed = StreamId::new(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(2)
            .ok_or(ConnectionError::NoMoreStreamIds)?;
        match self.endpoint {
            Endpoint::Client => assert!(proposed.is_client()),
            Endpoint::Server => assert!(proposed.is_server()),
        }
        Ok(proposed)
    }

    fn on_drop_stream(&mut self, stream_id: StreamId) -> Option<Frame> {
        let s = self.streams.remove(&stream_id).expect("stream not found");
        tracing::trace!(
            "Connection({}): removing dropped stream {}",
            self.id,
            stream_id
        );
        let frame = {
            let mut shared = s.lock();
            let frame = match shared.update_state(State::Closed) {
                State::Open { .. } => {
                    let mut header = Header::data(stream_id, 0);
                    header.rst();
                    Some(Frame::new(header))
                }
                State::RecvClosed => {
                    let mut header = Header::data(stream_id, 0);
                    header.fin();
                    Some(Frame::new(header))
                }
                State::SendClosed | State::Closed => None,
            };
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
            frame
        };
        frame
    }

    fn on_frame(&mut self, frame: Frame) -> Result<Action> {
        tracing::trace!(
            "Connection({}): received frame: {}",
            self.id,
            frame.header()
        );
        if frame.header().flags().contains(header::ACK) {
            let id = frame.header().stream_id();
            if let Some(stream) = self.streams.get(&id) {
                stream
                    .lock()
                    .update_state(State::Open { acknowledged: true });
            }
            if let Some(waker) = self.new_outbound_stream_waker.take() {
                waker.wake();
            }
        }
        // 处理终止帧
        if frame.is_termination() {
            return Err(ConnectionError::Closed);
        }
        Ok(self.on_data(frame))
    }

    fn on_data(&mut self, frame: Frame) -> Action {
        let stream_id = frame.header().stream_id();
        // 读取到重置流
        if frame.header().flags().contains(header::RST) {
            if let Some(s) = self.streams.get_mut(&stream_id) {
                let mut shared = s.lock();
                // 强制切换到 Closed 状态
                shared.update_state(State::Closed);
                if let Some(w) = shared.reader.take() {
                    w.wake();
                }
                if let Some(w) = shared.writer.take() {
                    w.wake()
                }
            }
        }

        let is_finish = frame.header().flags().contains(header::FIN);

        if frame.header().flags().contains(header::SYN) {
            if !self.is_valid_remote_id(stream_id) {
                tracing::error!(
                    "Connection({}): invalid remote stream id: {}",
                    self.id,
                    stream_id
                );
                return Action::Terminate(Frame::new_protocol_error());
            }
            // 流已经存在
            if self.streams.contains_key(&stream_id) {
                tracing::error!(
                    "Connection({}): stream {} already exists",
                    self.id,
                    stream_id
                );
                return Action::Terminate(Frame::new_protocol_error());
            }
            // 检查是否超过最大活动流数
            if self.streams.len() == self.config.max_active_streams {
                tracing::warn!("Connection({}): maximum number of streams reached", self.id);
                return Action::Terminate(Frame::new_protocol_error());
            }
            let stream = self.make_new_inbound_stream(stream_id);
            {
                let mut shared = stream.shared();
                if is_finish {
                    shared.update_state(State::RecvClosed);
                }
                //写入首包数据
                shared.buffer.extend(frame.into_body());
            }
            self.streams.insert(stream_id, stream.clone_shared());
            return Action::New(stream);
        }
        if let Some(s) = self.streams.get_mut(&stream_id) {
            let mut shared = s.lock();
            if is_finish {
                shared.update_state(State::RecvClosed);
            }
            shared.buffer.extend(frame.into_body());
            // 唤醒等待读取的任务
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
        } else {
            tracing::warn!(
                "Connection({}): received frame for unknown stream: {}",
                self.id,
                stream_id
            );
        }
        Action::None
    }

    fn make_new_inbound_stream(&mut self, id: StreamId) -> Stream {
        let config = self.config.clone();
        // 缓存10个包
        let (sender, receiver) = mpsc::channel(10);
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }
        Stream::new_inbound(id, self.id, config, sender)
    }

    fn is_valid_remote_id(&self, id: StreamId) -> bool {
        match self.endpoint {
            Endpoint::Client => id.is_server(),
            Endpoint::Server => id.is_client(),
        }
    }
}

impl<T> Active<T> {
    fn drop_all_streams(&mut self) {
        for (_, s) in self.streams.drain() {
            let mut shared = s.lock();
            shared.update_state(State::Closed);
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum Action {
    None,
    New(Stream),
    Terminate(Frame),
}
