use crate::{
    Config, ConnectionError, Result, StreamId,
    frame::{Codec, Flags, Frame, Header},
};
use asynchronous_codec::Framed;
use bytes::Bytes;
use cleanup::Cleanup;
use closing::Closing;
use futures::{
    AsyncRead, AsyncWrite, FutureExt, SinkExt, StreamExt,
    channel::mpsc,
    stream::{Fuse, SelectAll},
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, atomic::AtomicUsize},
    task::{Context, Poll, Waker},
};
use stream::{Shared, State, TaggedStream};
use tracing::{debug, error, trace, warn};

mod cleanup;
mod closing;
mod stream;

pub use stream::Stream;

#[allow(dead_code)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Endpoint {
    Client,
    Server,
}

#[allow(dead_code)]
const NEXT_CONNECTION_ID: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone, Copy)]
pub(crate) struct ConnectionId(usize);

impl ConnectionId {
    pub(crate) fn new() -> Self {
        Self(NEXT_CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

impl fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

#[derive(Debug)]
pub(crate) enum StreamCommand {
    SendFrame(Frame),
    CloseStream { ack: bool },
}

#[derive(Debug)]
pub(crate) enum Action {
    None,
    New(Stream),
    Terminate,
}

enum ConnectionState<T> {
    Active(Active<T>),
    Closing(Closing<T>),
    Closed,
    Cleanup(Cleanup),
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
    pub fn new(socket: T, endpoint: Endpoint, config: Config) -> Self {
        Self {
            inner: ConnectionState::Active(Active::new(socket, endpoint, config)),
        }
    }

    pub fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
                ConnectionState::Active(mut active) => match active.poll_new_outbound(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Ready(Ok(stream));
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
                        return Poll::Ready(Err(ConnectionError::Closed));
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(closing);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut cleanup) => match cleanup.poll_unpin(cx) {
                    Poll::Ready(e) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Cleanup(cleanup);
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
                        // 关闭连接成功
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
                        // 关闭连接成功
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(e) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Some(Err(e)));
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
                ConnectionState::Closing(mut closing) => match closing.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => {
                        warn!("Failure while closing connection: {}", e);
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(closing);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut cleanup) => match cleanup.poll_unpin(cx) {
                    Poll::Ready(e) => {
                        warn!("Failure while closing connection: {}", e);
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
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
                ConnectionState::Poisoned => unreachable!(),
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
    next_id: u32,
    socket: Fuse<Framed<T, Codec>>,
    streams: HashMap<StreamId, Arc<Mutex<Shared>>>,
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

impl<T> Active<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn new(socket: T, endpoint: Endpoint, config: Config) -> Self {
        let id = ConnectionId::new();
        let socket = Framed::new(socket, Codec::new(config.max_frame_size)).fuse();
        let next_id = if endpoint == Endpoint::Client { 1 } else { 2 };
        Self {
            id,
            endpoint,
            config: Arc::new(config),
            next_id,
            socket,
            streams: HashMap::new(),
            stream_receivers: SelectAll::default(),
            no_streams_waker: None,
            pending_read_frame: None,
            pending_write_frame: None,
            new_outbound_stream_waker: None,
        }
    }

    fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        if self.streams.len() >= self.config.max_num_streams {
            error!(
                "{}: maximum({}) number of streams reached",
                self.id, self.config.max_num_streams
            );
            return Poll::Ready(Err(ConnectionError::TooManyStreams));
        }
        if self.ack_backlog() >= self.config.max_ack_backlog {
            debug!(
                "{}: streams({}) waiting for ACK, task for wake-up",
                self.id, self.config.max_ack_backlog
            );
            self.new_outbound_stream_waker = Some(cx.waker().clone());
            return Poll::Ready(Err(ConnectionError::TooManyStreams));
        }
        trace!("{}: creating new outbound stream", self.id);
        let id = self.next_stream_id()?;
        let stream = self.make_new_outbound_stream(id);
        debug!("{}: new outbound {} of {}", self.id, id, self);
        Poll::Ready(Ok(stream))
    }

    fn make_new_outbound_stream(&mut self, id: StreamId) -> Stream {
        let config = self.config.clone();
        let (sender, receiver) = mpsc::channel(10);
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }
        Stream::new_outbound(id, self.id, config, sender)
    }

    fn make_new_inbound_stream(&mut self, id: StreamId) -> Stream {
        let config = self.config.clone();
        let (sender, receiver) = mpsc::channel(10);
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }
        Stream::new_inbound(id, self.id, config, sender)
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

    fn is_valid_remote_id(&self, id: StreamId) -> bool {
        match self.endpoint {
            Endpoint::Client => id.is_server(),
            Endpoint::Server => id.is_client(),
        }
    }

    fn close(self) -> Closing<T> {
        let pending_frames = self
            .pending_read_frame
            .into_iter()
            .chain(self.pending_write_frame)
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
                //Socket is ready to send
                if let Some(frame) = self
                    .pending_read_frame
                    .take()
                    .or_else(|| self.pending_write_frame.take())
                {
                    self.socket.start_send_unpin(frame)?;
                    continue;
                }
            }
            // Flush the socket
            match self.socket.poll_flush_unpin(cx)? {
                Poll::Ready(()) => {}
                Poll::Pending => {}
            }

            if self.pending_write_frame.is_none() {
                match self.stream_receivers.poll_next_unpin(cx) {
                    Poll::Ready(Some((_, Some(StreamCommand::SendFrame(frame))))) => {
                        trace!("{}/{}: sending frame", self.id, frame.header().stream_id());
                        self.pending_write_frame.replace(frame);
                        continue;
                    }
                    Poll::Ready(Some((id, Some(StreamCommand::CloseStream { ack })))) => {
                        trace!("{}/{}: sending close", self.id, id);
                        self.pending_write_frame.replace(Frame::new_close(id, ack));
                    }
                    Poll::Ready(Some((id, None))) => {
                        if let Some(frame) = self.on_drop_stream(id) {
                            self.pending_write_frame.replace(frame);
                        }
                        continue;
                    }
                    Poll::Ready(None) => {
                        self.new_outbound_stream_waker = Some(cx.waker().clone());
                    }
                    Poll::Pending => {}
                }
            }

            if self.pending_read_frame.is_none() {
                match self.socket.poll_next_unpin(cx) {
                    Poll::Ready(Some(frame)) => match self.on_frame(frame?)? {
                        Action::New(stream) => {
                            trace!("{}: new inbound {} of {}", self.id, stream, self);
                            return Poll::Ready(Ok(stream));
                        }
                        Action::Terminate => {
                            trace!("{}: sending close", self.id);
                            return Poll::Ready(Err(ConnectionError::Closed));
                        }
                        Action::None => {}
                    },
                    Poll::Ready(None) => {
                        return Poll::Ready(Err(ConnectionError::Closed));
                    }
                    Poll::Pending => {}
                }
            }
            return Poll::Pending;
        }
    }

    fn on_frame(&mut self, frame: Frame) -> Result<Action> {
        trace!("{}: received: {}", self.id, frame.header());
        let stream_id = frame.header().stream_id();
        if frame.header().flags().contains(Flags::ACK) {
            // ACK Frame
            if let Some(stream) = self.streams.get(&stream_id) {
                stream
                    .lock()
                    .update_state(self.id, stream_id, State::Open { ack: true });
            }
            if let Some(waker) = self.new_outbound_stream_waker.take() {
                waker.wake();
            }
        }
        if frame.header().flags().contains(Flags::RST) {
            if let Some(s) = self.streams.get_mut(&stream_id) {
                let mut shared = s.lock();
                shared.update_state(self.id, stream_id, State::Closed);
                if let Some(w) = shared.reader.take() {
                    w.wake()
                }
                if let Some(w) = shared.writer.take() {
                    w.wake()
                }
            }
            return Ok(Action::None);
        }
        let is_finish = frame.header().flags().contains(Flags::FIN); // 流半关闭
        if frame.header().flags().contains(Flags::SYN) {
            // 新建流
            if !self.is_valid_remote_id(stream_id) {
                error!("{}: invalid stream id {}", self.id, stream_id);
                return Ok(Action::Terminate);
            }
            if self.streams.contains_key(&stream_id) {
                error!("{}: stream {} already exists", self.id, stream_id);
                return Ok(Action::Terminate);
            }
            if self.streams.len() == self.config.max_num_streams {
                error!(
                    "{}: maximum({}) number of streams reached",
                    self.id, self.config.max_num_streams
                );
                return Ok(Action::Terminate);
            }
            let stream = self.make_new_inbound_stream(stream_id);
            {
                let mut shared = stream.shared();
                if is_finish {
                    shared.update_state(self.id, stream_id, State::RecvClosed);
                }
                shared.buffer.extend_from_slice(&frame.into_data()[..]);
            };
            self.streams.insert(stream_id, stream.clone_shared());
            return Ok(Action::New(stream));
        }
        if let Some(s) = self.streams.get_mut(&stream_id) {
            let mut shared = s.lock();
            if is_finish {
                shared.update_state(self.id, stream_id, State::RecvClosed);
            }
            shared.buffer.extend_from_slice(&frame.into_data()[..]);
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
        } else {
            trace!(
                "{}/{}: data frame for unknown stream, possibly dropped earlier: {:?}",
                self.id, stream_id, frame
            );
        }
        Ok(Action::None)
    }

    fn on_drop_stream(&mut self, stream_id: StreamId) -> Option<Frame> {
        let s = self.streams.get(&stream_id).expect("stream should exist");
        trace!("{}: dropping stream {}", self.id, stream_id);
        let mut shared = s.lock();
        let state = shared.update_state(self.id, stream_id, State::Closed);
        match state {
            State::Open { .. } => {
                let mut header = Header::new(stream_id, 0);
                header.rst();
                Some(Frame::new(header, Bytes::new()))
            }
            State::RecvClosed => {
                let mut header = Header::new(stream_id, 0);
                header.fin();
                Some(Frame::new(header, Bytes::new()))
            }
            State::SendClosed | State::Closed => None,
        }
    }
}

impl<T> Active<T> {
    fn drop_all_streams(&mut self) {
        self.streams.drain().for_each(|(sid, s)| {
            let mut shared = s.lock();
            shared.update_state(self.id, sid, State::Closed);
            if let Some(waker) = shared.reader.take() {
                waker.wake();
            }
            if let Some(waker) = shared.writer.take() {
                waker.wake();
            }
        });
    }
}
