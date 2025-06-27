use super::{StreamCommand, stream::TaggedStream};
use crate::{
    ConnectionError, StreamId,
    frame::{Codec, Frame},
};
use asynchronous_codec::Framed;
use futures::{
    AsyncRead, AsyncWrite, SinkExt, StreamExt,
    channel::mpsc,
    ready,
    stream::{Fuse, SelectAll},
};
use std::collections::VecDeque;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

enum State {
    ClosingStreamReceiver,
    DrainingStreamReceiver,
    FlushingPendingFrames,
    ClosingSocket,
}

pub struct Closing<T> {
    state: State,
    stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
    pending_frames: VecDeque<Frame>,
    socket: Fuse<Framed<T, Codec>>,
}

impl<T> Closing<T> {
    pub(crate) fn new(
        stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
        pending_frames: VecDeque<Frame>,
        socket: Fuse<Framed<T, Codec>>,
    ) -> Self {
        Self {
            state: State::ClosingStreamReceiver,
            stream_receivers,
            pending_frames,
            socket,
        }
    }
}

impl<T> Future for Closing<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), ConnectionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match this.state {
                State::ClosingStreamReceiver => {
                    // 关闭所有的写通道
                    this.stream_receivers
                        .iter_mut()
                        .for_each(|s| s.inner_mut().close());
                    this.state = State::DrainingStreamReceiver;
                }
                State::DrainingStreamReceiver => match this.stream_receivers.poll_next_unpin(cx) {
                    Poll::Ready(Some((_, Some(StreamCommand::SendFrame(frame))))) => {
                        this.pending_frames.push_back(frame);
                    }
                    Poll::Ready(Some((id, Some(StreamCommand::CloseStream)))) => {
                        // 关闭流
                        this.pending_frames.push_back(Frame::new_close(id));
                    }
                    Poll::Ready(Some((_, None))) => {}
                    Poll::Pending | Poll::Ready(None) => {
                        this.state = State::FlushingPendingFrames;
                        continue;
                    }
                },
                State::FlushingPendingFrames => {
                    // 发送余包
                    ready!(this.socket.poll_ready_unpin(cx))?;
                    match this.pending_frames.pop_front() {
                        Some(frame) => this.socket.start_send_unpin(frame)?,
                        None => this.state = State::ClosingSocket,
                    }
                }
                State::ClosingSocket => {
                    // 关闭socket
                    ready!(this.socket.poll_close_unpin(cx))?;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
