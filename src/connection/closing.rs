use crate::{Result, StreamId, connection::StreamCommand, frame, tagged_stream::TaggedStream};
use futures::{
    AsyncRead, AsyncWrite, SinkExt, StreamExt,
    channel::mpsc,
    ready,
    stream::{Fuse, SelectAll},
};

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

enum State {
    /// 关闭Stream接收器
    ClosingStreamReceiver,
    /// 排干Stream接收器
    DrainingStreamReceiver,
    /// 刷新待处理的帧
    FlushingPendingFrames,
    /// 关闭Socket
    ClosingSocket,
}

pub struct Closing<T> {
    state: State,
    stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
    pending_frames: VecDeque<frame::Frame>,
    socket: Fuse<frame::Framed<T>>,
}

impl<T> Closing<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
        pending_frames: VecDeque<frame::Frame>,
        socket: Fuse<frame::Framed<T>>,
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
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match this.state {
                State::ClosingStreamReceiver => {
                    for stream in this.stream_receivers.iter_mut() {
                        stream.inner_mut().close();
                    }
                    this.state = State::DrainingStreamReceiver;
                }
                State::DrainingStreamReceiver => match this.stream_receivers.poll_next_unpin(cx) {
                    Poll::Ready(Some((_, Some(StreamCommand::SendFrame(frame))))) => {
                        this.pending_frames.push_back(frame);
                    }
                    Poll::Ready(Some((id, Some(StreamCommand::CloseStream { ack })))) => {
                        this.pending_frames
                            .push_back(frame::Frame::new_close_stream(id, ack));
                    }
                    Poll::Ready(Some((_, None))) => {}
                    Poll::Pending | Poll::Ready(None) => {
                        this.state = State::FlushingPendingFrames;
                        continue;
                    }
                },
                State::FlushingPendingFrames => {
                    ready!(this.socket.poll_ready_unpin(cx))?;
                    match this.pending_frames.pop_front() {
                        Some(frame) => this.socket.start_send_unpin(frame)?,
                        None => this.state = State::ClosingSocket,
                    }
                }
                State::ClosingSocket => {
                    // 关闭底层Socket
                    ready!(this.socket.poll_close_unpin(cx))?;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
