use crate::{ConnectionError, StreamId, connection::StreamCommand, tagged_stream::TaggedStream};
use futures::{StreamExt, channel::mpsc, stream::SelectAll};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

enum State {
    /// 关闭stream接收
    ClosingStreamReceiver,
    /// 排干stream接收
    DrainingStreamReceiver,
}

pub struct Cleanup {
    state: State,
    stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
    error: Option<ConnectionError>,
}

impl Cleanup {
    pub(crate) fn new(
        stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
        error: ConnectionError,
    ) -> Self {
        Self {
            state: State::ClosingStreamReceiver,
            stream_receivers,
            error: Some(error),
        }
    }
}

impl Future for Cleanup {
    type Output = ConnectionError;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 获取Pin inner可变引用
        let this = self.get_mut();
        loop {
            match this.state {
                State::ClosingStreamReceiver => {
                    for stream in this.stream_receivers.iter_mut() {
                        stream.inner_mut().close();
                    }
                    // 切换到 排干接收器
                    this.state = State::DrainingStreamReceiver;
                }
                State::DrainingStreamReceiver => match this.stream_receivers.poll_next_unpin(cx) {
                    Poll::Ready(Some(cmd)) => {
                        // drop send command
                        drop(cmd);
                    }
                    Poll::Ready(None) | Poll::Pending => {
                        // 返回错误
                        return Poll::Ready(
                            this.error
                                .take()
                                .expect("to not be called after completion"),
                        );
                    }
                },
            }
        }
    }
}
