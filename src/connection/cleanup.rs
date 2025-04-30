use super::{StreamCommand, stream::TaggedStream};
use crate::{ConnectionError, StreamId};
use futures::{StreamExt, channel::mpsc, stream::SelectAll};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

enum State {
    ClosingStreamReceiver,
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
        let this = self.get_mut();
        loop {
            match this.state {
                State::ClosingStreamReceiver => {
                    this.stream_receivers
                        .iter_mut()
                        .for_each(|s| s.inner_mut().close());
                    this.state = State::DrainingStreamReceiver;
                }
                State::DrainingStreamReceiver => match this.stream_receivers.poll_next_unpin(cx) {
                    Poll::Ready(Some(cmd)) => {
                        drop(cmd);
                    }
                    Poll::Ready(None) | Poll::Pending => {
                        return Poll::Ready(this.error.take().unwrap_or(ConnectionError::Closed));
                    }
                },
            }
        }
    }
}
