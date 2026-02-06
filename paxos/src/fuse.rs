use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::FusedStream;
use pin_project_lite::pin_project;

pin_project! {
    /// A stream that yields `None` forever after the underlying stream ends.
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub(crate) struct Fuse<S> {
        #[pin]
        stream: Option<S>,
    }
}

impl<S> Fuse<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self {
            stream: Some(stream),
        }
    }

    pub(crate) fn terminated() -> Self {
        Self { stream: None }
    }
}

impl<S: Stream> Stream for Fuse<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let Some(stream) = this.stream.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };

        match stream.poll_next(cx) {
            Poll::Ready(None) => {
                this.stream.set(None);
                Poll::Ready(None)
            }
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.stream {
            Some(s) => s.size_hint(),
            None => (0, Some(0)),
        }
    }
}

impl<S: Stream> FusedStream for Fuse<S> {
    fn is_terminated(&self) -> bool {
        self.stream.is_none()
    }
}
