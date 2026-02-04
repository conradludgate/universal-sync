//! Bidirectional stream/sink combinators
//!
//! This module provides [`SinkStream`] for combining a sink and stream into
//! a single bidirectional connection, and [`Mapped`] for error mapping.

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, Stream};
use pin_project_lite::pin_project;

pin_project! {
    /// A bidirectional connection combining a sink and stream
    ///
    /// Implements both `Sink<Item>` and `Stream<Item = StreamItem>`.
    pub struct SinkStream<Si, St> {
        #[pin]
        sink: Si,
        #[pin]
        stream: St,
    }
}

impl<Si, St> SinkStream<Si, St> {
    /// Create a new `SinkStream` from a sink and stream
    pub fn new(sink: Si, stream: St) -> Self {
        Self { sink, stream }
    }
}

impl<Si, St> Stream for SinkStream<Si, St>
where
    St: Stream,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

impl<Si, St, I> Sink<I> for SinkStream<Si, St>
where
    Si: Sink<I>,
{
    type Error = Si::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().sink.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().sink.poll_close(cx)
    }
}

pin_project! {
    /// A sink/stream wrapper that maps `io::Error` to a custom error type
    ///
    /// This is useful when working with codecs that return `io::Error` but
    /// you need a different error type for your protocol.
    pub struct Mapped<S, E> {
        #[pin]
        inner: S,
        _error: PhantomData<fn() -> E>,
    }
}

impl<S, E> Mapped<S, E> {
    /// Create a new mapped sink/stream
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _error: PhantomData,
        }
    }
}

impl<S, T, E> Stream for Mapped<S, E>
where
    S: Stream<Item = Result<T, io::Error>>,
    E: From<io::Error>,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().inner.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, I, E> Sink<I> for Mapped<S, E>
where
    S: Sink<I, Error = io::Error>,
    E: From<io::Error>,
{
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(E::from)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().inner.start_send(item).map_err(E::from)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(E::from)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(E::from)
    }
}
