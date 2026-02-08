//! Bidirectional stream/sink combinators.

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use error_stack::Report;
use futures::{Sink, Stream};
use pin_project_lite::pin_project;

/// Like `From<io::Error>` but implementable for foreign types (e.g. `Report<E>`).
pub trait FromIoError {
    fn from_io_error(err: io::Error) -> Self;
}

impl<E: std::error::Error + Send + Sync + Default + 'static> FromIoError for Report<E> {
    fn from_io_error(err: io::Error) -> Self {
        Report::new(err).change_context(E::default())
    }
}

pin_project! {
    /// Combines a `Sink` and `Stream` into a single bidirectional type.
    pub struct SinkStream<Si, St> {
        #[pin]
        sink: Si,
        #[pin]
        stream: St,
    }
}

impl<Si, St> SinkStream<Si, St> {
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
    /// Maps `io::Error` to a custom error type via [`FromIoError`].
    pub struct Mapped<S, E> {
        #[pin]
        inner: S,
        _error: PhantomData<fn() -> E>,
    }
}

impl<S, E> Mapped<S, E> {
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
    E: FromIoError,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project().inner.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(E::from_io_error(e)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, I, E> Sink<I> for Mapped<S, E>
where
    S: Sink<I, Error = io::Error>,
    E: FromIoError,
{
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_ready(cx)
            .map_err(E::from_io_error)
    }

    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(item)
            .map_err(E::from_io_error)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(E::from_io_error)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .inner
            .poll_close(cx)
            .map_err(E::from_io_error)
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;
    use futures::{SinkExt, StreamExt};

    use super::*;

    #[tokio::test]
    async fn sink_stream_close() {
        let (tx, mut rx) = mpsc::channel::<u32>(8);
        let stream = futures::stream::empty::<u32>();
        let mut ss = SinkStream::new(tx, stream);

        ss.send(1).await.unwrap();
        ss.close().await.unwrap();

        assert_eq!(rx.next().await, Some(1));
        assert_eq!(rx.next().await, None);
    }

    #[tokio::test]
    async fn mapped_sink_close() {
        let (tx, mut rx) = mpsc::unbounded::<u32>();

        let io_sink = IoSink(tx);
        let stream = futures::stream::empty::<Result<u32, io::Error>>();
        let io_sink_stream = SinkStream::new(io_sink, stream);
        let mut mapped: Mapped<_, Report<ConnectorError>> = Mapped::new(io_sink_stream);

        mapped.send(42).await.unwrap();
        mapped.close().await.unwrap();

        assert_eq!(rx.next().await, Some(42));
        assert_eq!(rx.next().await, None);
    }

    #[tokio::test]
    async fn mapped_stream_error_conversion() {
        let err_stream = futures::stream::iter(vec![Err::<u32, io::Error>(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "test",
        ))]);
        let sink = IoSink(mpsc::unbounded::<u32>().0);
        let ss = SinkStream::new(sink, err_stream);
        let mut mapped: Mapped<_, Report<ConnectorError>> = Mapped::new(ss);
        let item = mapped.next().await.unwrap();
        assert!(item.is_err());
    }

    use std::marker::Unpin;

    struct IoSink<T>(mpsc::UnboundedSender<T>);
    impl<T> Unpin for IoSink<T> {}

    impl<T> Sink<T> for IoSink<T> {
        type Error = io::Error;
        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
            self.0
                .unbounded_send(item)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "closed"))
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.0.close_channel();
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug, Default)]
    struct ConnectorError;
    impl std::fmt::Display for ConnectorError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("test")
        }
    }
    impl std::error::Error for ConnectorError {}
}
