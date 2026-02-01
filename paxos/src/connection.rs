//! Lazy connection wrapper

use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures::{Sink, Stream};
use tokio::task::coop;

use crate::{AcceptorMessage, AcceptorRequest, Connector, Learner};

/// A lazy connection that establishes the actual connection on first use.
///
/// This implements `Stream + Sink` and will connect during the first
/// `poll_ready()` or `poll_next()` call.
///
/// The connector is responsible for implementing retry logic with backoff.
/// Tracks consecutive failures so callers can check connection health.
pub struct LazyConnection<L: Learner, C: Connector<L>> {
    addr: L::AcceptorAddr,
    connector: C,
    connecting: Option<C::ConnectFuture>,
    connection: Option<C::Connection>,
    /// Count of consecutive connection failures
    consecutive_failures: u32,
}

impl<L: Learner, C: Connector<L>> LazyConnection<L, C> {
    /// Create a new lazy connection to the given address.
    pub fn new(addr: L::AcceptorAddr, connector: C) -> Self {
        Self {
            addr,
            connector,
            connecting: None,
            connection: None,
            consecutive_failures: 0,
        }
    }

    /// Returns the number of consecutive connection failures.
    /// Resets to 0 when a connection succeeds.
    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Returns true if there's an active connection.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }
}

impl<L, C> LazyConnection<L, C>
where
    L: Learner,
    C: Connector<L>,
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
{
    fn poll_inner<R>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        poll_fn: impl FnOnce(Pin<&mut C::Connection>, &mut Context<'_>) -> Poll<R>,
    ) -> Poll<R> {
        let this = self.get_mut();

        loop {
            if let Some(conn) = &mut this.connection {
                return poll_fn(Pin::new(conn), cx);
            }

            if let Some(fut) = &mut this.connecting {
                let coop = ready!(coop::poll_proceed(cx));
                let res = ready!(Pin::new(fut).poll(cx));
                coop.made_progress();

                if let Ok(conn) = res {
                    this.connecting = None;
                    this.connection = Some(conn);
                    this.consecutive_failures = 0;
                    continue;
                }
                this.connecting = None;
                this.consecutive_failures += 1;
                // Try again - connector handles backoff
            }

            // Start a new connection attempt
            // The connector is responsible for backoff and returning an error when giving up
            this.connecting = Some(this.connector.connect(&this.addr));
        }
    }
}

impl<L, C> Stream for LazyConnection<L, C>
where
    L: Learner,
    C: Connector<L>,
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
{
    type Item = Result<AcceptorMessage<L>, L::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx, futures::Stream::poll_next)
    }
}

impl<L, C> Sink<AcceptorRequest<L>> for LazyConnection<L, C>
where
    L: Learner,
    C: Connector<L>,
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
{
    type Error = L::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_inner(cx, futures::Sink::poll_ready)
    }

    fn start_send(self: Pin<&mut Self>, item: AcceptorRequest<L>) -> Result<(), Self::Error> {
        let conn = self
            .get_mut()
            .connection
            .as_mut()
            .expect("start_send called before poll_ready returned Ready");
        Pin::new(conn).start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Some(conn) = &mut self.get_mut().connection else {
            return Poll::Ready(Ok(()));
        };
        Pin::new(conn).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Some(conn) = &mut self.get_mut().connection else {
            return Poll::Ready(Ok(()));
        };
        Pin::new(conn).poll_close(cx)
    }
}

impl<L: Learner, C: Connector<L>> Unpin for LazyConnection<L, C>
where
    C::ConnectFuture: Unpin,
    C::Connection: Unpin,
{
}
