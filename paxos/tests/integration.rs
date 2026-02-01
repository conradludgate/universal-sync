use std::{
    io,
    net::{IpAddr, Ipv4Addr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use basic_paxos::{
    Acceptor, AcceptorMessage, AcceptorRequest, Connector, Learner, Proposal, ProposerConfig,
    SharedAcceptorState, run_acceptor, run_proposer,
};
use futures::{Sink, SinkExt, Stream, channel::mpsc};

// --- Test Proposal Implementation ---

/// Proposal is just metadata (round, attempt) - the actual value is Message
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestProposal {
    round: u64,
    attempt: u64,
}

impl Proposal for TestProposal {
    type RoundId = u64;
    type AttemptId = u64;

    fn round(&self) -> u64 {
        self.round
    }

    fn attempt(&self) -> u64 {
        self.attempt
    }

    fn next_attempt(attempt: u64) -> u64 {
        attempt + 1
    }
}

// --- Test Learner/Acceptor Implementation ---

#[derive(Clone)]
struct TestState {
    round: u64,
    acceptors: Vec<IpAddr>,
    learned: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<Vec<String>>>,
}

impl TestState {
    fn new(acceptors: Vec<IpAddr>) -> Self {
        Self {
            round: 0,
            acceptors,
            learned: Arc::new(Mutex::new(Vec::new())),
            accepted: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Learner for TestState {
    type Proposal = TestProposal;
    type Message = String;
    type Error = io::Error;
    type AcceptorAddr = IpAddr;

    fn current_round(&self) -> u64 {
        self.round
    }

    fn validate(&self, _proposal: &TestProposal) -> bool {
        true
    }

    fn acceptors(&self) -> impl IntoIterator<Item = IpAddr> {
        self.acceptors.clone()
    }

    fn propose(&self, attempt: u64) -> TestProposal {
        TestProposal {
            round: self.round,
            attempt,
        }
    }

    async fn apply(&mut self, _proposal: TestProposal, message: String) -> Result<(), io::Error> {
        self.learned.lock().unwrap().push(message);
        self.round += 1;
        Ok(())
    }
}

impl Acceptor for TestState {
    async fn accept(&mut self, _proposal: TestProposal, message: String) -> Result<(), io::Error> {
        self.accepted.lock().unwrap().push(message);
        Ok(())
    }
}

// --- In-Memory Channel-Based Connection ---

type ProposerSender = mpsc::UnboundedSender<AcceptorRequest<TestState>>;
type ProposerReceiver = mpsc::UnboundedReceiver<AcceptorRequest<TestState>>;
type AcceptorSender = mpsc::UnboundedSender<AcceptorMessage<TestState>>;
type AcceptorReceiver = mpsc::UnboundedReceiver<AcceptorMessage<TestState>>;

fn create_connection_pair() -> (ProposerConn, AcceptorConnWrapper) {
    let (p_tx, p_rx) = mpsc::unbounded::<AcceptorRequest<TestState>>();
    let (a_tx, a_rx) = mpsc::unbounded::<AcceptorMessage<TestState>>();

    (
        ProposerConn { tx: p_tx, rx: a_rx },
        AcceptorConnWrapper { tx: a_tx, rx: p_rx },
    )
}

// Proposer-side connection
struct ProposerConn {
    tx: ProposerSender,
    rx: AcceptorReceiver,
}

impl Stream for ProposerConn {
    type Item = Result<AcceptorMessage<TestState>, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<AcceptorRequest<TestState>> for ProposerConn {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: AcceptorRequest<TestState>,
    ) -> Result<(), Self::Error> {
        self.tx
            .start_send_unpin(item)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

// Acceptor-side connection
struct AcceptorConnWrapper {
    tx: AcceptorSender,
    rx: ProposerReceiver,
}

impl Stream for AcceptorConnWrapper {
    type Item = Result<AcceptorRequest<TestState>, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(Some(msg)) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<AcceptorMessage<TestState>> for AcceptorConnWrapper {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: AcceptorMessage<TestState>,
    ) -> Result<(), Self::Error> {
        self.tx
            .start_send_unpin(item)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

// Channel-based connector that uses pre-established connections
#[derive(Clone)]
struct ChannelConnector {
    connections: Arc<Mutex<Vec<(IpAddr, ProposerConn)>>>,
}

impl Connector<TestState> for ChannelConnector {
    type Connection = ProposerConn;
    type Error = io::Error;
    type ConnectFuture = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ProposerConn, io::Error>> + Send>,
    >;

    fn connect(&mut self, addr: &IpAddr) -> Self::ConnectFuture {
        let connections = self.connections.clone();
        let addr = *addr;
        Box::pin(async move {
            let mut conns = connections.lock().unwrap();
            if let Some(pos) = conns.iter().position(|(a, _)| a == &addr) {
                Ok(conns.remove(pos).1)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no connection for {}", addr),
                ))
            }
        })
    }
}

// --- Tests ---

#[tokio::test]
async fn test_basic_consensus_channels() {
    let acceptor_addrs = vec![
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 3)),
    ];

    let learned = Arc::new(Mutex::new(Vec::<String>::new()));

    // Create connection pairs
    let mut proposer_conns = Vec::new();
    let mut acceptor_handles = Vec::new();

    for addr in &acceptor_addrs {
        let (proposer_conn, acceptor_conn) = create_connection_pair();
        proposer_conns.push((*addr, proposer_conn));

        let state = TestState::new(acceptor_addrs.clone());
        let handle = tokio::spawn(async move {
            let _ = run_acceptor(state, SharedAcceptorState::new(), acceptor_conn).await;
        });
        acceptor_handles.push(handle);
    }

    // Create connector with pre-established connections
    let connector = ChannelConnector {
        connections: Arc::new(Mutex::new(proposer_conns)),
    };

    // Create proposer state
    let mut state = TestState::new(acceptor_addrs);
    state.learned = learned.clone();

    // Create message channel
    let (mut tx, rx) = mpsc::channel::<String>(16);

    // Send a proposal
    tx.send("hello world".to_string()).await.unwrap();
    drop(tx); // Close channel to signal no more messages

    // Run proposer
    run_proposer(state, connector, rx, ProposerConfig::default())
        .await
        .unwrap();

    // Abort acceptor tasks
    for handle in acceptor_handles {
        handle.abort();
    }

    // Check result
    let learned = learned.lock().unwrap();
    assert_eq!(learned.len(), 1);
    assert_eq!(learned[0], "hello world");
}

#[tokio::test]
async fn test_multiple_proposals() {
    let acceptor_addrs = vec![
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 3)),
    ];

    let learned = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut proposer_conns = Vec::new();
    let mut acceptor_handles = Vec::new();

    for addr in &acceptor_addrs {
        let (proposer_conn, acceptor_conn) = create_connection_pair();
        proposer_conns.push((*addr, proposer_conn));

        let state = TestState::new(acceptor_addrs.clone());
        let handle = tokio::spawn(async move {
            let _ = run_acceptor(state, SharedAcceptorState::new(), acceptor_conn).await;
        });
        acceptor_handles.push(handle);
    }

    let connector = ChannelConnector {
        connections: Arc::new(Mutex::new(proposer_conns)),
    };

    let mut state = TestState::new(acceptor_addrs);
    state.learned = learned.clone();

    let (mut tx, rx) = mpsc::channel::<String>(16);

    // Send multiple proposals
    tx.send("first".to_string()).await.unwrap();
    tx.send("second".to_string()).await.unwrap();
    tx.send("third".to_string()).await.unwrap();
    drop(tx);

    run_proposer(state, connector, rx, ProposerConfig::default())
        .await
        .unwrap();

    for handle in acceptor_handles {
        handle.abort();
    }

    let learned = learned.lock().unwrap();
    assert_eq!(learned.len(), 3);
    assert_eq!(learned[0], "first");
    assert_eq!(learned[1], "second");
    assert_eq!(learned[2], "third");
}

#[tokio::test]
async fn test_minority_slow() {
    // Test that consensus works even with one slow acceptor
    use std::time::Duration;

    let acceptor_addrs = vec![
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 3)),
    ];

    let learned = Arc::new(Mutex::new(Vec::<String>::new()));

    let mut proposer_conns = Vec::new();
    let mut acceptor_handles = Vec::new();

    for (i, addr) in acceptor_addrs.iter().enumerate() {
        let (proposer_conn, acceptor_conn) = create_connection_pair();
        proposer_conns.push((*addr, proposer_conn));

        let state = TestState::new(acceptor_addrs.clone());

        if i < 2 {
            // Normal acceptors
            let handle = tokio::spawn(async move {
                let _ = run_acceptor(state, SharedAcceptorState::new(), acceptor_conn).await;
            });
            acceptor_handles.push(handle);
        } else {
            // Slow acceptor - just holds the connection open but processes slowly
            let handle = tokio::spawn(async move {
                // Keep connection alive but don't respond quickly enough
                tokio::time::sleep(Duration::from_secs(100)).await;
                let _ = run_acceptor(state, SharedAcceptorState::new(), acceptor_conn).await;
            });
            acceptor_handles.push(handle);
        }
    }

    let connector = ChannelConnector {
        connections: Arc::new(Mutex::new(proposer_conns)),
    };

    let mut state = TestState::new(acceptor_addrs);
    state.learned = learned.clone();

    let (mut tx, rx) = mpsc::channel::<String>(16);
    tx.send("survives slow".to_string()).await.unwrap();
    drop(tx);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_proposer(state, connector, rx, ProposerConfig::default()),
    )
    .await;

    for handle in acceptor_handles {
        handle.abort();
    }

    match result {
        Ok(Ok(())) => {
            let learned = learned.lock().unwrap();
            assert_eq!(learned.len(), 1);
            assert_eq!(learned[0], "survives slow");
        }
        Ok(Err(e)) => panic!("proposer error: {e:?}"),
        Err(_) => panic!("timeout - consensus should work with 2/3 acceptors responding"),
    }
}
