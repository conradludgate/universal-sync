//! Turmoil-based simulation tests for Paxos
//!
//! These tests use turmoil's network simulation for more realistic
//! distributed systems testing with network partitions, latency, etc.

use std::{
    collections::BTreeMap,
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use basic_paxos::{
    Acceptor, AcceptorMessage, AcceptorRequest, BackoffConfig, Connector, Learner, LearnerSession,
    Proposal, ProposerConfig, SharedAcceptorState, Sleep, run_acceptor, run_learner, run_proposer,
};
use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, Stream, channel::mpsc};
use rand::rngs::StdRng;
use tokio_util::codec::{Decoder, Encoder, Framed};
use turmoil::Builder;

/// Initialize tracing for tests. Call at the start of each test.
/// Uses RUST_LOG env var for filtering (defaults to "debug" for this crate).
fn init_tracing() -> impl Sized {
    use tracing::Dispatch;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{EnvFilter, fmt};

    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("basic_paxos=debug")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_test_writer()
        .finish();

    // Use registry and set as the default for this thread only,
    // using tracing::dispatcher::set_global_default will set for the whole process (not wanted).
    // Instead, use set_default in a scope, but make it a no-op closure to persist it for this thread.
    let dispatch = Dispatch::new(subscriber);
    tracing::dispatcher::set_default(&dispatch)
}

const ACCEPTOR_PORT: u16 = 9999;

// --- Turmoil Sleep Implementation ---

#[derive(Clone, Copy, Default)]
struct TurmoilSleep;

impl Sleep for TurmoilSleep {
    async fn sleep(&self, duration: Duration) {
        // Turmoil intercepts tokio::time, so we use tokio's sleep
        tokio::time::sleep(duration).await;
    }
}

/// Create proposer config A with a seeded RNG for deterministic jitter
fn turmoil_config_a(seed: u64) -> ProposerConfig<TurmoilSleep, StdRng> {
    ProposerConfig::with_seed(
        BackoffConfig {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(200),
            multiplier: 2.0,
        },
        TurmoilSleep,
        seed,
    )
}

/// Create proposer config B with different seed for asymmetric behavior
fn turmoil_config_b(seed: u64) -> ProposerConfig<TurmoilSleep, StdRng> {
    ProposerConfig::with_seed(
        BackoffConfig {
            initial: Duration::from_millis(15), // Slightly different
            max: Duration::from_millis(250),
            multiplier: 2.0,
        },
        TurmoilSleep,
        seed,
    )
}

/// Default config with seed 0
fn turmoil_config() -> ProposerConfig<TurmoilSleep, StdRng> {
    turmoil_config_a(0)
}

// --- Test Proposal Implementation ---

/// Proposal is just metadata (node_id, round, attempt) - the actual value is Message
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct TestProposal {
    node_id: SocketAddr,
    round: u64,
    attempt: u64,
}

impl Proposal for TestProposal {
    type NodeId = SocketAddr;
    type RoundId = u64;
    type AttemptId = u64;

    fn node_id(&self) -> SocketAddr {
        self.node_id
    }

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

/// Learned entry containing both proposal metadata and message
#[derive(Debug, Clone, PartialEq, Eq)]
struct LearnedEntry {
    proposal: TestProposal,
    message: String,
}

/// Shared learned state using BTreeMap to prevent duplicates.
/// Key is round, value is the (proposal, message) that was learned for that round.
type SharedLearned = Arc<Mutex<BTreeMap<u64, LearnedEntry>>>;

/// Stores full proposals for thorough testing
#[derive(Clone)]
struct TestState {
    node_id: SocketAddr,
    acceptors: Vec<SocketAddr>,
    /// Full proposals learned, keyed by round to prevent duplicates
    learned: SharedLearned,
}

impl TestState {
    fn new(node_id: SocketAddr, acceptors: Vec<SocketAddr>) -> Self {
        Self {
            node_id,
            acceptors,
            learned: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl Learner for TestState {
    type Proposal = TestProposal;
    type Message = String;
    type Error = io::Error;

    fn node_id(&self) -> SocketAddr {
        self.node_id
    }

    fn current_round(&self) -> u64 {
        // Derive from shared state to stay synchronized across proposers
        self.learned.lock().unwrap().len() as u64
    }

    fn validate(&self, _proposal: &TestProposal) -> bool {
        true
    }

    fn acceptors(&self) -> impl IntoIterator<Item = SocketAddr> {
        self.acceptors.clone()
    }

    fn propose(&self, attempt: u64) -> TestProposal {
        TestProposal {
            node_id: self.node_id,
            round: self.current_round(),
            attempt,
        }
    }

    async fn apply(&mut self, proposal: TestProposal, message: String) -> Result<(), io::Error> {
        // Only insert if not already learned (prevents duplicates with shared state)
        let round = proposal.round;
        self.learned
            .lock()
            .unwrap()
            .entry(round)
            .or_insert(LearnedEntry { proposal, message });
        Ok(())
    }
}

impl Acceptor for TestState {
    async fn accept(&mut self, _proposal: TestProposal, _message: String) -> Result<(), io::Error> {
        Ok(())
    }
}

// --- Retry Stream for Multi-Round Tests ---

/// A message stream that keeps producing messages until target_rounds are learned.
/// This allows proposers to retry until they eventually succeed.
///
/// Key design: the message for each round is DETERMINISTIC. This ensures that
/// when a proposer retries a round after rejection, it uses the same message
/// value, which is essential for Paxos correctness.
struct RetryMessageStream {
    proposer_id: String,
    target_rounds: usize,
    learned: SharedLearned,
}

impl RetryMessageStream {
    fn new(proposer_id: String, target_rounds: usize, learned: SharedLearned) -> Self {
        Self {
            proposer_id,
            target_rounds,
            learned,
        }
    }
}

impl Stream for RetryMessageStream {
    type Item = String;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let learned_count = self.learned.lock().unwrap().len();

        // If we've learned enough rounds, we're done
        if learned_count >= self.target_rounds {
            return Poll::Ready(None);
        }

        // The round we're trying to commit
        let current_round = learned_count;

        // Deterministic message per round - same message on retries
        // This is crucial: on rejection, the proposer will get the same message
        // value again, just with a higher attempt number
        let msg = format!("{}-round{}", self.proposer_id, current_round);
        Poll::Ready(Some(msg))
    }
}

// --- Postcard-based Codec ---

/// Generic length-prefixed postcard codec
struct PostcardCodec<Enc, Dec>(std::marker::PhantomData<(Enc, Dec)>);

impl<Enc, Dec> Default for PostcardCodec<Enc, Dec> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Enc: serde::Serialize, Dec> Encoder<Enc> for PostcardCodec<Enc, Dec> {
    type Error = io::Error;

    fn encode(&mut self, item: Enc, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded =
            postcard::to_allocvec(&item).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        dst.put_u32_le(encoded.len() as u32);
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

impl<Enc, Dec: serde::de::DeserializeOwned> Decoder for PostcardCodec<Enc, Dec> {
    type Item = Dec;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_le_bytes(src[..4].try_into().unwrap()) as usize;
        if src.len() < 4 + len {
            return Ok(None);
        }
        src.advance(4);
        let data = src.split_to(len);
        let item = postcard::from_bytes(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(item))
    }
}

/// Codec for proposer-side: encodes AcceptorRequest, decodes AcceptorMessage
type ProposerCodec = PostcardCodec<AcceptorRequest<TestState>, AcceptorMessage<TestState>>;

/// Codec for acceptor-side: encodes AcceptorMessage, decodes AcceptorRequest
type AcceptorCodec = PostcardCodec<AcceptorMessage<TestState>, AcceptorRequest<TestState>>;

// --- Connection Types ---

type ProposerConn = Framed<turmoil::net::TcpStream, ProposerCodec>;

/// TCP connector with exponential backoff on connection failures.
#[derive(Clone)]
struct TcpConnector {
    current_delay: Duration,
    attempts: u32,
    max_attempts: u32,
}

impl TcpConnector {
    const BASE_DELAY: Duration = Duration::from_millis(50);
    const MAX_DELAY: Duration = Duration::from_secs(5);

    fn new() -> Self {
        Self {
            current_delay: Self::BASE_DELAY,
            attempts: 0,
            max_attempts: 10,
        }
    }
}

impl Connector<TestState> for TcpConnector {
    type Connection = ProposerConn;
    type Error = io::Error;
    type ConnectFuture = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ProposerConn, io::Error>> + Send>,
    >;

    fn connect(&mut self, addr: &SocketAddr) -> Self::ConnectFuture {
        let addr = *addr;
        let delay = self.current_delay;
        let attempts = self.attempts;
        let max_attempts = self.max_attempts;

        // Increase delay and attempts for next call
        self.current_delay = (self.current_delay * 2).min(Self::MAX_DELAY);
        self.attempts += 1;

        Box::pin(async move {
            // Check if we've exceeded max attempts
            if attempts >= max_attempts {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("failed to connect to {} after {} attempts", addr, attempts),
                ));
            }

            // Wait before attempting connection (backoff)
            if delay > Self::BASE_DELAY {
                tokio::time::sleep(delay).await;
            }
            let stream = turmoil::net::TcpStream::connect(addr).await?;
            Ok(Framed::new(stream, ProposerCodec::default()))
        })
    }
}

/// Convert hostnames to SocketAddrs using turmoil's DNS lookup
fn resolve_acceptors(names: &[&str]) -> Vec<SocketAddr> {
    names
        .iter()
        .map(|name| SocketAddr::new(turmoil::lookup(*name), ACCEPTOR_PORT))
        .collect()
}

/// Get a unique node ID for a proposer (uses port 0 to distinguish from acceptors)
fn proposer_node_id(name: &str) -> SocketAddr {
    SocketAddr::new(turmoil::lookup(name), 0)
}

/// Get a node ID for an acceptor host
fn acceptor_node_id(name: &str) -> SocketAddr {
    SocketAddr::new(turmoil::lookup(name), ACCEPTOR_PORT)
}

// --- Turmoil Tests ---

fn start_acceptor(
    sim: &mut turmoil::Sim<'_>,
    name: &'static str,
    acceptor_names: &'static [&'static str],
) {
    sim.host(name, move || async move {
        let my_node_id = acceptor_node_id(name);
        let listener =
            turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, ACCEPTOR_PORT)).await?;
        let acceptor_addrs = resolve_acceptors(acceptor_names);
        let state = TestState::new(my_node_id, acceptor_addrs);
        let shared = SharedAcceptorState::new();
        loop {
            let (stream, _) = listener.accept().await?;
            let conn = Framed::new(stream, AcceptorCodec::default());
            let state = state.clone();
            let shared = shared.clone();
            tokio::spawn(async move {
                let _ = run_acceptor(state, shared, conn).await;
            });
        }
    });
}

#[test]
fn turmoil_basic_consensus() {
    let _guard = init_tracing();
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(55))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];
    let learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));

    // Start acceptors
    for name in ACCEPTOR_NAMES {
        start_acceptor(&mut sim, name, ACCEPTOR_NAMES);
    }

    // Start proposer as a client
    let learned_clone = learned.clone();
    sim.client("proposer", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        tx.send("hello turmoil".to_string()).await.unwrap();
        drop(tx);

        run_proposer(state, TcpConnector::new(), rx, turmoil_config())
            .await
            .map_err(|e| {
                Box::new(io::Error::other(format!("{e:?}"))) as Box<dyn std::error::Error>
            })?;
        Ok(())
    });

    // Run simulation
    sim.run().unwrap();

    // Verify
    let learned = learned.lock().unwrap();
    assert_eq!(learned.len(), 1);
    let round0 = learned.get(&0).expect("should have round 0");
    assert_eq!(round0.proposal.round, 0);
    assert_eq!(round0.message, "hello turmoil");
}

#[test]
fn turmoil_multiple_messages() {
    let _guard = init_tracing();
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];
    let learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));

    for name in ACCEPTOR_NAMES {
        start_acceptor(&mut sim, name, ACCEPTOR_NAMES);
    }

    let learned_clone = learned.clone();
    sim.client("proposer", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        tx.send("first".to_string()).await.unwrap();
        tx.send("second".to_string()).await.unwrap();
        tx.send("third".to_string()).await.unwrap();
        drop(tx);

        run_proposer(state, TcpConnector::new(), rx, turmoil_config())
            .await
            .map_err(|e| {
                Box::new(io::Error::other(format!("{e:?}"))) as Box<dyn std::error::Error>
            })?;
        Ok(())
    });

    sim.run().unwrap();

    let learned = learned.lock().unwrap();
    assert_eq!(learned.len(), 3);
    assert_eq!(learned.get(&0).unwrap().message, "first");
    assert_eq!(learned.get(&1).unwrap().message, "second");
    assert_eq!(learned.get(&2).unwrap().message, "third");
}

#[test]
fn turmoil_with_latency() {
    let _guard = init_tracing();
    // Test with variable message latency
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .min_message_latency(Duration::from_millis(10))
        .max_message_latency(Duration::from_millis(100))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];
    let learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));

    for name in ACCEPTOR_NAMES {
        start_acceptor(&mut sim, name, ACCEPTOR_NAMES);
    }

    let learned_clone = learned.clone();
    sim.client("proposer", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        tx.send("with latency".to_string()).await.unwrap();
        drop(tx);

        run_proposer(state, TcpConnector::new(), rx, turmoil_config())
            .await
            .map_err(|e| {
                Box::new(io::Error::other(format!("{e:?}"))) as Box<dyn std::error::Error>
            })?;
        Ok(())
    });

    sim.run().unwrap();

    let learned = learned.lock().unwrap();
    assert_eq!(learned.len(), 1);
    assert_eq!(learned.get(&0).unwrap().message, "with latency");
}

#[test]
fn turmoil_competing_proposers() {
    let _guard = init_tracing();
    // Two proposers competing for multiple rounds.
    // They SHARE the learned state (simulating a shared database or same-node scenario).
    // This ensures they stay synchronized on which round to propose for.
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(10))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];
    const TARGET_ROUNDS: usize = 3;

    // SHARED learned state between both proposers
    let shared_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));

    for name in ACCEPTOR_NAMES {
        start_acceptor(&mut sim, name, ACCEPTOR_NAMES);
    }

    // Proposer A - shares learned state, starts immediately
    let learned_a = shared_learned.clone();
    sim.client("proposer-a", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer-a"), acceptor_addrs);
        state.learned = learned_a.clone();

        let stream = RetryMessageStream::new("A".to_string(), TARGET_ROUNDS, learned_a);

        let _ = run_proposer(state, TcpConnector::new(), stream, turmoil_config_a(42)).await;
        Ok(())
    });

    // Proposer B - shares the SAME learned state, starts with a small delay
    let learned_b = shared_learned.clone();
    sim.client("proposer-b", async move {
        // Small delay to break symmetry
        tokio::time::sleep(Duration::from_millis(5)).await;

        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer-b"), acceptor_addrs);
        state.learned = learned_b.clone();

        let stream = RetryMessageStream::new("B".to_string(), TARGET_ROUNDS, learned_b);

        let _ = run_proposer(state, TcpConnector::new(), stream, turmoil_config_b(123)).await;
        Ok(())
    });

    sim.run().unwrap();

    let learned = shared_learned.lock().unwrap();

    println!("Shared state learned {} rounds:", learned.len());
    for (round, entry) in learned.iter() {
        println!(
            "  Round {round}: attempt={}, message={}",
            entry.proposal.attempt, entry.message
        );
    }

    // Should have learned at least one round (ideally all, but contention may cause timeouts)
    assert!(
        !learned.is_empty(),
        "Should learn at least one round, got 0"
    );

    // Ideally we learn all rounds, but with contention we might time out
    if learned.len() < TARGET_ROUNDS {
        panic!(
            "Note: Only learned {} of {} rounds (contention caused timeout)",
            learned.len(),
            TARGET_ROUNDS
        );
    }

    // Verify round ordering for rounds that were learned
    for (round, entry) in learned.iter() {
        assert_eq!(
            entry.proposal.round, *round,
            "Round mismatch in learned state"
        );
    }

    // Count how many rounds each proposer won
    let a_wins = learned
        .values()
        .filter(|e| e.message.starts_with("A-"))
        .count();
    let b_wins = learned
        .values()
        .filter(|e| e.message.starts_with("B-"))
        .count();
    println!("Proposer A won {a_wins} rounds, Proposer B won {b_wins} rounds");
}

#[test]
fn turmoil_one_acceptor_slow() {
    let _guard = init_tracing();
    // Test that consensus can be reached even when one acceptor is very slow
    // (simulates a partially failed acceptor)
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(10))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];

    // Start all 3 acceptors, but one is very slow
    for (i, name) in ACCEPTOR_NAMES.iter().enumerate() {
        sim.host(*name, move || async move {
            // Acceptor-2 has a long startup delay
            if i == 2 {
                tokio::time::sleep(Duration::from_secs(20)).await;
            }

            let my_node_id = acceptor_node_id(ACCEPTOR_NAMES[i]);
            let listener =
                turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, ACCEPTOR_PORT)).await?;
            let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
            let state = TestState::new(my_node_id, acceptor_addrs);
            let shared = SharedAcceptorState::new();
            loop {
                let (stream, _) = listener.accept().await?;
                let conn = Framed::new(stream, AcceptorCodec::default());
                let state = state.clone();
                let shared = shared.clone();
                tokio::spawn(async move {
                    let _ = run_acceptor(state, shared, conn).await;
                });
            }
        });
    }

    let learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let learned_clone = learned.clone();

    sim.client("proposer", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        tx.send("value-with-one-slow".to_string()).await.unwrap();
        drop(tx);

        let _ = run_proposer(state, TcpConnector::new(), rx, turmoil_config()).await;
        Ok(())
    });

    sim.run().unwrap();

    let learned = learned.lock().unwrap();
    assert_eq!(learned.len(), 1, "Should have learned one value");
    assert_eq!(
        learned.get(&0).unwrap().message,
        "value-with-one-slow",
        "Should have learned the correct value"
    );
}

#[test]
fn turmoil_acceptor_crashes_and_recovers() {
    let _guard = init_tracing();
    // Test that the proposer can handle an acceptor that crashes and comes back
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];

    for name in ACCEPTOR_NAMES {
        start_acceptor(&mut sim, name, ACCEPTOR_NAMES);
    }

    let learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let learned_clone = learned.clone();

    sim.client("proposer", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        // Send multiple messages
        for i in 0..3 {
            tx.send(format!("message-{i}")).await.unwrap();
        }
        drop(tx);

        let _ = run_proposer(state, TcpConnector::new(), rx, turmoil_config()).await;
        Ok(())
    });

    // Crash acceptor-2 after a short delay, then bounce it back
    sim.client("chaos", async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        turmoil::partition("acceptor-2", "proposer");

        // Wait a bit then heal the partition
        tokio::time::sleep(Duration::from_millis(200)).await;
        turmoil::repair("acceptor-2", "proposer");

        Ok(())
    });

    sim.run().unwrap();

    let learned = learned.lock().unwrap();
    assert!(
        learned.len() >= 2,
        "Should have learned at least 2 messages despite partition, got {}",
        learned.len()
    );
}

#[test]
fn turmoil_high_latency_acceptor() {
    let _guard = init_tracing();
    // Test that consensus works when one acceptor has very high latency
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];

    for name in ACCEPTOR_NAMES {
        start_acceptor(&mut sim, name, ACCEPTOR_NAMES);
    }

    let learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let learned_clone = learned.clone();

    sim.client("proposer", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        tx.send("value-with-slow-acceptor".to_string())
            .await
            .unwrap();
        drop(tx);

        let _ = run_proposer(state, TcpConnector::new(), rx, turmoil_config()).await;
        Ok(())
    });

    // Hold packets to acceptor-2 for a while (simulates very high latency)
    sim.client("slow-link", async move {
        turmoil::hold("acceptor-2", "proposer");
        // Release after consensus should be reached with the other two
        tokio::time::sleep(Duration::from_secs(5)).await;
        turmoil::release("acceptor-2", "proposer");
        Ok(())
    });

    sim.run().unwrap();

    let learned = learned.lock().unwrap();
    assert_eq!(learned.len(), 1, "Should have learned one value");
    assert_eq!(learned.get(&0).unwrap().message, "value-with-slow-acceptor");
}

/// Codec for learner-side: same as ProposerCodec (encodes AcceptorRequest, decodes AcceptorMessage)
type LearnerCodec = ProposerCodec;

// --- Learner Tests ---

/// Helper: start an acceptor host with both proposer and learner listener support
fn start_acceptor_with_learner_support(
    sim: &mut turmoil::Sim<'_>,
    name: &'static str,
    acceptor_names: &'static [&'static str],
) {
    sim.host(name, move || async move {
        let my_node_id = acceptor_node_id(name);
        let acceptor_addrs = resolve_acceptors(acceptor_names);
        let shared = SharedAcceptorState::new();

        let listener =
            turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, ACCEPTOR_PORT)).await?;

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let conn = Framed::new(stream, AcceptorCodec::default());
            let state = TestState::new(my_node_id, acceptor_addrs.clone());
            let shared = shared.clone();
            tokio::spawn(async move {
                let _ = run_acceptor(state, shared, conn).await;
            });
        }
    });
}

/// Helper: connect to all acceptors' learner ports
async fn connect_to_all_acceptors(
    names: &[&str],
) -> std::result::Result<Vec<Framed<turmoil::net::TcpStream, LearnerCodec>>, io::Error> {
    let mut connections = Vec::new();
    for name in names {
        let addr = SocketAddr::new(turmoil::lookup(*name), ACCEPTOR_PORT);
        let stream = turmoil::net::TcpStream::connect(addr).await?;
        connections.push(Framed::new(stream, LearnerCodec::default()));
    }
    Ok(connections)
}

#[test]
fn turmoil_learner_sync() {
    let _guard = init_tracing();
    // Test that a learner can sync from acceptors and receive proposals
    // Learner connects to ALL acceptors and requires quorum confirmation
    //
    // This test runs proposer first, then learner connects after proposer finishes.
    // Both run in the same simulation using a notification channel.

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];

    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build();

    // Start all acceptors with learner support
    for name in ACCEPTOR_NAMES {
        start_acceptor_with_learner_support(&mut sim, name, ACCEPTOR_NAMES);
    }

    let proposer_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let proposer_learned_clone = proposer_learned.clone();
    let learner_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let learner_learned_clone = learner_learned.clone();

    // Notification channel - proposer signals when done
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();

    // Proposer commits some values
    sim.client("proposer", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = proposer_learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        for i in 0..3 {
            tx.send(format!("message-{i}")).await.unwrap();
        }
        drop(tx);

        run_proposer(state, TcpConnector::new(), rx, turmoil_config())
            .await
            .map_err(|e| {
                Box::new(io::Error::other(format!("{e:?}"))) as Box<dyn std::error::Error>
            })?;

        // Signal completion
        let _ = done_tx.send(());
        Ok(())
    });

    // wait for proposer to finish
    while !sim.step().unwrap() {}

    // Learner waits for proposer to finish, then syncs
    sim.client("learner", async move {
        // Wait for proposer to finish
        let _ = (&mut done_rx).await;

        // Connect to ALL acceptors' learner ports
        let connections = connect_to_all_acceptors(ACCEPTOR_NAMES).await?;

        let mut state = TestState::new(proposer_node_id("learner"), vec![]);
        state.learned = learner_learned_clone;

        let mut session = LearnerSession::new(&state, connections).await?;
        while session.learn_one(&mut state).await?.is_some()
            && state.learned.lock().unwrap().len() < 3
        {}

        Ok(())
    });

    sim.run().unwrap();

    // Check that proposer learned values
    let proposer_learned = proposer_learned.lock().unwrap();
    assert_eq!(
        proposer_learned.len(),
        3,
        "Proposer should have learned 3 values"
    );

    // Check that learner synced the values (should have all 3 with quorum from all acceptors)
    let learner_learned = learner_learned.lock().unwrap();
    assert_eq!(
        learner_learned.len(),
        3,
        "Learner should have synced all 3 values with quorum, got {}",
        learner_learned.len()
    );

    // Verify learner has the same values as proposer
    for (round, entry) in learner_learned.iter() {
        let proposer_entry = proposer_learned.get(round).expect("Round should exist");
        assert_eq!(
            entry.message, proposer_entry.message,
            "Learner and proposer should agree on round {round}"
        );
    }
}

#[test]
fn turmoil_learner_live_broadcast() {
    let _guard = init_tracing();
    // Test that a learner receives live broadcasts as values are accepted
    // Learner connects to ALL acceptors and requires quorum confirmation
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];

    // Start all acceptors with learner support
    for name in ACCEPTOR_NAMES {
        start_acceptor_with_learner_support(&mut sim, name, ACCEPTOR_NAMES);
    }

    let learner_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let learner_learned_clone = learner_learned.clone();

    // Learner connects to ALL acceptors FIRST (before proposer commits)
    sim.client("learner", async move {
        // Connect to ALL acceptors' learner ports immediately
        let connections = connect_to_all_acceptors(ACCEPTOR_NAMES).await?;

        let mut state = TestState::new(proposer_node_id("learner"), vec![]);
        state.learned = learner_learned_clone;

        // Run learner with all connections - requires quorum
        let _ =
            tokio::time::timeout(Duration::from_secs(10), run_learner(state, connections)).await;

        Ok(())
    });

    let proposer_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let proposer_learned_clone = proposer_learned.clone();

    // Proposer commits values AFTER learner is connected
    sim.client("proposer", async move {
        // Wait for learner to connect and sync
        tokio::time::sleep(Duration::from_secs(1)).await;

        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer"), acceptor_addrs);
        state.learned = proposer_learned_clone;

        let (mut tx, rx) = mpsc::channel::<String>(16);
        for i in 0..3 {
            tx.send(format!("live-message-{i}")).await.unwrap();
            // Small delay between messages
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        drop(tx);

        run_proposer(state, TcpConnector::new(), rx, turmoil_config())
            .await
            .map_err(|e| {
                Box::new(io::Error::other(format!("{e:?}"))) as Box<dyn std::error::Error>
            })?;
        Ok(())
    });

    sim.run().unwrap();

    // Check proposer learned all values
    let proposer_learned = proposer_learned.lock().unwrap();
    assert_eq!(
        proposer_learned.len(),
        3,
        "Proposer should have learned 3 values"
    );

    // Check learner received live broadcasts with quorum
    let learner_learned = learner_learned.lock().unwrap();
    println!(
        "Learner received {} values via live broadcast",
        learner_learned.len()
    );
    assert_eq!(
        learner_learned.len(),
        3,
        "Learner should have received all 3 live broadcasts with quorum, got {}",
        learner_learned.len()
    );
}
