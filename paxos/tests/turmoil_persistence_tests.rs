//! Turmoil-based persistence tests for Paxos
//!
//! These tests use turmoil's `unstable-fs` feature to test acceptor crash recovery
//! with file-based persistence.

use std::{
    collections::BTreeMap,
    io::{self, Read, Seek, Write},
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use basic_paxos::{
    Acceptor, AcceptorMessage, AcceptorRequest, AcceptorStateStore, BackoffConfig, Connector,
    Learner, Proposal, Proposer, ProposerConfig, RoundState, Sleep, run_acceptor, run_proposer,
};
use bytes::{Buf, BufMut, BytesMut};
use futures::Stream;

// Turmoil's filesystem shim
use rand::rngs::StdRng;
use tokio::sync::broadcast;
use tokio_util::codec::{Decoder, Encoder, Framed};
use turmoil::Builder;
use turmoil::fs::shim::std::fs::{self, File, OpenOptions};

fn init_tracing() -> impl Sized {
    use tracing::Dispatch;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    // Generate a unique trace file in target/tmp
    let trace_dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let trace_file = trace_dir.join(format!(
        "paxos_trace_{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    eprintln!("Trace file: {}", trace_file.display());

    let fmt_layer = fmt::layer()
        .with_test_writer()
        .with_span_events(fmt::format::FmtSpan::CLOSE);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("basic_paxos=debug,turmoil_persistence_tests=debug"));

    let subscriber = tracing_subscriber::registry().with(filter).with(fmt_layer);

    let dispatch = Dispatch::new(subscriber);
    tracing::dispatcher::set_default(&dispatch)
}

const ACCEPTOR_PORT: u16 = 9999;

// --- Turmoil Sleep Implementation ---

#[derive(Clone, Copy, Default)]
struct TurmoilSleep;

impl Sleep for TurmoilSleep {
    async fn sleep(&self, duration: std::time::Duration) {
        tokio::time::sleep(duration).await;
    }
}

fn turmoil_config(seed: u64) -> ProposerConfig<TurmoilSleep, StdRng> {
    ProposerConfig::with_seed(
        BackoffConfig {
            initial: std::time::Duration::from_millis(10),
            max: std::time::Duration::from_millis(200),
            multiplier: 2.0,
        },
        TurmoilSleep,
        seed,
    )
    .with_phase_timeout(std::time::Duration::from_secs(5))
}

// --- Test Proposal Implementation ---

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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct LearnedEntry {
    proposal: TestProposal,
    message: String,
}

type SharedLearned = Arc<Mutex<BTreeMap<u64, LearnedEntry>>>;

#[derive(Clone)]
struct TestState {
    node_id: SocketAddr,
    acceptors: Vec<SocketAddr>,
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
        let round = proposal.round;
        let mut learned = self.learned.lock().unwrap();
        // Always keep the entry with the highest proposal key (the actual winner)
        let entry = learned.entry(round).or_insert(LearnedEntry {
            proposal: proposal.clone(),
            message: message.clone(),
        });
        if proposal.key() > entry.proposal.key() {
            *entry = LearnedEntry { proposal, message };
        }
        Ok(())
    }
}

impl Acceptor for TestState {
    async fn accept(&mut self, _proposal: TestProposal, _message: String) -> Result<(), io::Error> {
        Ok(())
    }
}

// --- Retry Stream ---

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
        if learned_count >= self.target_rounds {
            return Poll::Ready(None);
        }
        // Use a simple value - the round is tracked by the protocol, not the message
        // We just identify which proposer sent this value
        let msg = format!("{}-value", self.proposer_id);
        Poll::Ready(Some(msg))
    }
}

// --- Postcard-based Codec ---

struct PostcardCodec<Enc, Dec>(std::marker::PhantomData<(Enc, Dec)>);

impl<Enc, Dec> Default for PostcardCodec<Enc, Dec> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Enc: serde::Serialize, Dec> Encoder<Enc> for PostcardCodec<Enc, Dec> {
    type Error = io::Error;

    fn encode(&mut self, item: Enc, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = postcard::to_allocvec(&item)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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

type ProposerCodec = PostcardCodec<AcceptorRequest<TestState>, AcceptorMessage<TestState>>;
type AcceptorCodec = PostcardCodec<AcceptorMessage<TestState>, AcceptorRequest<TestState>>;

type ProposerConn = Framed<turmoil::net::TcpStream, ProposerCodec>;

// --- TCP Connector ---

#[derive(Clone)]
struct TcpConnector {
    current_delay: std::time::Duration,
    attempts: u32,
    max_attempts: u32,
}

impl TcpConnector {
    const BASE_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
    const MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

    fn new() -> Self {
        Self {
            current_delay: Self::BASE_DELAY,
            attempts: 0,
            max_attempts: 20, // More attempts for persistence tests
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

        self.current_delay = (self.current_delay * 2).min(Self::MAX_DELAY);
        self.attempts += 1;

        Box::pin(async move {
            if attempts >= max_attempts {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("failed to connect to {} after {} attempts", addr, attempts),
                ));
            }
            if delay > Self::BASE_DELAY {
                tokio::time::sleep(delay).await;
            }
            let stream = turmoil::net::TcpStream::connect(addr).await?;
            Ok(Framed::new(stream, ProposerCodec::default()))
        })
    }
}

// --- File-Backed Acceptor State Store ---

/// An entry stored in the persistence file.
/// We store both promises and accepts to fully recover state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum PersistenceEntry {
    Promise {
        round: u64,
        proposal: TestProposal,
    },
    Accept {
        round: u64,
        proposal: TestProposal,
        message: String,
    },
}

/// File-backed acceptor state store.
///
/// Appends entries to a file on promise/accept and loads state from file on creation.
/// Uses length-prefixed postcard format for entries.
#[derive(Clone)]
struct FileBackedAcceptorState {
    /// Open file handle for appending
    file: Arc<Mutex<File>>,
    /// In-memory state (populated from file on load)
    inner: Arc<Mutex<BTreeMap<u64, RoundState<TestState>>>>,
    /// Broadcast channel for learners
    broadcast: broadcast::Sender<(TestProposal, String)>,
}

impl FileBackedAcceptorState {
    /// Create a new file-backed state, loading existing data from file if present.
    fn new(path: PathBuf) -> io::Result<Self> {
        let (broadcast, _) = broadcast::channel(16);
        let inner = Arc::new(Mutex::new(BTreeMap::new()));

        // Open file for reading and appending (create if doesn't exist)
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        // Load existing data from file
        Self::load_from_file(&mut file, &inner)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            inner,
            broadcast,
        })
    }

    /// Load state from file, replaying all entries.
    fn load_from_file(
        file: &mut File,
        inner: &Mutex<BTreeMap<u64, RoundState<TestState>>>,
    ) -> io::Result<()> {
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        if data.is_empty() {
            return Ok(());
        }

        let mut pos = 0;
        let mut last_valid_pos = 0;
        let mut inner = inner.lock().unwrap();

        while pos + 4 <= data.len() {
            let entry_start = pos;
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + len > data.len() {
                // Incomplete entry at end of file (crash during write)
                tracing::warn!(
                    entry_start,
                    expected_len = len,
                    available = data.len() - pos,
                    "truncated entry at end of persistence file, will repair"
                );
                break;
            }

            let entry: PersistenceEntry = match postcard::from_bytes(&data[pos..pos + len]) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(
                        ?e,
                        entry_start,
                        "failed to deserialize persistence entry, will repair"
                    );
                    break;
                }
            };
            pos += len;
            last_valid_pos = pos;

            match entry {
                PersistenceEntry::Promise { round, proposal } => {
                    let state = inner.entry(round).or_default();
                    // Only update if this proposal is higher
                    if state
                        .promised
                        .as_ref()
                        .is_none_or(|p| p.key() < proposal.key())
                    {
                        tracing::trace!(round, ?proposal, "loaded promise");
                        state.promised = Some(proposal);
                    }
                }
                PersistenceEntry::Accept {
                    round,
                    proposal,
                    message,
                } => {
                    let state = inner.entry(round).or_default();
                    // Only update if this proposal is higher than what we have
                    let dominated = state
                        .accepted
                        .as_ref()
                        .is_some_and(|(p, _)| p.key() >= proposal.key());
                    if !dominated {
                        tracing::trace!(round, ?proposal, %message, "loaded accept");
                        state.promised = Some(proposal.clone());
                        state.accepted = Some((proposal, message));
                    } else {
                        tracing::trace!(round, ?proposal, "skipped dominated accept");
                    }
                }
            }
        }

        // If we detected corruption/truncation, repair the file by truncating to last valid position
        if last_valid_pos < data.len() {
            tracing::warn!(
                last_valid_pos,
                file_len = data.len(),
                "repairing persistence file by truncating to last valid entry"
            );
            file.set_len(last_valid_pos as u64)?;
            file.seek(std::io::SeekFrom::End(0))?;
            file.sync_all()?;
        }

        // Log final loaded state
        for (round, state) in inner.iter() {
            if let Some((proposal, message)) = &state.accepted {
                tracing::debug!(round, %message, ?proposal, "final loaded state");
            }
        }
        tracing::info!(rounds = inner.len(), "loaded state from file");
        Ok(())
    }

    /// Append an entry to the persistence file.
    fn append_entry(&self, entry: &PersistenceEntry) -> io::Result<()> {
        tracing::trace!(?entry, "appending persistence entry");

        let encoded = postcard::to_allocvec(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut file = self.file.lock().unwrap();
        let len = (encoded.len() as u32).to_le_bytes();
        file.write_all(&len)?;
        file.write_all(&encoded)?;
        file.sync_all()?;

        Ok(())
    }
}

/// Receiver for file-backed state broadcasts.
struct FileBackedReceiver {
    inner: tokio_stream::wrappers::BroadcastStream<(TestProposal, String)>,
}

impl Stream for FileBackedReceiver {
    type Item = (TestProposal, String);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match std::task::ready!(Pin::new(&mut self.get_mut().inner).poll_next(cx)) {
            Some(Ok(item)) => Poll::Ready(Some(item)),
            _ => Poll::Ready(None),
        }
    }
}

impl AcceptorStateStore<TestState> for FileBackedAcceptorState {
    type Receiver = FileBackedReceiver;

    fn get(&self, round: u64) -> RoundState<TestState> {
        self.inner
            .lock()
            .unwrap()
            .get(&round)
            .cloned()
            .unwrap_or_default()
    }

    fn promise(&self, proposal: &TestProposal) -> Result<(), RoundState<TestState>> {
        let mut inner = self.inner.lock().unwrap();
        let round = proposal.round();
        let state = inner.entry(round).or_default();

        // Reject if we already promised to a higher proposal
        if state
            .promised
            .as_ref()
            .is_some_and(|p| p.key() > proposal.key())
        {
            return Err(state.clone());
        }

        // Also reject if we already accepted a higher proposal
        // (can't promise to go backwards)
        if state
            .accepted
            .as_ref()
            .is_some_and(|(p, _)| p.key() > proposal.key())
        {
            return Err(state.clone());
        }

        // Persist AND update in-memory state atomically (holding the lock)
        self.append_entry(&PersistenceEntry::Promise {
            round,
            proposal: proposal.clone(),
        })
        .expect("failed to persist promise");

        state.promised = Some(proposal.clone());
        Ok(())
    }

    fn accept(
        &self,
        proposal: &TestProposal,
        message: &String,
    ) -> Result<(), RoundState<TestState>> {
        let mut inner = self.inner.lock().unwrap();
        let round = proposal.round();
        let state = inner.entry(round).or_default();

        // Reject if we promised to a higher proposal
        if state
            .promised
            .as_ref()
            .is_some_and(|p| p.key() > proposal.key())
        {
            return Err(state.clone());
        }

        // Also reject if we already accepted a higher proposal for this round
        // This prevents overwriting a potentially-chosen value with a lower proposal
        if state
            .accepted
            .as_ref()
            .is_some_and(|(p, _)| p.key() > proposal.key())
        {
            return Err(state.clone());
        }

        // Persist AND update in-memory state atomically (holding the lock)
        self.append_entry(&PersistenceEntry::Accept {
            round,
            proposal: proposal.clone(),
            message: message.clone(),
        })
        .expect("failed to persist accept");

        state.accepted = Some((proposal.clone(), message.clone()));
        state.promised = Some(proposal.clone());

        let _ = self.broadcast.send((proposal.clone(), message.clone()));
        Ok(())
    }

    fn subscribe(&self) -> Self::Receiver {
        FileBackedReceiver {
            inner: tokio_stream::wrappers::BroadcastStream::new(self.broadcast.subscribe()),
        }
    }

    fn accepted_from(&self, from_round: u64) -> Vec<(TestProposal, String)> {
        let result: Vec<_> = self
            .inner
            .lock()
            .unwrap()
            .range(from_round..)
            .filter_map(|(_, state)| state.accepted.clone())
            .collect();
        tracing::debug!(
            from_round,
            count = result.len(),
            "accepted_from returning values"
        );
        for (proposal, message) in &result {
            tracing::trace!(round = proposal.round, %message, ?proposal, "accepted_from entry");
        }
        result
    }

    fn highest_accepted_round(&self) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|(round, state)| state.accepted.as_ref().map(|_| *round))
    }
}

// --- Helper Functions ---

fn resolve_acceptors(names: &[&str]) -> Vec<SocketAddr> {
    names
        .iter()
        .map(|name| SocketAddr::new(turmoil::lookup(*name), ACCEPTOR_PORT))
        .collect()
}

fn proposer_node_id(name: &str) -> SocketAddr {
    SocketAddr::new(turmoil::lookup(name), 0)
}

fn acceptor_node_id(name: &str) -> SocketAddr {
    SocketAddr::new(turmoil::lookup(name), ACCEPTOR_PORT)
}

fn learner_node_id(name: &str) -> SocketAddr {
    SocketAddr::new(turmoil::lookup(name), 1)
}

/// Start a persistent acceptor that loads state from file on startup.
fn start_persistent_acceptor(
    sim: &mut turmoil::Sim<'_>,
    name: &'static str,
    acceptor_names: &'static [&'static str],
) {
    sim.host(name, move || async move {
        let my_node_id = acceptor_node_id(name);
        let acceptor_addrs = resolve_acceptors(acceptor_names);

        // Ensure data directory exists
        let _ = fs::create_dir_all("/data");

        // Use a file in /data for persistence
        let persistence_path = PathBuf::from(format!("/data/{}.paxos", name));
        let shared = FileBackedAcceptorState::new(persistence_path)?;

        let listener =
            turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, ACCEPTOR_PORT)).await?;

        tracing::info!(?my_node_id, "acceptor started");

        loop {
            let (stream, mut addr) = listener.accept().await?;
            addr.set_port(0);
            let conn = Framed::new(stream, AcceptorCodec::default());
            let state = TestState::new(my_node_id, acceptor_addrs.clone());
            let shared = shared.clone();
            tokio::spawn(async move {
                let _ = run_acceptor(state, shared, conn, addr).await;
            });
        }
    });
}

// --- Tests ---

/// Test that acceptors can crash and recover, maintaining consensus.
///
/// This test runs two competing proposers concurrently, bounces an acceptor
/// mid-test, and verifies that:
/// 1. Both proposers complete their rounds (using phase timeout to recover)
/// 2. Learners see consistent consensus from the persisted acceptor state
#[test]
fn persistence_with_acceptor_bounce() {
    let _guard = init_tracing();
    let mut sim = Builder::new()
        .simulation_duration(std::time::Duration::from_secs(120))
        .build();

    const ACCEPTOR_NAMES: &[&str] = &["acceptor-0", "acceptor-1", "acceptor-2"];
    const TOTAL_ROUNDS: usize = 6;

    // Each proposer has its own learned state (they're competing)
    let proposer1_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let proposer2_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));

    // Start acceptors with file persistence
    for name in ACCEPTOR_NAMES {
        start_persistent_acceptor(&mut sim, name, ACCEPTOR_NAMES);
    }

    // Proposer 1 - competes for all rounds
    let p1_learned = proposer1_learned.clone();
    sim.client("proposer-1", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer-1"), acceptor_addrs);
        state.learned = p1_learned.clone();

        let messages = RetryMessageStream::new("p1".to_string(), TOTAL_ROUNDS, p1_learned);
        run_proposer(state, TcpConnector::new(), messages, turmoil_config(1)).await?;

        tracing::info!("proposer-1 completed");
        Ok(())
    });

    // Proposer 2 - also competes for all rounds
    let p2_learned = proposer2_learned.clone();
    sim.client("proposer-2", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(proposer_node_id("proposer-2"), acceptor_addrs);
        state.learned = p2_learned.clone();

        let messages = RetryMessageStream::new("p2".to_string(), TOTAL_ROUNDS, p2_learned);
        run_proposer(state, TcpConnector::new(), messages, turmoil_config(2)).await?;

        tracing::info!("proposer-2 completed");
        Ok(())
    });

    // Run until both proposers have made some progress, then bounce
    let mut bounced = false;
    for _ in 0..20000 {
        if sim.step().unwrap() {
            break;
        }

        let p1_count = proposer1_learned.lock().unwrap().len();
        let p2_count = proposer2_learned.lock().unwrap().len();

        // Bounce acceptor-1 after both proposers have made some progress
        if !bounced && p1_count >= 2 && p2_count >= 2 {
            tracing::info!(
                "bouncing acceptor-1, p1 has {} rounds, p2 has {} rounds",
                p1_count,
                p2_count
            );
            sim.bounce("acceptor-1");
            bounced = true;
        }

        // Exit when both have completed
        if p1_count >= TOTAL_ROUNDS && p2_count >= TOTAL_ROUNDS {
            break;
        }
    }

    assert!(bounced, "acceptor should have been bounced");

    // Let simulation fully settle
    for _ in 0..500 {
        if sim.step().unwrap() {
            break;
        }
    }

    let p1_count = proposer1_learned.lock().unwrap().len();
    let p2_count = proposer2_learned.lock().unwrap().len();
    tracing::info!("proposers completed: p1={}, p2={}", p1_count, p2_count);

    // Both proposers should have completed all rounds (thanks to phase timeout)
    assert!(
        p1_count >= TOTAL_ROUNDS && p2_count >= TOTAL_ROUNDS,
        "proposers should complete {} rounds, got p1={}, p2={}",
        TOTAL_ROUNDS,
        p1_count,
        p2_count
    );

    // Now run learners to verify consensus - they are the source of truth
    let learner1_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));
    let learner2_learned: SharedLearned = Arc::new(Mutex::new(BTreeMap::new()));

    let l1_learned = learner1_learned.clone();
    sim.client("learner-1", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(learner_node_id("learner-1"), acceptor_addrs);
        state.learned = l1_learned;

        // Run learner using Proposer::learn_one()
        let mut proposer = Proposer::new(state.node_id(), TcpConnector::new());
        proposer.sync_actors(state.acceptors());
        proposer.start_sync(&state);
        loop {
            let Some((p, m)) = proposer.learn_one(&state).await else {
                break;
            };
            state.apply(p, m).await?;
        }

        tracing::info!("learner-1 completed");
        Ok(())
    });

    let l2_learned = learner2_learned.clone();
    sim.client("learner-2", async move {
        let acceptor_addrs = resolve_acceptors(ACCEPTOR_NAMES);
        let mut state = TestState::new(learner_node_id("learner-2"), acceptor_addrs);
        state.learned = l2_learned;

        // Run learner using Proposer::learn_one()
        let mut proposer = Proposer::new(state.node_id(), TcpConnector::new());
        proposer.sync_actors(state.acceptors());
        proposer.start_sync(&state);
        loop {
            let Some((p, m)) = proposer.learn_one(&state).await else {
                break;
            };
            state.apply(p, m).await?;
        }

        tracing::info!("learner-2 completed");
        Ok(())
    });

    // Run until learners complete - need enough steps for message delivery
    for i in 0..10000 {
        if sim.step().unwrap() {
            tracing::debug!("simulation ended at step {}", i);
            break;
        }
        let l1_count = learner1_learned.lock().unwrap().len();
        let l2_count = learner2_learned.lock().unwrap().len();
        if l1_count >= TOTAL_ROUNDS && l2_count >= TOTAL_ROUNDS {
            tracing::debug!("learners complete at step {}", i);
            break;
        }
    }

    // Verify learners got all rounds
    let l1_values: Vec<_> = learner1_learned
        .lock()
        .unwrap()
        .clone()
        .into_iter()
        .collect();
    let l2_values: Vec<_> = learner2_learned
        .lock()
        .unwrap()
        .clone()
        .into_iter()
        .collect();

    // Log learner values for debugging
    tracing::debug!("learner-1 learned {} rounds:", l1_values.len());
    for (round, entry) in &l1_values {
        tracing::debug!(
            round,
            message = %entry.message,
            proposal_attempt = entry.proposal.attempt,
            proposal_node = ?entry.proposal.node_id,
            "learner-1 value"
        );
    }
    tracing::debug!("learner-2 learned {} rounds:", l2_values.len());
    for (round, entry) in &l2_values {
        tracing::debug!(
            round,
            message = %entry.message,
            proposal_attempt = entry.proposal.attempt,
            proposal_node = ?entry.proposal.node_id,
            "learner-2 value"
        );
    }

    assert!(
        l1_values.len() >= TOTAL_ROUNDS,
        "learner-1 should have learned at least {} rounds, got {}",
        TOTAL_ROUNDS,
        l1_values.len()
    );
    assert!(
        l2_values.len() >= TOTAL_ROUNDS,
        "learner-2 should have learned at least {} rounds, got {}",
        TOTAL_ROUNDS,
        l2_values.len()
    );

    // The key assertion: learners must agree with each other (consensus)
    for (round, l1_entry) in &l1_values {
        let l2_entry = l2_values.iter().find(|(r, _)| r == round).map(|(_, e)| e);

        assert_eq!(
            Some(&l1_entry.message),
            l2_entry.map(|e| &e.message),
            "learners disagree on round {} message",
            round
        );
    }

    tracing::info!(
        "SUCCESS: Both learners agree on {} rounds after acceptor bounce with competing proposers",
        l1_values.len()
    );
}
