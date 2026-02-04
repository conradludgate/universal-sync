//! Push-based proposer for Paxos
//!
//! This module provides a synchronous, push-based API for proposing values.
//! The caller is responsible for:
//! - Sending messages to acceptors
//! - Handling timeouts and retries
//! - Managing the network layer
//!
//! # Usage
//!
//! ```ignore
//! let mut proposer = Proposer::new();
//!
//! // Start a proposal (gets acceptors from learner)
//! let messages = proposer.propose(&learner, attempt, value);
//! // Send messages to acceptors...
//!
//! // Process responses
//! for (acceptor_id, response) in responses {
//!     match proposer.receive(&learner, acceptor_id, response) {
//!         ProposeResult::Continue(messages) => {
//!             // Send these messages to acceptors
//!         }
//!         ProposeResult::Learned { proposal, message } => {
//!             // Consensus reached!
//!         }
//!         ProposeResult::Rejected { superseded_by } => {
//!             // Retry with higher attempt
//!         }
//!     }
//! }
//! ```
//!
//! For learning values from other proposers, use [`QuorumTracker`] to track
//! accepted values and detect when quorum is reached.

mod quorum;

use std::collections::BTreeSet;
use std::marker::PhantomData;

pub use quorum::QuorumTracker;

use crate::core::{AcceptPhaseResult, PreparePhaseResult, ProposerCore};
use crate::messages::{AcceptorMessage, AcceptorRequest};
use crate::traits::{Learner, Proposal, ProposalKey};

/// Result of calling `propose()` or `receive()`.
#[derive(Debug, Clone)]
pub enum ProposeResult<L: Learner> {
    /// Continue the protocol - send these messages to the specified acceptors.
    Continue(Vec<(L::AcceptorId, AcceptorRequest<L>)>),
    /// Consensus reached - value was learned.
    Learned {
        /// The learned proposal
        proposal: L::Proposal,
        /// The learned message/value
        message: L::Message,
    },
    /// Proposal was rejected by a higher proposal.
    Rejected {
        /// The key of the proposal that superseded ours
        superseded_by: ProposalKey<L::Proposal>,
    },
}

/// State for a single round of proposing.
struct RoundState<L: Learner> {
    /// The core proposer state machine
    core: ProposerCore<ProposalKey<L::Proposal>, L::Proposal, L::Message, L::AcceptorId>,
    /// The actual proposal (contains the message/value)
    proposal: L::Proposal,
    /// The acceptors for this round (captured from learner at propose time)
    acceptors: BTreeSet<L::AcceptorId>,
}

/// Push-based Paxos proposer.
///
/// This proposer does not manage timing or network I/O. Instead, it provides
/// a pure state machine that the caller drives by:
/// 1. Calling `propose()` to start a proposal and get messages to send
/// 2. Calling `receive()` with responses to process them
///
/// The caller is responsible for sending messages, handling timeouts,
/// and retrying with higher attempts when rejected.
///
/// Acceptors are read from the learner at each `propose()` call.
pub struct Proposer<L: Learner> {
    /// Active proposal state (if any)
    active: Option<RoundState<L>>,
    /// Marker for learner type
    _marker: PhantomData<L>,
}

impl<L: Learner> Default for Proposer<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Learner> Proposer<L> {
    /// Create a new proposer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: None,
            _marker: PhantomData,
        }
    }

    /// Start a new proposal round.
    ///
    /// Returns `Prepare` messages to send to all acceptors.
    /// If there are no acceptors, immediately returns `Learned`.
    ///
    /// # Arguments
    ///
    /// * `learner` - The learner providing round info, proposal creation, and acceptor set
    /// * `attempt` - The attempt number for this proposal
    /// * `message` - The message/value to propose
    #[must_use]
    pub fn propose(
        &mut self,
        learner: &L,
        attempt: <L::Proposal as Proposal>::AttemptId,
        message: L::Message,
    ) -> ProposeResult<L> {
        // Get acceptors from learner
        let acceptors_iter = learner.acceptors().into_iter();
        let num_acceptors = acceptors_iter.len();
        let acceptors: BTreeSet<_> = acceptors_iter.collect();

        // Create the proposal
        let proposal = learner.propose(attempt);
        let key = proposal.key();

        // Handle empty acceptor set - immediately learned (local proposal)
        if acceptors.is_empty() {
            self.active = None;
            return ProposeResult::Learned { proposal, message };
        }

        // Create core state machine
        let core = ProposerCore::new(key, message.clone(), num_acceptors);

        // Generate Prepare messages for all acceptors
        let messages: Vec<_> = acceptors
            .iter()
            .map(|id| (*id, AcceptorRequest::Prepare(proposal.clone())))
            .collect();

        self.active = Some(RoundState {
            core,
            proposal,
            acceptors,
        });

        ProposeResult::Continue(messages)
    }

    /// Process a response from an acceptor.
    ///
    /// Returns either:
    /// - `Continue(messages)` - send these messages to continue the protocol
    /// - `Learned { proposal, message }` - consensus reached
    /// - `Rejected { superseded_by }` - proposal was superseded, retry with higher attempt
    #[must_use]
    pub fn receive(
        &mut self,
        _learner: &L,
        acceptor_id: L::AcceptorId,
        response: AcceptorMessage<L>,
    ) -> ProposeResult<L> {
        let Some(state) = &mut self.active else {
            // No active proposal, ignore
            return ProposeResult::Continue(vec![]);
        };

        let promised_key = response.promised.key();
        let accepted = response.accepted;

        if state.core.is_preparing() {
            // Phase 1: collecting promises
            let accepted_for_core = accepted.as_ref().map(|(p, m)| (p.clone(), m.clone()));

            let result = state.core.handle_promise(
                acceptor_id,
                promised_key,
                accepted_for_core,
                Proposal::key,
            );

            match result {
                PreparePhaseResult::Pending => ProposeResult::Continue(vec![]),
                PreparePhaseResult::Quorum { value } => {
                    // Got quorum of promises, send Accept to all acceptors
                    let proposal = state.proposal.clone();
                    let messages: Vec<_> = state
                        .acceptors
                        .iter()
                        .map(|id| {
                            (
                                *id,
                                AcceptorRequest::Accept(proposal.clone(), value.clone()),
                            )
                        })
                        .collect();
                    ProposeResult::Continue(messages)
                }
                PreparePhaseResult::Rejected {
                    superseded_by,
                    accepted: _,
                } => {
                    self.active = None;
                    ProposeResult::Rejected { superseded_by }
                }
            }
        } else if state.core.is_accepting() {
            // Phase 2: collecting accepts
            let accepted_key = accepted.as_ref().map(|(p, _)| p.key());
            let proposal = state.proposal.clone();

            let result = state
                .core
                .handle_accepted(acceptor_id, accepted_key, proposal.clone());

            match result {
                AcceptPhaseResult::Pending => ProposeResult::Continue(vec![]),
                AcceptPhaseResult::Learned { proposal, value } => {
                    self.active = None;
                    ProposeResult::Learned {
                        proposal,
                        message: value,
                    }
                }
                AcceptPhaseResult::Rejected { superseded_by } => {
                    self.active = None;
                    ProposeResult::Rejected { superseded_by }
                }
            }
        } else {
            // Already learned or failed, no-op
            ProposeResult::Continue(vec![])
        }
    }

    /// Check if there's an active proposal in progress.
    #[must_use]
    pub fn is_proposing(&self) -> bool {
        self.active.is_some()
    }

    /// Cancel the current proposal (if any).
    pub fn cancel(&mut self) {
        self.active = None;
    }
}

#[cfg(test)]
mod tests {
    use error_stack::Report;

    use super::*;
    use crate::traits::{Validated, ValidationError};

    // Simple test types
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestProposal {
        node: u32,
        round: u64,
        attempt: u32,
    }

    impl Proposal for TestProposal {
        type NodeId = u32;
        type RoundId = u64;
        type AttemptId = u32;

        fn node_id(&self) -> Self::NodeId {
            self.node
        }
        fn round(&self) -> Self::RoundId {
            self.round
        }
        fn attempt(&self) -> Self::AttemptId {
            self.attempt
        }
        fn next_attempt(attempt: Self::AttemptId) -> Self::AttemptId {
            attempt + 1
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestMessage(String);

    struct TestLearner {
        node_id: u32,
        round: u64,
        acceptors: Vec<u32>,
    }

    impl Learner for TestLearner {
        type Proposal = TestProposal;
        type Message = TestMessage;
        type Error = std::io::Error;
        type AcceptorId = u32;

        fn node_id(&self) -> u32 {
            self.node_id
        }

        fn current_round(&self) -> u64 {
            self.round
        }

        fn acceptors(&self) -> impl IntoIterator<Item = u32, IntoIter: ExactSizeIterator> {
            self.acceptors.clone()
        }

        fn propose(&self, attempt: u32) -> TestProposal {
            TestProposal {
                node: self.node_id,
                round: self.round,
                attempt,
            }
        }

        fn validate(&self, _proposal: &TestProposal) -> Result<Validated, Report<ValidationError>> {
            Ok(Validated::assert_valid())
        }

        async fn apply(
            &mut self,
            _proposal: TestProposal,
            _message: TestMessage,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn test_empty_acceptors_immediate_learn() {
        let mut proposer: Proposer<TestLearner> = Proposer::new();
        let learner = TestLearner {
            node_id: 1,
            round: 0,
            acceptors: vec![],
        };
        let message = TestMessage("hello".into());

        let result = proposer.propose(&learner, 0, message.clone());

        match result {
            ProposeResult::Learned {
                proposal,
                message: m,
            } => {
                assert_eq!(proposal.node, 1);
                assert_eq!(proposal.round, 0);
                assert_eq!(m, message);
            }
            _ => panic!("expected Learned"),
        }
    }

    #[test]
    fn test_single_acceptor_quorum() {
        let mut proposer: Proposer<TestLearner> = Proposer::new();
        let learner = TestLearner {
            node_id: 1,
            round: 0,
            acceptors: vec![100],
        };
        let message = TestMessage("hello".into());

        // Start proposal
        let result = proposer.propose(&learner, 0, message.clone());
        let ProposeResult::Continue(messages) = result else {
            panic!("expected Continue");
        };
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, 100);

        // Simulate promise response
        let response = AcceptorMessage {
            promised: TestProposal {
                node: 1,
                round: 0,
                attempt: 0,
            },
            accepted: None,
        };
        let result = proposer.receive(&learner, 100, response);

        // Should get Accept messages (quorum reached with 1 acceptor)
        let ProposeResult::Continue(messages) = result else {
            panic!("expected Continue with Accept messages");
        };
        assert_eq!(messages.len(), 1);
        match &messages[0].1 {
            AcceptorRequest::Accept(_, m) => assert_eq!(m, &message),
            AcceptorRequest::Prepare(_) => panic!("expected Accept, got Prepare"),
        }

        // Simulate accept response
        let response = AcceptorMessage {
            promised: TestProposal {
                node: 1,
                round: 0,
                attempt: 0,
            },
            accepted: Some((
                TestProposal {
                    node: 1,
                    round: 0,
                    attempt: 0,
                },
                message.clone(),
            )),
        };
        let result = proposer.receive(&learner, 100, response);

        match result {
            ProposeResult::Learned {
                proposal,
                message: m,
            } => {
                assert_eq!(proposal.node, 1);
                assert_eq!(m, message);
            }
            _ => panic!("expected Learned"),
        }
    }

    #[test]
    fn test_rejection() {
        let mut proposer: Proposer<TestLearner> = Proposer::new();
        let learner = TestLearner {
            node_id: 1,
            round: 0,
            acceptors: vec![100],
        };
        let message = TestMessage("hello".into());

        // Start proposal
        let _ = proposer.propose(&learner, 0, message);

        // Simulate rejection (higher proposal promised)
        let response = AcceptorMessage {
            promised: TestProposal {
                node: 2,
                round: 0,
                attempt: 1,
            }, // Higher proposal
            accepted: None,
        };
        let result = proposer.receive(&learner, 100, response);

        match result {
            ProposeResult::Rejected { superseded_by } => {
                assert_eq!(superseded_by.attempt(), 1);
                assert_eq!(superseded_by.node_id(), 2);
            }
            _ => panic!("expected Rejected"),
        }

        assert!(!proposer.is_proposing());
    }
}
