//! Push-based Paxos proposer.
//! Caller drives the protocol by calling `propose()` and `receive()`.

mod quorum;

use std::collections::BTreeSet;
use std::marker::PhantomData;

pub use quorum::QuorumTracker;
use tracing::{debug, trace};

use crate::core::{AcceptPhaseResult, PreparePhaseResult, ProposerCore};
use crate::messages::{AcceptorMessage, AcceptorRequest};
use crate::traits::{Learner, Proposal, ProposalKey};

#[derive(Debug, Clone)]
pub enum ProposeResult<L: Learner> {
    Continue(Vec<(L::AcceptorId, AcceptorRequest<L>)>),
    Learned {
        proposal: L::Proposal,
        message: L::Message,
    },
    Rejected {
        superseded_by: ProposalKey<L::Proposal>,
    },
}

struct RoundState<L: Learner> {
    core: ProposerCore<ProposalKey<L::Proposal>, L::Proposal, L::Message, L::AcceptorId>,
    proposal: L::Proposal,
    acceptors: BTreeSet<L::AcceptorId>,
}

/// Push-based Paxos proposer.
///
/// Does not manage timing or network I/O. The caller sends messages,
/// handles timeouts, and retries with higher attempts when rejected.
/// Acceptors are read from the learner at each `propose()` call.
pub struct Proposer<L: Learner> {
    active: Option<RoundState<L>>,
    _marker: PhantomData<L>,
}

impl<L: Learner> Default for Proposer<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: Learner> Proposer<L> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            active: None,
            _marker: PhantomData,
        }
    }

    /// Start a new proposal round. Returns `Prepare` messages to send to all acceptors.
    /// If there are no acceptors, immediately returns `Learned`.
    #[must_use]
    pub fn propose(
        &mut self,
        learner: &L,
        attempt: <L::Proposal as Proposal>::AttemptId,
        message: L::Message,
    ) -> ProposeResult<L> {
        let acceptors_iter = learner.acceptors().into_iter();
        let num_acceptors = acceptors_iter.len();

        let proposal = learner.propose(attempt);
        let key = proposal.key();

        if num_acceptors == 0 {
            debug!(round = ?key.0, ?attempt, "no acceptors, immediately learned");
            self.active = None;
            return ProposeResult::Learned { proposal, message };
        }

        debug!(round = ?key.0, ?attempt, num_acceptors, "starting proposal");

        let core = ProposerCore::new(key, message.clone(), num_acceptors);

        let acceptors: BTreeSet<_> = acceptors_iter.collect();
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

    #[must_use]
    pub fn receive(
        &mut self,
        _learner: &L,
        acceptor_id: L::AcceptorId,
        response: AcceptorMessage<L>,
    ) -> ProposeResult<L> {
        let Some(state) = &mut self.active else {
            trace!(?acceptor_id, "ignoring response, no active proposal");
            return ProposeResult::Continue(vec![]);
        };

        let promised_key = response.promised.key();
        let accepted = response.accepted;

        if state.core.is_preparing() {
            trace!(?acceptor_id, promised = ?promised_key, "received promise");
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
                    debug!(round = ?state.proposal.round(), "prepare quorum reached, sending accepts");
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
                    debug!(round = ?state.proposal.round(), ?superseded_by, "proposal rejected during prepare");
                    self.active = None;
                    ProposeResult::Rejected { superseded_by }
                }
            }
        } else if state.core.is_accepting() {
            trace!(?acceptor_id, accepted = ?accepted.as_ref().map(|(p, _)| p.key()), "received accept response");
            let accepted_key = accepted.as_ref().map(|(p, _)| p.key());
            let proposal = state.proposal.clone();

            let result = state
                .core
                .handle_accepted(acceptor_id, accepted_key, proposal.clone());

            match result {
                AcceptPhaseResult::Pending => ProposeResult::Continue(vec![]),
                AcceptPhaseResult::Learned { proposal, value } => {
                    debug!(round = ?proposal.round(), "accept quorum reached, value learned");
                    self.active = None;
                    ProposeResult::Learned {
                        proposal,
                        message: value,
                    }
                }
                AcceptPhaseResult::Rejected { superseded_by } => {
                    debug!(round = ?state.proposal.round(), ?superseded_by, "proposal rejected during accept");
                    self.active = None;
                    ProposeResult::Rejected { superseded_by }
                }
            }
        } else {
            trace!("ignoring response, proposal already completed");
            ProposeResult::Continue(vec![])
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn is_proposing(&self) -> bool {
        self.active.is_some()
    }
}

#[cfg(test)]
mod tests {
    use error_stack::Report;

    use super::*;
    use crate::traits::{Validated, ValidationError};

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

        let result = proposer.propose(&learner, 0, message.clone());
        let ProposeResult::Continue(messages) = result else {
            panic!("expected Continue");
        };
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, 100);

        let response = AcceptorMessage {
            promised: TestProposal {
                node: 1,
                round: 0,
                attempt: 0,
            },
            accepted: None,
        };
        let result = proposer.receive(&learner, 100, response);

        let ProposeResult::Continue(messages) = result else {
            panic!("expected Continue with Accept messages");
        };
        assert_eq!(messages.len(), 1);
        match &messages[0].1 {
            AcceptorRequest::Accept(_, m) => assert_eq!(m, &message),
            AcceptorRequest::Prepare(_) => panic!("expected Accept, got Prepare"),
        }

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

        let _ = proposer.propose(&learner, 0, message);

        let response = AcceptorMessage {
            promised: TestProposal {
                node: 2,
                round: 0,
                attempt: 1,
            },
            accepted: None,
        };
        let result = proposer.receive(&learner, 100, response);

        match result {
            ProposeResult::Rejected { superseded_by } => {
                assert_eq!(superseded_by.attempt(), 1);
                assert_eq!(superseded_by.2, 2);
            }
            _ => panic!("expected Rejected"),
        }

        assert!(!proposer.is_proposing());
    }
}
