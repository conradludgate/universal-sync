//! Acceptor handler for processing Paxos protocol messages.

use std::fmt;

use tracing::trace;

use super::{AcceptorMessage, AcceptorStateStore};
use crate::{Learner, Proposal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidProposal;

impl fmt::Display for InvalidProposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid proposal")
    }
}

impl std::error::Error for InvalidProposal {}

pub(crate) enum PromiseOutcome<A: Learner> {
    Promised(AcceptorMessage<A>),
    Outdated(AcceptorMessage<A>),
}

pub(crate) enum AcceptOutcome<A: Learner> {
    Accepted(AcceptorMessage<A>),
    Outdated(AcceptorMessage<A>),
}

pub struct AcceptorHandler<A, S> {
    acceptor: A,
    state: S,
}

impl<A, S> AcceptorHandler<A, S>
where
    A: Learner,
    S: AcceptorStateStore<A>,
{
    pub fn new(acceptor: A, state: S) -> Self {
        Self { acceptor, state }
    }

    pub(crate) fn node_id(&self) -> <A::Proposal as Proposal>::NodeId {
        self.acceptor.node_id()
    }

    pub(crate) fn state(&self) -> &S {
        &self.state
    }

    pub(crate) fn acceptor(&self) -> &A {
        &self.acceptor
    }

    /// Used by the runner to apply learned values.
    pub(crate) fn acceptor_mut(&mut self) -> &mut A {
        &mut self.acceptor
    }

    pub(crate) async fn handle_prepare(
        &mut self,
        proposal: &A::Proposal,
    ) -> Result<PromiseOutcome<A>, InvalidProposal> {
        if self.acceptor.validate(proposal).is_err() {
            return Err(InvalidProposal);
        }

        match self.state.promise(proposal).await {
            Ok(()) => {
                trace!(round = ?proposal.round(), "promised");
                let state = self.state.get(proposal.round()).await;
                Ok(PromiseOutcome::Promised(AcceptorMessage::from_round_state(
                    state,
                )))
            }
            Err(round_state) => {
                trace!(round = ?proposal.round(), "promise rejected - outdated");
                Ok(PromiseOutcome::Outdated(AcceptorMessage::from_round_state(
                    round_state,
                )))
            }
        }
    }

    /// Value is NOT applied to the learner here — that only happens when
    /// quorum is confirmed via the learning process.
    pub(crate) async fn handle_accept(
        &mut self,
        proposal: &A::Proposal,
        message: &A::Message,
    ) -> Result<AcceptOutcome<A>, InvalidProposal> {
        if self.acceptor.validate(proposal).is_err() {
            return Err(InvalidProposal);
        }

        match self.state.accept(proposal, message).await {
            Ok(()) => {
                trace!(round = ?proposal.round(), "accepted");
                let state = self.state.get(proposal.round()).await;
                Ok(AcceptOutcome::Accepted(AcceptorMessage::from_round_state(
                    state,
                )))
            }
            Err(round_state) => {
                trace!(round = ?proposal.round(), "accept rejected - outdated");
                Ok(AcceptOutcome::Outdated(AcceptorMessage::from_round_state(
                    round_state,
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use error_stack::Report;

    use super::*;
    use crate::acceptor::RoundState;
    use crate::{Validated, ValidationError};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TP {
        node: u32,
        round: u64,
        attempt: u32,
    }

    impl Proposal for TP {
        type NodeId = u32;
        type RoundId = u64;
        type AttemptId = u32;
        fn node_id(&self) -> u32 {
            self.node
        }
        fn round(&self) -> u64 {
            self.round
        }
        fn attempt(&self) -> u32 {
            self.attempt
        }
        fn next_attempt(a: u32) -> u32 {
            a + 1
        }
    }

    struct TL {
        reject_node: Option<u32>,
    }

    impl Learner for TL {
        type Proposal = TP;
        type Message = String;
        type Error = std::io::Error;
        type AcceptorId = u32;
        fn node_id(&self) -> u32 {
            0
        }
        fn current_round(&self) -> u64 {
            0
        }
        fn acceptors(&self) -> impl IntoIterator<Item = u32, IntoIter: ExactSizeIterator> {
            vec![]
        }
        fn propose(&self, _a: u32) -> TP {
            TP {
                node: 0,
                round: 0,
                attempt: 0,
            }
        }
        fn validate(&self, p: &TP) -> Result<Validated, Report<ValidationError>> {
            if self.reject_node == Some(p.node) {
                Err(Report::new(ValidationError))
            } else {
                Ok(Validated::assert_valid())
            }
        }
        async fn apply(&mut self, _: TP, _: String) -> Result<(), std::io::Error> {
            Ok(())
        }
    }

    struct MemStore {
        state: Mutex<BTreeMap<u64, RoundState<TL>>>,
    }

    impl MemStore {
        fn new() -> Self {
            Self {
                state: Mutex::new(BTreeMap::new()),
            }
        }
    }

    impl AcceptorStateStore<TL> for MemStore {
        type Subscription = futures::stream::Empty<(TP, String)>;

        async fn get(&self, round: u64) -> RoundState<TL> {
            self.state
                .lock()
                .unwrap()
                .get(&round)
                .cloned()
                .unwrap_or_default()
        }

        async fn promise(&self, proposal: &TP) -> Result<(), RoundState<TL>> {
            let mut state = self.state.lock().unwrap();
            let rs = state.entry(proposal.round()).or_default();
            if let Some(ref p) = rs.promised
                && (p.attempt > proposal.attempt
                    || (p.attempt == proposal.attempt && p.node > proposal.node))
            {
                return Err(rs.clone());
            }
            rs.promised = Some(proposal.clone());
            Ok(())
        }

        async fn accept(&self, proposal: &TP, message: &String) -> Result<(), RoundState<TL>> {
            let mut state = self.state.lock().unwrap();
            let rs = state.entry(proposal.round()).or_default();
            if rs.promised.as_ref() != Some(proposal) {
                return Err(rs.clone());
            }
            rs.accepted = Some((proposal.clone(), message.clone()));
            Ok(())
        }

        async fn subscribe_from(&self, _: u64) -> Self::Subscription {
            unimplemented!()
        }

        async fn highest_accepted_round(&self) -> Option<u64> {
            unimplemented!()
        }

        async fn get_accepted_from(&self, _: u64) -> Vec<(TP, String)> {
            unimplemented!()
        }
    }

    #[test]
    fn invalid_proposal_display() {
        assert_eq!(InvalidProposal.to_string(), "invalid proposal");
        let _: &dyn std::error::Error = &InvalidProposal;
    }

    #[tokio::test]
    async fn handle_prepare_promised() {
        let learner = TL { reject_node: None };
        let store = MemStore::new();
        let mut handler = AcceptorHandler::new(learner, store);
        let proposal = TP {
            node: 1,
            round: 0,
            attempt: 0,
        };
        let result = handler.handle_prepare(&proposal).await.unwrap();
        assert!(matches!(result, PromiseOutcome::Promised(_)));
    }

    #[tokio::test]
    async fn handle_prepare_outdated() {
        let learner = TL { reject_node: None };
        let store = MemStore::new();
        let mut handler = AcceptorHandler::new(learner, store);

        let high = TP {
            node: 1,
            round: 0,
            attempt: 5,
        };
        handler.handle_prepare(&high).await.unwrap();

        let low = TP {
            node: 1,
            round: 0,
            attempt: 0,
        };
        let result = handler.handle_prepare(&low).await.unwrap();
        assert!(matches!(result, PromiseOutcome::Outdated(_)));
    }

    #[tokio::test]
    async fn handle_prepare_invalid() {
        let learner = TL {
            reject_node: Some(99),
        };
        let store = MemStore::new();
        let mut handler = AcceptorHandler::new(learner, store);
        let proposal = TP {
            node: 99,
            round: 0,
            attempt: 0,
        };
        let result = handler.handle_prepare(&proposal).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handle_accept_accepted() {
        let learner = TL { reject_node: None };
        let store = MemStore::new();
        let mut handler = AcceptorHandler::new(learner, store);

        let proposal = TP {
            node: 1,
            round: 0,
            attempt: 0,
        };
        handler.handle_prepare(&proposal).await.unwrap();

        let result = handler
            .handle_accept(&proposal, &"msg".to_string())
            .await
            .unwrap();
        assert!(matches!(result, AcceptOutcome::Accepted(_)));
    }

    #[tokio::test]
    async fn handle_accept_outdated() {
        let learner = TL { reject_node: None };
        let store = MemStore::new();
        let mut handler = AcceptorHandler::new(learner, store);

        let p1 = TP {
            node: 1,
            round: 0,
            attempt: 0,
        };
        handler.handle_prepare(&p1).await.unwrap();

        let p2 = TP {
            node: 2,
            round: 0,
            attempt: 0,
        };
        let result = handler
            .handle_accept(&p2, &"msg".to_string())
            .await
            .unwrap();
        assert!(matches!(result, AcceptOutcome::Outdated(_)));
    }

    #[tokio::test]
    async fn handle_accept_invalid() {
        let learner = TL {
            reject_node: Some(99),
        };
        let store = MemStore::new();
        let mut handler = AcceptorHandler::new(learner, store);
        let proposal = TP {
            node: 99,
            round: 0,
            attempt: 0,
        };
        let result = handler.handle_accept(&proposal, &"msg".to_string()).await;
        assert!(result.is_err());
    }

    #[test]
    fn handler_accessors() {
        let learner = TL { reject_node: None };
        let store = MemStore::new();
        let mut handler = AcceptorHandler::new(learner, store);
        assert_eq!(handler.node_id(), 0);
        let _ = handler.state();
        let _ = handler.acceptor();
        let _ = handler.acceptor_mut();
    }
}
