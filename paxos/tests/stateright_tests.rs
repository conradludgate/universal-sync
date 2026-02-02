//! Stateright model checker tests for Multi-Paxos
//!
//! This module uses Stateright to exhaustively verify the Paxos implementation.
//! It mirrors the TLA+ specification in `spec/MultiPaxos.tla`.
//!
//! The acceptor logic uses the same `AcceptorCore` and message types as the
//! production code, ensuring the model checker verifies the actual implementation.

use std::borrow::Cow;
use std::hash::Hash;
use std::sync::Arc;

use basic_paxos::core::{
    AcceptorCore, AcceptorRequest, AcceptorResponse, PreparePhaseResult,
    ProposalKey as ProposalKeyGeneric, ProposerCore,
};
use stateright::actor::{Actor, ActorModel, Id, Network, Out};
use stateright::{Checker, Model};

/// Message value (simplified to an integer for model checking)
type Value = u64;

/// Concrete proposal key type for model checking
type ProposalKey = ProposalKeyGeneric<u64, u64, usize>;

/// Acceptor state - uses the shared AcceptorCore
type AcceptorState = AcceptorCore<u64, ProposalKey, Value>;

/// Core request type alias
type Request = AcceptorRequest<ProposalKey, Value>;

/// Core response type alias  
type Response = AcceptorResponse<ProposalKey, Value>;

/// Messages in the Paxos protocol - uses core types
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum PaxosMsg {
    /// Request from proposer to acceptor (uses core type)
    Request(Request),
    /// Response from acceptor to proposer (uses core type)
    /// - For Prepare: contains current promised/accepted state
    /// - For Accept: if accepted.0 == for_proposal, the accept succeeded
    Response(Response),
}

// =============================================================================
// PROPOSER
// =============================================================================

/// Proposer state using ProposerCore for phase tracking
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProposerState {
    /// Core proposer logic - tracks phase and quorum
    core: ProposerCore<ProposalKey, ProposalKey, Value, Id>,
    /// Current round number
    current_round: u64,
    /// True if we've learned a value for this round
    learned: Option<(u64, Value)>,
}

// =============================================================================
// COMBINED ACTOR (for heterogeneous actors)
// =============================================================================

/// Combined actor enum to support heterogeneous actors in the model
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum PaxosActor {
    Acceptor {
        id: usize,
    },
    Proposer {
        id: usize,
        acceptor_ids: Vec<Id>,
        initial_value: Value,
    },
}

/// Combined state
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum PaxosActorState {
    Acceptor(AcceptorState),
    Proposer(ProposerState),
}

impl Actor for PaxosActor {
    type Msg = PaxosMsg;
    type State = PaxosActorState;
    type Timer = ();
    type Storage = ();
    type Random = ();

    fn on_start(
        &self,
        _id: Id,
        _storage: &Option<Self::Storage>,
        o: &mut Out<Self>,
    ) -> Self::State {
        match self {
            PaxosActor::Acceptor { .. } => PaxosActorState::Acceptor(AcceptorCore::new()),
            PaxosActor::Proposer {
                id,
                acceptor_ids,
                initial_value,
            } => {
                // Start by sending Prepare to all acceptors
                let proposal = ProposalKey {
                    round: 0,
                    attempt: 0,
                    node: *id,
                };
                for &acc in acceptor_ids {
                    o.send(acc, PaxosMsg::Request(AcceptorRequest::Prepare(proposal)));
                }
                PaxosActorState::Proposer(ProposerState {
                    core: ProposerCore::new(proposal, *initial_value, acceptor_ids.len()),
                    current_round: 0,
                    learned: None,
                })
            }
        }
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        // Clone what we need to avoid borrow issues
        let current_state = state.as_ref().clone();

        match (self, current_state) {
            (PaxosActor::Acceptor { .. }, PaxosActorState::Acceptor(acc_state)) => {
                self.handle_acceptor_msg(&acc_state, state, src, msg, o);
            }
            (
                PaxosActor::Proposer {
                    id, acceptor_ids, ..
                },
                PaxosActorState::Proposer(prop_state),
            ) => {
                self.handle_proposer_msg(*id, acceptor_ids, &prop_state, state, src, msg, o);
            }
            _ => {}
        }
    }
}

impl PaxosActor {
    fn handle_acceptor_msg(
        &self,
        acc_state: &AcceptorState,
        state: &mut Cow<PaxosActorState>,
        src: Id,
        msg: PaxosMsg,
        o: &mut Out<Self>,
    ) {
        let PaxosMsg::Request(request) = msg else {
            return;
        };

        let mut new_state = acc_state.clone();

        // Use the shared handle_request method from AcceptorCore
        let response = new_state.handle_request(request, |p| p.round);

        // Check if state changed (response.promised matches for_proposal means we promised/accepted)
        let state_changed = response.promised.as_ref() == Some(&response.for_proposal);

        if state_changed {
            *state.to_mut() = PaxosActorState::Acceptor(new_state);
        }

        // Always send a response
        o.send(src, PaxosMsg::Response(response));
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_proposer_msg(
        &self,
        id: usize,
        acceptor_ids: &[Id],
        prop_state: &ProposerState,
        state: &mut Cow<PaxosActorState>,
        src: Id,
        msg: PaxosMsg,
        o: &mut Out<Self>,
    ) {
        // Skip if already learned
        if prop_state.learned.is_some() {
            return;
        }

        let PaxosMsg::Response(response) = msg else {
            return;
        };

        // Only process responses for our current proposal
        if response.for_proposal != *prop_state.core.proposal() {
            return;
        }

        let mut new_core = prop_state.core.clone();

        if new_core.is_preparing() {
            // Handle promise response
            let promised_key = match &response.promised {
                Some(p) => *p,
                None => return, // No promise, ignore
            };

            let result = new_core.handle_promise(src, promised_key, response.accepted, |p| *p);

            match result {
                PreparePhaseResult::Quorum { value } => {
                    // Got quorum - send Accept to all
                    let proposal = *new_core.proposal();
                    for &acc in acceptor_ids {
                        o.send(
                            acc,
                            PaxosMsg::Request(AcceptorRequest::Accept(proposal, value)),
                        );
                    }
                }
                PreparePhaseResult::Rejected { superseded_by, .. } => {
                    // Retry with higher attempt
                    let new_attempt = superseded_by.attempt + 1;
                    let new_proposal = ProposalKey {
                        round: prop_state.current_round,
                        attempt: new_attempt,
                        node: id,
                    };
                    for &acc in acceptor_ids {
                        o.send(
                            acc,
                            PaxosMsg::Request(AcceptorRequest::Prepare(new_proposal)),
                        );
                    }
                    // Create new core for the new proposal
                    new_core = ProposerCore::new(
                        new_proposal,
                        *prop_state.core.value(),
                        acceptor_ids.len(),
                    );
                }
                PreparePhaseResult::Pending => {}
            }

            *state.to_mut() = PaxosActorState::Proposer(ProposerState {
                core: new_core,
                current_round: prop_state.current_round,
                learned: None,
            });
        } else if new_core.is_accepting() {
            // Handle accept response
            let accepted_key = response.accepted.as_ref().map(|(p, _)| *p);
            let proposal = *new_core.proposal();

            let result = new_core.handle_accepted(src, accepted_key, proposal);

            match result {
                basic_paxos::core::AcceptPhaseResult::Learned { value, .. } => {
                    *state.to_mut() = PaxosActorState::Proposer(ProposerState {
                        core: new_core,
                        current_round: prop_state.current_round,
                        learned: Some((proposal.round, value)),
                    });
                }
                basic_paxos::core::AcceptPhaseResult::Rejected { superseded_by } => {
                    // Retry with higher attempt
                    let new_attempt = superseded_by.attempt + 1;
                    let new_proposal = ProposalKey {
                        round: prop_state.current_round,
                        attempt: new_attempt,
                        node: id,
                    };
                    for &acc in acceptor_ids {
                        o.send(
                            acc,
                            PaxosMsg::Request(AcceptorRequest::Prepare(new_proposal)),
                        );
                    }
                    new_core = ProposerCore::new(
                        new_proposal,
                        *prop_state.core.value(),
                        acceptor_ids.len(),
                    );
                    *state.to_mut() = PaxosActorState::Proposer(ProposerState {
                        core: new_core,
                        current_round: prop_state.current_round,
                        learned: None,
                    });
                }
                basic_paxos::core::AcceptPhaseResult::Pending => {
                    *state.to_mut() = PaxosActorState::Proposer(ProposerState {
                        core: new_core,
                        current_round: prop_state.current_round,
                        learned: None,
                    });
                }
            }
        }
    }
}

// =============================================================================
// MODEL CHECKING
// =============================================================================

/// Configuration for bounding state space
#[derive(Clone)]
struct PaxosConfig {
    max_attempt: u64,
}

/// Build a Paxos model with the specified configuration
fn paxos_model(
    num_proposers: usize,
    num_acceptors: usize,
    values: &[Value],
) -> ActorModel<PaxosActor, PaxosConfig, ()> {
    paxos_model_with_config(num_proposers, num_acceptors, values, 3)
}

/// Build a Paxos model with custom max_attempt bound
fn paxos_model_with_config(
    num_proposers: usize,
    num_acceptors: usize,
    values: &[Value],
    max_attempt: u64,
) -> ActorModel<PaxosActor, PaxosConfig, ()> {
    let acceptor_ids: Vec<Id> = (0..num_acceptors).map(Id::from).collect();

    // Use ordered network (FIFO per-link) for smaller state space
    let mut model = ActorModel::new(PaxosConfig { max_attempt }, ())
        .init_network(Network::new_ordered([]))
        // Bound state space: don't explore states with too many retries
        .within_boundary(|cfg, state| {
            state
                .actor_states
                .iter()
                .all(|s: &Arc<PaxosActorState>| match s.as_ref() {
                    PaxosActorState::Proposer(ps) => ps.core.proposal().attempt <= cfg.max_attempt,
                    _ => true,
                })
        });

    // Add acceptors
    for i in 0..num_acceptors {
        model = model.actor(PaxosActor::Acceptor { id: i });
    }

    // Add proposers with different values
    for (i, &value) in (0..num_proposers).zip(values.iter().cycle()) {
        model = model.actor(PaxosActor::Proposer {
            id: num_acceptors + i,
            acceptor_ids: acceptor_ids.clone(),
            initial_value: value,
        });
    }

    // Add safety property: Agreement
    // If any proposer is Done, all Done proposers must have the same value for the same round
    model = model.property(stateright::Expectation::Always, "Agreement", |_, state| {
        let done_values: Vec<(u64, Value)> = state
            .actor_states
            .iter()
            .filter_map(|s: &Arc<PaxosActorState>| {
                if let PaxosActorState::Proposer(ps) = s.as_ref() {
                    ps.learned
                } else {
                    None
                }
            })
            .collect();

        // Group by round and check all values are the same
        for (round, value) in &done_values {
            for (r2, v2) in &done_values {
                if round == r2 && value != v2 {
                    return false; // Agreement violated!
                }
            }
        }
        true
    });

    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_paxos_single_proposer() {
        // 1 proposer, 3 acceptors - simplest case, should be fast
        let model = paxos_model(1, 3, &[1]);

        let checker = model.checker().threads(num_cpus::get()).spawn_bfs().join();

        checker.assert_properties();
        println!(
            "Single proposer: {} states explored",
            checker.unique_state_count()
        );
    }

    #[test]
    #[ignore]
    fn check_paxos_two_proposers() {
        // 2 proposers with different values, 3 acceptors
        // Use smaller max_attempt bound (2 instead of 3) for faster checking
        let model = paxos_model_with_config(2, 3, &[1, 2], 2);

        let checker = model.checker().threads(num_cpus::get()).spawn_bfs().join();

        checker.assert_properties();
        println!(
            "Two proposers: {} states explored",
            checker.unique_state_count()
        );
    }
}
