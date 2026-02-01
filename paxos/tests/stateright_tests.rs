//! Stateright model checker tests for Multi-Paxos
//!
//! This module uses Stateright to exhaustively verify the Paxos implementation.
//! It mirrors the TLA+ specification in `spec/MultiPaxos.tla`.
//!
//! The acceptor logic uses the same `AcceptorCore` and message types as the
//! production code, ensuring the model checker verifies the actual implementation.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;
use std::sync::Arc;

use basic_paxos::core::{
    AcceptorCore, AcceptorRequest, AcceptorResponse, ProposalKey as ProposalKeyGeneric,
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

/// Proposer phase
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ProposerPhase {
    /// Collecting promises
    Preparing {
        proposal: ProposalKey,
        value: Value,
        /// Map from acceptor ID to their accepted value (tracks unique acceptors)
        promises: BTreeMap<Id, Option<(ProposalKey, Value)>>,
    },
    /// Collecting accepts
    Accepting {
        proposal: ProposalKey,
        value: Value,
        /// Set of acceptors that have accepted (tracks unique acceptors)
        accepts: BTreeSet<Id>,
    },
    /// Successfully learned a value for this round
    Done { round: u64, value: Value },
}

/// Proposer state
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProposerState {
    phase: ProposerPhase,
    current_round: u64,
    current_attempt: u64,
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

impl PaxosActor {
    fn quorum(num_acceptors: usize) -> usize {
        num_acceptors / 2 + 1
    }
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
                    phase: ProposerPhase::Preparing {
                        proposal,
                        value: *initial_value,
                        promises: BTreeMap::new(),
                    },
                    current_round: 0,
                    current_attempt: 0,
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
        let quorum = Self::quorum(acceptor_ids.len());
        let phase = prop_state.phase.clone();

        match (phase, msg) {
            // Handle Response (Promise) during Preparing phase
            (
                ProposerPhase::Preparing {
                    proposal,
                    value,
                    promises,
                },
                PaxosMsg::Response(response),
            ) if response.for_proposal == proposal => {
                // Check if we got a promise for our proposal
                if response.promised == Some(proposal) {
                    let mut new_promises = promises.clone();
                    // Use insert to deduplicate by acceptor ID
                    new_promises.insert(src, response.accepted);

                    if new_promises.len() >= quorum {
                        // Got quorum! Pick value from highest accepted, or use ours
                        let chosen_value = new_promises
                            .values()
                            .filter_map(|acc| acc.as_ref())
                            .max_by_key(|(p, _)| p)
                            .map(|(_, v)| *v)
                            .unwrap_or(value);

                        // Send Accept to all acceptors
                        for &acc in acceptor_ids {
                            o.send(
                                acc,
                                PaxosMsg::Request(AcceptorRequest::Accept(proposal, chosen_value)),
                            );
                        }

                        let new_state = PaxosActorState::Proposer(ProposerState {
                            phase: ProposerPhase::Accepting {
                                proposal,
                                value: chosen_value,
                                accepts: BTreeSet::new(),
                            },
                            current_round: prop_state.current_round,
                            current_attempt: prop_state.current_attempt,
                        });
                        *state.to_mut() = new_state;
                    } else {
                        let new_state = PaxosActorState::Proposer(ProposerState {
                            phase: ProposerPhase::Preparing {
                                proposal,
                                value,
                                promises: new_promises,
                            },
                            current_round: prop_state.current_round,
                            current_attempt: prop_state.current_attempt,
                        });
                        *state.to_mut() = new_state;
                    }
                } else if let Some(higher) = response.promised {
                    // Someone promised a higher proposal - need to retry with higher attempt
                    if higher > proposal {
                        let new_attempt = higher.attempt + 1;
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
                        let new_state = PaxosActorState::Proposer(ProposerState {
                            phase: ProposerPhase::Preparing {
                                proposal: new_proposal,
                                value,
                                promises: BTreeMap::new(),
                            },
                            current_round: prop_state.current_round,
                            current_attempt: new_attempt,
                        });
                        *state.to_mut() = new_state;
                    }
                }
            }
            // Handle Response during Accepting phase (Accept confirmation)
            (
                ProposerPhase::Accepting {
                    proposal,
                    value,
                    accepts,
                },
                PaxosMsg::Response(response),
            ) if response.for_proposal == proposal => {
                // Check if accept succeeded: accepted field contains our proposal
                let accepted = response
                    .accepted
                    .as_ref()
                    .is_some_and(|(p, _)| *p == proposal);

                if accepted {
                    let mut new_accepts = accepts.clone();
                    new_accepts.insert(src); // Deduplicate by acceptor ID

                    if new_accepts.len() >= quorum {
                        // Success! Value is learned
                        let new_state = PaxosActorState::Proposer(ProposerState {
                            phase: ProposerPhase::Done {
                                round: proposal.round,
                                value,
                            },
                            current_round: prop_state.current_round,
                            current_attempt: prop_state.current_attempt,
                        });
                        *state.to_mut() = new_state;
                    } else {
                        let new_state = PaxosActorState::Proposer(ProposerState {
                            phase: ProposerPhase::Accepting {
                                proposal,
                                value,
                                accepts: new_accepts,
                            },
                            current_round: prop_state.current_round,
                            current_attempt: prop_state.current_attempt,
                        });
                        *state.to_mut() = new_state;
                    }
                }
                // If not accepted, ignore (could handle rejection here)
            }
            _ => {}
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
                    PaxosActorState::Proposer(ps) => ps.current_attempt <= cfg.max_attempt,
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
                if let PaxosActorState::Proposer(ps) = s.as_ref()
                    && let ProposerPhase::Done { round, value } = ps.phase
                {
                    return Some((round, value));
                }
                None
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
    fn check_paxos_two_proposers() {
        // 2 proposers with different values, 3 acceptors
        let model = paxos_model(2, 3, &[1, 2]);

        let checker = model.checker().threads(num_cpus::get()).spawn_bfs().join();

        checker.assert_properties();
        println!(
            "Two proposers: {} states explored",
            checker.unique_state_count()
        );
    }
}
