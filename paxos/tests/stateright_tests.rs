//! Stateright model checker tests for Multi-Paxos
//!
//! This module uses Stateright to exhaustively verify the Paxos implementation.
//! It mirrors the TLA+ specification in `spec/MultiPaxos.tla`.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;

use stateright::actor::{Actor, ActorModel, Id, Network, Out};
use stateright::{Checker, Model};

/// Proposal key: (round, attempt, node_id) with lexicographic ordering
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ProposalKey {
    round: u64,
    attempt: u64,
    node: usize,
}

/// Message value (simplified to an integer for model checking)
type Value = u64;

/// Messages in the Paxos protocol
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum PaxosMsg {
    /// Phase 1a: Proposer sends Prepare
    Prepare { proposal: ProposalKey },
    /// Phase 1b: Acceptor promises (returns highest promised and accepted)
    Promise {
        proposal: ProposalKey,
        promised: Option<ProposalKey>,
        accepted: Option<(ProposalKey, Value)>,
    },
    /// Phase 2a: Proposer sends Accept with value
    Accept { proposal: ProposalKey, value: Value },
    /// Phase 2b: Acceptor acknowledges accept
    Accepted { proposal: ProposalKey, value: Value },
}

// =============================================================================
// ACCEPTOR
// =============================================================================

/// Acceptor state for model checking
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct AcceptorState {
    /// Per-round: highest promised proposal
    promised: BTreeMap<u64, ProposalKey>,
    /// Per-round: accepted (proposal, value)
    accepted: BTreeMap<u64, (ProposalKey, Value)>,
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
        accepts: std::collections::BTreeSet<Id>,
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
            PaxosActor::Acceptor { .. } => PaxosActorState::Acceptor(AcceptorState::default()),
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
                    o.send(acc, PaxosMsg::Prepare { proposal });
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
        match msg {
            PaxosMsg::Prepare { proposal } => {
                let r = proposal.round;
                let current_promised = acc_state.promised.get(&r).copied();
                let current_accepted = acc_state.accepted.get(&r).cloned();

                // Can only promise if proposal >= current promised and >= current accepted
                let dominated_by_promise = current_promised.is_some_and(|p| p > proposal);
                let dominated_by_accept = current_accepted
                    .as_ref()
                    .is_some_and(|(p, _)| *p > proposal);

                if !dominated_by_promise && !dominated_by_accept {
                    // Update promise
                    let new_state = match state.as_ref() {
                        PaxosActorState::Acceptor(s) => {
                            let mut s = s.clone();
                            s.promised.insert(r, proposal);
                            PaxosActorState::Acceptor(s)
                        }
                        _ => unreachable!(),
                    };
                    *state.to_mut() = new_state;
                    o.send(
                        src,
                        PaxosMsg::Promise {
                            proposal,
                            promised: Some(proposal),
                            accepted: current_accepted,
                        },
                    );
                } else {
                    // Reject: send current state
                    o.send(
                        src,
                        PaxosMsg::Promise {
                            proposal,
                            promised: current_promised,
                            accepted: current_accepted,
                        },
                    );
                }
            }
            PaxosMsg::Accept { proposal, value } => {
                let r = proposal.round;
                let current_promised = acc_state.promised.get(&r).copied();
                let current_accepted = acc_state.accepted.get(&r).cloned();

                // Can only accept if proposal >= current promised and >= current accepted
                let dominated_by_promise = current_promised.is_some_and(|p| p > proposal);
                let dominated_by_accept = current_accepted
                    .as_ref()
                    .is_some_and(|(p, _)| *p > proposal);

                if !dominated_by_promise && !dominated_by_accept {
                    // Accept
                    let new_state = match state.as_ref() {
                        PaxosActorState::Acceptor(s) => {
                            let mut s = s.clone();
                            s.promised.insert(r, proposal);
                            s.accepted.insert(r, (proposal, value));
                            PaxosActorState::Acceptor(s)
                        }
                        _ => unreachable!(),
                    };
                    *state.to_mut() = new_state;
                    o.send(src, PaxosMsg::Accepted { proposal, value });
                }
            }
            _ => {}
        }
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
            (
                ProposerPhase::Preparing {
                    proposal,
                    value,
                    promises,
                },
                PaxosMsg::Promise {
                    proposal: p,
                    promised,
                    accepted,
                },
            ) if p == proposal => {
                // Check if we got a promise for our proposal
                if promised == Some(proposal) {
                    let mut new_promises = promises.clone();
                    // Use insert to deduplicate by acceptor ID
                    new_promises.insert(src, accepted);

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
                                PaxosMsg::Accept {
                                    proposal,
                                    value: chosen_value,
                                },
                            );
                        }

                        let new_state = PaxosActorState::Proposer(ProposerState {
                            phase: ProposerPhase::Accepting {
                                proposal,
                                value: chosen_value,
                                accepts: std::collections::BTreeSet::new(),
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
                } else if let Some(higher) = promised {
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
                                PaxosMsg::Prepare {
                                    proposal: new_proposal,
                                },
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
            (
                ProposerPhase::Accepting {
                    proposal,
                    value,
                    accepts,
                },
                PaxosMsg::Accepted { proposal: p, .. },
            ) if p == proposal => {
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
