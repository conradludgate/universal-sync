//! Stateright model checker tests for Multi-Paxos.
//! Mirrors the TLA+ specification in `spec/MultiPaxos.tla`.

use std::borrow::Cow;
use std::hash::Hash;
use std::sync::Arc;

use itertools::Itertools;
use stateright::actor::{Actor, ActorModel, Id, Network, Out};
use stateright::{Checker, Model};

use super::{
    AcceptorCore, AcceptorRequest, AcceptorResponse, PreparePhaseResult,
    ProposalKey as ProposalKeyGeneric, ProposerCore,
};

type Value = u64;
type ProposalKey = ProposalKeyGeneric<u64, u64, usize>;
type AcceptorState = AcceptorCore<u64, ProposalKey, Value>;
type Request = AcceptorRequest<ProposalKey, Value>;
type Response = AcceptorResponse<ProposalKey, Value>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum PaxosMsg {
    Request(Request),
    /// For Prepare: contains current promised/accepted state.
    /// For Accept: if accepted.0 == `for_proposal`, the accept succeeded.
    Response(Response),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProposerState {
    core: ProposerCore<ProposalKey, ProposalKey, Value, Id>,
    current_round: u64,
    learned: Option<(u64, Value)>,
}

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
        let current_state = state.as_ref().clone();

        match (self, current_state) {
            (PaxosActor::Acceptor { .. }, PaxosActorState::Acceptor(acc_state)) => {
                Self::handle_acceptor_msg(&acc_state, state, src, msg, o);
            }
            (
                PaxosActor::Proposer {
                    id, acceptor_ids, ..
                },
                PaxosActorState::Proposer(prop_state),
            ) => {
                Self::handle_proposer_msg(*id, acceptor_ids, &prop_state, state, src, msg, o);
            }
            _ => {}
        }
    }
}

impl PaxosActor {
    fn handle_acceptor_msg(
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
        let response = new_state.handle_request(request, |p| p.round);

        let state_changed = response.promised.as_ref() == Some(&response.for_proposal);

        if state_changed {
            *state.to_mut() = PaxosActorState::Acceptor(new_state);
        }

        o.send(src, PaxosMsg::Response(response));
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_proposer_msg(
        id: usize,
        acceptor_ids: &[Id],
        prop_state: &ProposerState,
        state: &mut Cow<PaxosActorState>,
        src: Id,
        msg: PaxosMsg,
        o: &mut Out<Self>,
    ) {
        if prop_state.learned.is_some() {
            return;
        }

        let PaxosMsg::Response(response) = msg else {
            return;
        };

        if response.for_proposal != *prop_state.core.proposal() {
            return;
        }

        let mut new_core = prop_state.core.clone();

        if new_core.is_preparing() {
            let promised_key = match &response.promised {
                Some(p) => *p,
                None => return,
            };

            let result = new_core.handle_promise(src, promised_key, response.accepted, |p| *p);

            match result {
                PreparePhaseResult::Quorum { value } => {
                    let proposal = *new_core.proposal();
                    for &acc in acceptor_ids {
                        o.send(
                            acc,
                            PaxosMsg::Request(AcceptorRequest::Accept(proposal, value)),
                        );
                    }
                }
                PreparePhaseResult::Rejected { superseded_by, .. } => {
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
                }
                PreparePhaseResult::Pending => {}
            }

            *state.to_mut() = PaxosActorState::Proposer(ProposerState {
                core: new_core,
                current_round: prop_state.current_round,
                learned: None,
            });
        } else if new_core.is_accepting() {
            let accepted_key = response.accepted.as_ref().map(|(p, _)| *p);
            let proposal = *new_core.proposal();

            let result = new_core.handle_accepted(src, accepted_key, proposal);

            match result {
                super::AcceptPhaseResult::Learned { value, .. } => {
                    *state.to_mut() = PaxosActorState::Proposer(ProposerState {
                        core: new_core,
                        current_round: prop_state.current_round,
                        learned: Some((proposal.round, value)),
                    });
                }
                super::AcceptPhaseResult::Rejected { superseded_by } => {
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
                super::AcceptPhaseResult::Pending => {
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

#[derive(Clone)]
struct PaxosConfig {
    max_attempt: u64,
}

fn paxos_model(
    num_proposers: usize,
    num_acceptors: usize,
    values: &[Value],
) -> ActorModel<PaxosActor, PaxosConfig, ()> {
    paxos_model_with_config(num_proposers, num_acceptors, values, 3)
}

fn check_agreement(state: &stateright::actor::ActorModelState<PaxosActor>) -> bool {
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

    for (round, value) in &done_values {
        for (r2, v2) in &done_values {
            if round == r2 && value != v2 {
                return false;
            }
        }
    }
    true
}

fn check_promise_integrity(state: &stateright::actor::ActorModelState<PaxosActor>) -> bool {
    for s in &state.actor_states {
        if let PaxosActorState::Acceptor(acc) = s.as_ref() {
            for (round, (acc_prop, _)) in &acc.accepted {
                if let Some(promised) = acc.promised.get(round) {
                    if promised < acc_prop {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
    }
    true
}

fn check_consistency(state: &stateright::actor::ActorModelState<PaxosActor>) -> bool {
    let acceptors: Vec<&AcceptorState> = state
        .actor_states
        .iter()
        .filter_map(|s| {
            if let PaxosActorState::Acceptor(acc) = s.as_ref() {
                Some(acc)
            } else {
                None
            }
        })
        .collect();

    let n = acceptors.len();
    let quorum_size = n / 2 + 1;
    let quorums: Vec<Vec<usize>> = (0..n).combinations(quorum_size).collect();

    let mut rounds: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for acc in &acceptors {
        for round in acc.accepted.keys() {
            rounds.insert(*round);
        }
    }

    for round in rounds {
        let mut quorum_accepted: Vec<(ProposalKey, Value)> = Vec::new();

        for quorum in &quorums {
            let accepted_in_quorum: Vec<_> = quorum
                .iter()
                .map(|&i| acceptors[i].accepted.get(&round))
                .collect();

            if accepted_in_quorum.iter().all(Option::is_some) {
                let first = accepted_in_quorum[0].unwrap();
                if accepted_in_quorum.iter().all(|a| a.unwrap() == first) {
                    quorum_accepted.push(*first);
                }
            }
        }

        for (p1, v1) in &quorum_accepted {
            for (p2, v2) in &quorum_accepted {
                if p2 > p1 && v1 != v2 {
                    return false;
                }
            }
        }
    }
    true
}

fn paxos_model_with_config(
    num_proposers: usize,
    num_acceptors: usize,
    values: &[Value],
    max_attempt: u64,
) -> ActorModel<PaxosActor, PaxosConfig, ()> {
    let acceptor_ids: Vec<Id> = (0..num_acceptors).map(Id::from).collect();

    let mut model = ActorModel::new(PaxosConfig { max_attempt }, ())
        .init_network(Network::new_ordered([]))
        .within_boundary(|cfg, state| {
            state
                .actor_states
                .iter()
                .all(|s: &Arc<PaxosActorState>| match s.as_ref() {
                    PaxosActorState::Proposer(ps) => ps.core.proposal().attempt <= cfg.max_attempt,
                    PaxosActorState::Acceptor(_) => true,
                })
        });

    for i in 0..num_acceptors {
        model = model.actor(PaxosActor::Acceptor { id: i });
    }

    for (i, &value) in (0..num_proposers).zip(values.iter().cycle()) {
        model = model.actor(PaxosActor::Proposer {
            id: num_acceptors + i,
            acceptor_ids: acceptor_ids.clone(),
            initial_value: value,
        });
    }

    model = model.property(stateright::Expectation::Always, "Agreement", |_, state| {
        check_agreement(state)
    });

    model = model.property(
        stateright::Expectation::Always,
        "PromiseIntegrity",
        |_, state| check_promise_integrity(state),
    );

    model = model.property(
        stateright::Expectation::Always,
        "Consistency",
        |_, state| check_consistency(state),
    );

    model
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_paxos_single_proposer() {
        let model = paxos_model(1, 3, &[1]);

        let checker = model.checker().threads(num_cpus::get()).spawn_bfs().join();

        checker.assert_properties();
        println!(
            "Single proposer: {} states explored",
            checker.unique_state_count()
        );
    }

    #[test]
    #[ignore = "slow"]
    fn check_paxos_two_proposers() {
        let model = paxos_model_with_config(2, 3, &[1, 2], 2);

        let checker = model.checker().threads(num_cpus::get()).spawn_bfs().join();

        checker.assert_properties();
        println!(
            "Two proposers: {} states explored",
            checker.unique_state_count()
        );
    }
}
