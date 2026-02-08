# Multi-Paxos TLA+ Specification

This directory contains a TLA+ specification for the Multi-Paxos implementation.

## Files

- `MultiPaxos.tla` - The TLA+ specification
- `MultiPaxos.cfg` - TLC model checker configuration

## Running the Model Checker

### Install TLA+ Tools

```bash
# Download TLA+ tools
wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

# Or use the VS Code TLA+ extension (recommended)
```

### Run TLC

```bash
# Basic run
java -jar tla2tools.jar -config MultiPaxos.cfg MultiPaxos.tla

# With more workers for faster checking
java -jar tla2tools.jar -workers auto -config MultiPaxos.cfg MultiPaxos.tla

# Generate states to a file (for larger models)
java -jar tla2tools.jar -workers auto -checkpoint 60 -config MultiPaxos.cfg MultiPaxos.tla
```

## Specification Overview

### State Variables

| TLA+ Variable | Rust Equivalent |
|---------------|-----------------|
| `promised[a][r]` | `SharedAcceptorState.inner[r].promised` |
| `accepted[a][r]` | `SharedAcceptorState.inner[r].accepted` |
| `proposerAttempt[p][r]` | `attempt` in `run_proposer` |
| `learned[r]` | Implicit in `Learner::apply()` being called |

### Actions Mapping

| TLA+ Action | Rust Code |
|-------------|-----------|
| `Promise(a, p)` | `AcceptorStateStore::promise()` in `state.rs` |
| `Accept(a, p, v)` | `AcceptorStateStore::accept()` in `state.rs` |
| `Learn(r, v)` | `QuorumTracker::track()` returning quorum → `Learner::apply()` |
| `IncrementAttempt(p, r)` | `ProposeResult::Rejected` → increment `attempt` |

### Key Invariants

1. **Agreement**: At most one value learned per round
   - Rust: `QuorumTracker` only returns when quorum reached with same proposal key

2. **Consistency**: Higher proposals must carry forward accepted values
   - Rust: `run_proposal` checks `highest_accepted` during prepare phase

3. **PromiseIntegrity**: Promise >= any accepted proposal
   - Rust: `accept()` updates both `accepted` and `promised`

## Validating Implementation Matches Spec

### 1. Property Tests

Add these assertions to turmoil tests:

```rust
// Agreement: check all learners have same value per round
fn check_agreement(learners: &[LearnerState]) {
    for round in all_rounds {
        let values: HashSet<_> = learners
            .iter()
            .filter_map(|l| l.learned.get(&round))
            .collect();
        assert!(values.len() <= 1, "Agreement violated at round {}", round);
    }
}
```

### 2. Trace Validation

Record acceptor state transitions and verify against TLA+ allowed transitions:

```rust
#[derive(Debug)]
enum AcceptorEvent {
    Promise { round: u64, proposal: ProposalKey },
    Accept { round: u64, proposal: ProposalKey, value: Value },
}

// Validate: Promise proposal >= previous promise and >= accepted
// Validate: Accept proposal >= promised
```

### 3. Stateright Model

For tighter integration, implement a Stateright model that mirrors this TLA+ spec
and checks your actual Rust types.

## Model Checking Tips

1. **Start small**: Use 2 proposers, 3 acceptors, 2 rounds, 3 attempts
2. **Increase gradually**: Add more rounds/attempts if no bugs found
3. **Symmetry**: Use symmetry sets to reduce state space
4. **Deadlock**: Disable deadlock checking initially (Paxos can deadlock without liveness)

## References

- [Lamport's Paxos TLA+ Spec](https://github.com/tlaplus/Examples/tree/master/specifications/Paxos)
- [TLA+ Video Course](https://lamport.azurewebsites.net/video/videos.html)
- [Practical TLA+](https://www.learntla.com/)
