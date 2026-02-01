---------------------------- MODULE MultiPaxos ----------------------------
(***************************************************************************)
(* TLA+ specification for Multi-Paxos with signed proposals.               *)
(*                                                                         *)
(* Key features:                                                           *)
(* - Log-based: each round is an independent consensus instance            *)
(* - Proposal ordering: (round, attempt, node_id) lexicographic            *)
(* - Signed proposals: acceptors cannot forge, only echo valid proposals   *)
(* - Learner catch-up: can recover historical rounds from acceptors        *)
(***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Proposers,      \* Set of proposer node IDs
    Acceptors,      \* Set of acceptor node IDs  
    Rounds,         \* Set of round IDs (log slots) to model
    Attempts,       \* Set of attempt IDs (e.g., 0..MaxAttempts)
    Values,         \* Set of possible message values
    NONE            \* Sentinel value representing "no proposal" or "no value"

(***************************************************************************)
(* A Proposal is a record with round, attempt, node_id.                    *)
(* ProposalKey ordering: (round, attempt, node_id) lexicographic.          *)
(***************************************************************************)

Proposals == [round: Rounds, attempt: Attempts, node: Proposers]

\* Lexicographic comparison of proposal keys
\* Returns TRUE if p1 < p2
\* NONE is treated as "less than any proposal" (i.e., no promise/accept yet)
ProposalLT(p1, p2) ==
    IF p1 = NONE THEN p2 /= NONE  \* NONE < any real proposal
    ELSE IF p2 = NONE THEN FALSE  \* real proposal is never < NONE
    ELSE \/ p1.round < p2.round
         \/ (p1.round = p2.round /\ p1.attempt < p2.attempt)
         \/ (p1.round = p2.round /\ p1.attempt = p2.attempt /\ p1.node < p2.node)

ProposalLE(p1, p2) == p1 = p2 \/ ProposalLT(p1, p2)

ProposalGT(p1, p2) == ProposalLT(p2, p1)

ProposalGE(p1, p2) == p1 = p2 \/ ProposalGT(p1, p2)

\* Maximum proposal from a set (or NONE if empty)
MaxProposal(S) ==
    IF S = {} THEN NONE
    ELSE CHOOSE p \in S : \A q \in S : ProposalLE(q, p)

(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLES
    \* Per-acceptor, per-round state
    promised,       \* promised[a][r] = highest proposal promised by acceptor a for round r (or NONE)
    accepted,       \* accepted[a][r] = <<proposal, value>> accepted by acceptor a for round r (or NONE)
    
    \* Per-proposer state (for modeling)
    proposerAttempt, \* proposerAttempt[p][r] = current attempt number for proposer p in round r
    
    \* Learned values (for invariant checking)
    learned         \* learned[r] = set of values that have been learned for round r

vars == <<promised, accepted, proposerAttempt, learned>>

(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeOK ==
    /\ promised \in [Acceptors -> [Rounds -> Proposals \cup {NONE}]]
    /\ accepted \in [Acceptors -> [Rounds -> (Proposals \times Values) \cup {NONE}]]
    /\ proposerAttempt \in [Proposers -> [Rounds -> Attempts]]
    /\ learned \in [Rounds -> SUBSET Values]

(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

Init ==
    /\ promised = [a \in Acceptors |-> [r \in Rounds |-> NONE]]
    /\ accepted = [a \in Acceptors |-> [r \in Rounds |-> NONE]]
    /\ proposerAttempt = [p \in Proposers |-> [r \in Rounds |-> 0]]
    /\ learned = [r \in Rounds |-> {}]

(***************************************************************************)
(* HELPER DEFINITIONS                                                      *)
(***************************************************************************)

\* Quorum: strict majority of acceptors
Quorum == {Q \in SUBSET Acceptors : Cardinality(Q) * 2 > Cardinality(Acceptors)}

\* Get the accepted proposal for a round at an acceptor (or NONE)
AcceptedProposal(a, r) ==
    IF accepted[a][r] = NONE THEN NONE
    ELSE accepted[a][r][1]

\* Get the accepted value for a round at an acceptor (or NONE)  
AcceptedValue(a, r) ==
    IF accepted[a][r] = NONE THEN NONE
    ELSE accepted[a][r][2]

\* Highest accepted proposal in a set of acceptors for a round
HighestAccepted(Q, r) ==
    LET accProps == {AcceptedProposal(a, r) : a \in Q} \ {NONE}
    IN MaxProposal(accProps)

\* Value associated with highest accepted proposal
HighestAcceptedValue(Q, r) ==
    LET highest == HighestAccepted(Q, r)
    IN IF highest = NONE THEN NONE
       ELSE CHOOSE v \in Values : 
            \E a \in Q : accepted[a][r] = <<highest, v>>

(***************************************************************************)
(* ACTIONS                                                                 *)
(***************************************************************************)

\* Phase 1a: Proposer sends Prepare
\* (Modeled implicitly - proposer creates a proposal)
\* 
\* Phase 1b: Acceptor promises
\* An acceptor promises to a proposal if:
\* - The proposal is >= any previously promised proposal for this round
\* - The proposal is >= any previously accepted proposal for this round
Promise(a, p) ==
    LET r == p.round IN
    \* p must be >= any previously promised or accepted proposal
    /\ ProposalGE(p, promised[a][r])
    /\ ProposalGE(p, AcceptedProposal(a, r))
    /\ promised' = [promised EXCEPT ![a][r] = p]
    /\ UNCHANGED <<accepted, proposerAttempt, learned>>

\* Phase 2a: Proposer sends Accept (after gathering quorum of promises)
\* Phase 2b: Acceptor accepts
\* 
\* Key Paxos constraints modeled:
\* 1. Proposal must be >= any promised/accepted proposal at this acceptor
\* 2. Proposer consistency: same proposal implies same value everywhere
\* 3. Value selection: proposer must use highest accepted value from a promising quorum
\*    (This is the core Paxos safety mechanism)
Accept(a, p, v) ==
    LET r == p.round IN
    \* p must be >= any previously promised or accepted proposal at this acceptor
    /\ ProposalGE(p, promised[a][r])
    /\ ProposalGE(p, AcceptedProposal(a, r))
    \* Proposer consistency: if this proposal was already accepted anywhere, value must match
    /\ \A aa \in Acceptors : 
        accepted[aa][r] /= NONE /\ accepted[aa][r][1] = p => accepted[aa][r][2] = v
    \* Paxos value selection rule: there must exist a quorum that promised this proposal,
    \* and v must be the value from the highest accepted proposal in that quorum
    \* (or any value if no one in the quorum had accepted anything)
    /\ \E Q \in Quorum :
        \* All in Q have promised exactly p (responded to Prepare(p))
        /\ \A aa \in Q : promised[aa][r] = p
        \* v must be from highest accepted in Q, or free choice if none accepted
        /\ LET highest == HighestAccepted(Q, r)
           IN highest = NONE \/ HighestAcceptedValue(Q, r) = v
    /\ accepted' = [accepted EXCEPT ![a][r] = <<p, v>>]
    /\ promised' = [promised EXCEPT ![a][r] = p]  \* Also updates promise
    /\ UNCHANGED <<proposerAttempt, learned>>

\* Learn: A value is learned when a quorum of acceptors have accepted it
\* with the same proposal key
Learn(r, v) ==
    /\ \E Q \in Quorum :
        \E p \in Proposals :
            \A a \in Q : accepted[a][r] = <<p, v>>
    /\ learned' = [learned EXCEPT ![r] = learned[r] \cup {v}]
    /\ UNCHANGED <<promised, accepted, proposerAttempt>>

\* Proposer increments attempt after rejection
\* (Models the proposer seeing a higher proposal and backing off)
IncrementAttempt(p, r) ==
    /\ proposerAttempt[p][r] + 1 \in Attempts
    /\ proposerAttempt' = [proposerAttempt EXCEPT ![p][r] = @ + 1]
    /\ UNCHANGED <<promised, accepted, learned>>

(***************************************************************************)
(* COMPLETE PROPOSER ACTIONS (for realistic modeling)                      *)
(***************************************************************************)

\* A proposer runs Phase 1 and Phase 2 together:
\* 1. Creates proposal with current attempt
\* 2. Gets promises from a quorum
\* 3. Picks value (own or highest accepted)
\* 4. Gets accepts from a quorum
\*
\* This models the "happy path" - in practice, failures cause retries
\* with IncrementAttempt

ProposeAndAccept(proposer, r, myValue) ==
    LET proposal == [round |-> r, 
                     attempt |-> proposerAttempt[proposer][r], 
                     node |-> proposer]
    IN
    \* There exists a quorum that will promise and accept
    \E Q \in Quorum :
        \* All acceptors in Q can promise this proposal
        /\ \A a \in Q : 
            /\ ProposalGE(proposal, promised[a][r])
            /\ ProposalGE(proposal, AcceptedProposal(a, r))
        \* Determine the value to propose
        /\ LET valueToAccept == 
               IF HighestAccepted(Q, r) = NONE 
               THEN myValue
               ELSE HighestAcceptedValue(Q, r)
           IN
           \* Update all acceptors in quorum atomically (abstraction)
           /\ promised' = [a \in Acceptors |-> 
                [rr \in Rounds |-> 
                    IF a \in Q /\ rr = r THEN proposal
                    ELSE promised[a][rr]]]
           /\ accepted' = [a \in Acceptors |->
                [rr \in Rounds |->
                    IF a \in Q /\ rr = r THEN <<proposal, valueToAccept>>
                    ELSE accepted[a][rr]]]
           /\ UNCHANGED <<proposerAttempt, learned>>

(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \/ \E a \in Acceptors, p \in Proposals : Promise(a, p)
    \/ \E a \in Acceptors, p \in Proposals, v \in Values : Accept(a, p, v)
    \/ \E r \in Rounds, v \in Values : Learn(r, v)
    \/ \E p \in Proposers, r \in Rounds : IncrementAttempt(p, r)
    \* Optional: use ProposeAndAccept for more constrained exploration
    \* \/ \E p \in Proposers, r \in Rounds, v \in Values : ProposeAndAccept(p, r, v)

(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* AGREEMENT: At most one value can be learned per round
\* This is the core Paxos safety property
Agreement == 
    \A r \in Rounds : Cardinality(learned[r]) <= 1

\* VALIDITY: Any learned value must have been proposed
\* (Trivially true since we only accept values from Values)
Validity ==
    \A r \in Rounds : learned[r] \subseteq Values

\* CONSISTENCY: If a quorum has accepted (p, v) for round r,
\* then no quorum can accept (p', v') where p' > p and v' /= v
\* This is the key invariant that ensures agreement
Consistency ==
    \A r \in Rounds :
        \A Q1, Q2 \in Quorum :
            \A p1, p2 \in Proposals :
                \A v1, v2 \in Values :
                    (/\ \A a \in Q1 : accepted[a][r] = <<p1, v1>>
                     /\ \A a \in Q2 : accepted[a][r] = <<p2, v2>>
                     /\ ProposalGT(p2, p1))
                    => v1 = v2

\* PROMISE INTEGRITY: An acceptor's promise is >= any accepted proposal
PromiseIntegrity ==
    \A a \in Acceptors, r \in Rounds :
        AcceptedProposal(a, r) /= NONE =>
            (promised[a][r] /= NONE /\ ProposalGE(promised[a][r], AcceptedProposal(a, r)))

\* MONOTONIC PROMISES: Once promised, only higher proposals can be promised
\* (This is maintained by the Promise action's precondition)

\* MONOTONIC ACCEPTS: Once accepted, only higher proposals can be accepted
\* (This is maintained by the Accept action's precondition)

(***************************************************************************)
(* LIVENESS (requires fairness assumptions)                                *)
(***************************************************************************)

\* Eventually some value is learned for each round
\* (Only holds under strong fairness and no permanent failures)
\* Liveness == \A r \in Rounds : <>(learned[r] /= {})

(***************************************************************************)
(* SPECIFICATION                                                           *)
(***************************************************************************)

Spec == Init /\ [][Next]_vars

\* With weak fairness on all actions
FairSpec == Spec /\ WF_vars(Next)

=============================================================================
