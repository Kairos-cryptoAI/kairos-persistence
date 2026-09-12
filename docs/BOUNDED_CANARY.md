# Bounded DEV canary operational admission

This is internal operational storage, not a change to the frozen trading contracts,
strategy, evaluator or historical evidence. It does not qualify PAPER by itself.

`CanarySessionRepository.begin_readonly` creates a run at the database clock.
`append_observation` accepts a complete five-symbol measurement (including failures)
only near that clock, at its fixed 5–60 second cadence. Samples form a SHA-256 chain.
The internal recorder is trusted to query the real sources; a chain is an integrity
proof, not independent authentication of a venue. The live recorder is a separate
integration boundary and is not provided by this storage patch.

`certify_readonly` derives a receipt from those persisted rows: at least 24 actual
hours, full chain, per-symbol time-weighted availability >=99%, p95 absolute basis,
spread and slippage <=25 bps, observed book age <=5s, timestamp skew <=2s, no
unrecovered gaps/reconciliation drift or recorded execution mutations. Missing
time counts as unavailable. The window must end with fresh nonempty books for all
five symbols. Legacy summary JSON or an `accepted=true` field cannot arm a session.

Arming re-verifies the complete proof and binds it to the database instance,
`kairos-paper-gate`, exact DEV URL/chain, local/remote account and configuration/code
fingerprints. The operational receipt TTL is **one hour** by explicit conservative
policy, not a venue guarantee. Callers can only tighten it to a positive value <=1h.
Current account/book/risk checks remain mandatory after acceptance.

Each receipt can arm only one session. Its fixed plan includes all five symbols
and requests stop, target, timeout, restart and entry-cancel scenarios. A request
does not prove that the market actually executed that scenario. The deadline is
set once by the DB clock, at most two hours. At most ten slots are allowed.

`PaperCanaryArmRepository.arm(..., session_id=..., slot_id=...)` inserts an attempt,
the existing exact arm and the review audit/outbox in one transaction. The attempt
is charged at arm time, even if Risk later refuses it or entry expires. Exact replay
returns the same row and does not recharge; reuse of a slot with different bytes
fails. Old unbound calls and historical arms fail closed. No public message IDs or
metadata are rewritten. Slots run in fixed order, one outstanding attempt in the
only active session for the isolated project/database. Changing local/remote
accounts cannot mint a second simultaneous allowance. A nonterminal PAPER DEV
trade also blocks arming a new session. Migration016 fails closed on conflicting
old active sessions instead of silently retiring evidence or exposure.

`refresh` can settle an attempt only from an exact durable Risk rejection, an
unconsumed expired arm, or a terminal reconciled trade with no unresolved journal
effects. `stop` enters DRAINING: it blocks new authorization but does not cancel
protective orders or interrupt recovery. Restart never resets the counter, receipt,
plan, deadline or stop reason. This slice has no method that marks a session PASSED.

Execution must call `bind_entry(decision=..., expected_scope=..., effect_id=...)`
before preparing the journal effect. This only creates an immutable internal
decision/trade/effect binding, not mutation authority. Immediately before the
actual venue mutation it must enter `async with final_dispatch(...)`. The helper
requires the exact durable PREPARED entry request and claims the attempt once.

The lock order is execution account -> trade -> canary dispatch session lock ->
short session-row transaction. `stop` takes the same dispatch lock, then the row;
it never takes execution account/trade locks. The final helper keeps a dedicated
connection's session-level advisory lock across the caller's bounded call (at
most30s). Its short claim transaction **commits before yielding**. A process loss
before or after the venue call cannot roll back that claim. A second dispatch is
refused even if this sacrifices an unsent slot. An exception/timeout requires
journal reconciliation, not automatic resubmission. The helper itself never
calls the venue. Connection release unlocks the dispatch boundary.

`recovery_binding(...)` is a read-only exact-binding lookup permitted after a stop
or expiry. It cannot bind a new effect or issue a mutation lease. Existing protective
exits/reconciliation must not be routed through the new-entry helper.

The risk module provides `python -m kairos_risk.canary_runner` with `preview-plan`,
`arm-session`, `status`, `stop` and single-step `submit-next`. It does not run a
background retry loop. There is no receipt-import/acceptance-override command.
Publishing through the old individual-canary command also requires session and
slot identifiers; leaving either absent fails before input loading.

Engine wiring of the helper and a live receipt recorder are separate boundaries;
the post-hoc acceptance evaluator must still prove actual session-scoped coverage.
Until those boundaries and real DEV observations/canary/soak pass, qualification
remains false. No paid feeds/models are used by this controller.
