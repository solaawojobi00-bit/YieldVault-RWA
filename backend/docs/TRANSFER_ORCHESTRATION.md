# Idempotent Retry-Safe Transfer Orchestration

**Issue:** #1043
**Module:** [`backend/src/transferOrchestrator.ts`](../src/transferOrchestrator.ts)
**Tests:** [`backend/src/__tests__/transferOrchestrator.test.ts`](../src/__tests__/transferOrchestrator.test.ts)

## Why this exists

A vault transfer is the only operation in this backend that cannot be taken back.
Once `submitVaultOperation` broadcasts an envelope, value has moved on chain.

That makes the obvious resilience shape — "wrap the RPC call, retry on error" —
actively dangerous here. Every orchestrator attempt rebuilds the transaction from
a freshly loaded account sequence number, so a retry produces a **different**
envelope rather than a duplicate of the first one. The network will happily
include both. Retrying a submission that may already have been broadcast is
therefore a double-spend, not a harmless repeat.

> Re-submitting the *identical signed envelope* is safe — Stellar deduplicates by
> transaction hash — which is why `sorobanClient.retryWithBudget` may retry
> `sendTransaction` on the envelope it already built. That safety does **not**
> extend one level up, because this module rebuilds and re-signs.

This service keeps retry safety and removes the hazard.

## Guarantees

| Guarantee | Mechanism |
|---|---|
| One transfer per idempotency key | `IdempotencyStore.execute` + write-ahead attempt journal |
| Concurrent duplicates coalesce | `IdempotencyStore` in-flight promise sharing |
| Key reuse with different params is rejected | Fingerprint comparison → `IdempotencyConflictError` (409) |
| A retry never duplicates a broadcast transaction | `indeterminate` classification is never retried |
| Bounded retry pressure on the RPC endpoint | Capped exponential backoff + full jitter, shared retry budget |
| An open circuit fails fast | Shared `sorobanCircuitBreaker` |
| A hung RPC call cannot hold a request open | Per-attempt deadline (`attemptTimeoutMs`) |
| A doomed key does not re-run its doomed submission | Terminal outcomes replay from the journal |
| Ambiguous outcomes are never silently dropped | Parked as `indeterminate` + gauge + operator queue |

## Failure classification

Classification is the heart of the module: it decides whether a retry is safe.

| Class | Meaning | Behaviour |
|---|---|---|
| `retryable` | The failure provably left nothing on chain | Retried with backoff, up to `TRANSFER_MAX_ATTEMPTS` |
| `terminal` | Will fail identically however often it is repeated | Fails immediately; replayed from the journal on repeat calls |
| `indeterminate` | The submission may have been broadcast | **Never** retried; parked for reconciliation |

Mapping from Soroban error codes:

| Code / condition | Class | Rationale |
|---|---|---|
| `INVALID_ADDRESS`, `INVALID_AMOUNT`, any `422` | `terminal` | The request is malformed; nothing was built or sent |
| `SIMULATION_ERROR` | `terminal` | The contract rejected the invocation; simulation is pre-send |
| `RESTORE_REQUIRED` (`503`) | `retryable` | Ledger entries need restoring; nothing was sent |
| `RPC_ERROR` | `retryable` | The RPC endpoint rejected the envelope before inclusion |
| `SUBMISSION_FAILED` | `indeterminate` | Raised at/after the `sendTransaction` boundary |
| Attempt timeout | `indeterminate` | The call may still be in flight — that is what a timeout means |
| Anything unrecognised | `retryable` | `submitVaultOperation` funnels post-send faults into the explicit codes above, so unknown errors are almost always pre-send |

Two deliberate choices:

- **Classification is structural, not `instanceof`-based.** `SorobanSimulationError`
  reaches this module through more than one resolution path (the Jest harness
  remaps the `./sorobanClient` specifier; a bundler or duplicated dependency can
  do the same in production). Deciding whether funds may have moved must not hinge
  on constructor identity.
- **The `retryable` default is a judgement call, not a proof.** Callers that
  cannot accept it can narrow classification with the `classifyError` option,
  which is consulted before the built-in rules.

## Attempt journal

Every attempt is journalled as `in_flight` **before** the submission starts, and
the transition is written as soon as the outcome is known. A crash therefore
always leaves evidence of how far the transfer got.

The journal answers two cases the idempotency store cannot:

1. **A completed submission whose cached response was evicted** (pod recycle,
   Redis eviction, TTL). The store would happily let a second transaction be
   submitted; the journal replays the original hash instead.
2. **Terminal and indeterminate outcomes.** `IdempotencyStore` only caches
   successes, so a doomed key would re-run its doomed submission on every retry.

States: `in_flight` → `submitted` | `failed` | `indeterminate` | `blocked`.

Entries are pruned after `TRANSFER_JOURNAL_TTL_MS` and bounded to
`TRANSFER_JOURNAL_MAX_ENTRIES`, **except** `indeterminate` entries — those
represent possible unreconciled value movement and are never dropped by a timer
or by capacity pressure.

### Durability

Like the withdrawal recovery journal and the write-ahead audit log, this journal
lives in a bounded in-process map. Deployments that need cross-replica durability
supply a `TransferJournalSink`, which receives every transition and can mirror it
to Postgres or Redis without changes to this module. A sink that throws or
rejects is logged and ignored — mirroring must never fail a transfer.

## Reconciling an indeterminate transfer

An `indeterminate` outcome means: *look on chain before doing anything else.*

```ts
import { transferOrchestrator } from './transferOrchestrator';

// 1. What is parked?
const pending = transferOrchestrator.pendingReconciliation();

// 2. Confirm on chain whether each transaction landed, then either record the
//    real hash downstream, or — having established nothing landed — release the
//    key so a corrected attempt is allowed.
await transferOrchestrator.resolve(pending[0].idempotencyKey);
```

`resolve()` drops the journal entry *and* the idempotency entry, so the next call
with that key executes a fresh submission rather than replaying a stale response.
Until it is called, every repeat of that key throws
`TransferIndeterminateError` (HTTP 409) — deliberately not a 5xx, because a 5xx
invites clients to retry, which is the one thing that must not happen here.

## Usage

```ts
import { transferOrchestrator } from './transferOrchestrator';

const outcome = await transferOrchestrator.orchestrate(
  { operationType: 'deposit', walletAddress, amount, asset },
  idempotencyKey,
  { correlationId },
);

outcome.txHash;        // on-chain transaction hash
outcome.replayed;      // true when no new submission was made
outcome.replaySource;  // 'none' | 'idempotency-store' | 'journal'
outcome.attempts;      // submissions started by this call (0 when replayed)
```

`orchestrateTransfer(params, key)` is retained for callers written against the
original helper signature and returns `{ result, replayed }`.

### Idempotency keys

Keys are trimmed, then required to be non-empty, at most 255 characters, and
printable non-whitespace ASCII (safe as a Redis key segment). Anything else
throws `TransferKeyError` (HTTP 400) before a submission is attempted.

Parameters are canonicalised before fingerprinting: the wallet address is
upper-cased (Stellar addresses are case-insensitive, so `gabc…` and `GABC…` must
not be able to submit twice under one key) and fields are trimmed. The amount is
**not** reinterpreted — silently renormalising a caller's amount would be a
silent change to how much money moves.

### Integrating with the HTTP layer

This service owns idempotency for the **on-chain submission**.
`handleVaultOperation` in `vaultEndpoints.ts` separately wraps the whole HTTP
request in `idempotencyStore.execute` under the raw `Idempotency-Key`.

Calling this service with that same raw key from inside that wrapper would
deadlock on a fingerprint conflict against the request-level entry — the outer
entry is keyed on the request body, the inner on the transfer params. A caller
that nests the two must derive a distinct key:

```ts
await transferOrchestrator.orchestrate(params, `${idempotencyKey}:onchain`);
```

The endpoints are intentionally left on their existing path in this change; the
service is standalone and directly usable, and rewiring the request-level
idempotency contract is a separate, larger decision.

### Error surface

| Error | HTTP | When |
|---|---|---|
| `TransferKeyError` | 400 | Unusable idempotency key; nothing attempted |
| `IdempotencyConflictError` | 409 | Key reused with different transfer parameters |
| `TransferIndeterminateError` | 409 | Submission outcome unknown; reconcile before retrying |
| `TransferFailedError` (`terminal`) | 422 | Replay of a previously terminal failure |
| `TransferFailedError` (`retryable`) | 503 | Retries or retry budget exhausted |
| `CircuitOpenError` | 503 | Circuit open; honour `retryAfterMs` |
| `SorobanSimulationError` | as carried | First terminal failure, rethrown unchanged |

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `TRANSFER_MAX_ATTEMPTS` | `3` | Total submissions per call, including the first |
| `TRANSFER_RETRY_INITIAL_DELAY_MS` | `250` | First backoff before jitter |
| `TRANSFER_RETRY_MAX_DELAY_MS` | `4000` | Backoff ceiling before jitter |
| `TRANSFER_ATTEMPT_TIMEOUT_MS` | `20000` | Per-attempt deadline; a breach is indeterminate |
| `TRANSFER_JOURNAL_TTL_MS` | `86400000` | Retention for resolved journal entries |
| `TRANSFER_JOURNAL_MAX_ENTRIES` | `5000` | Journal capacity |

Retry pressure and fail-fast behaviour additionally honour the shared
`RETRY_BUDGET_*` and `CIRCUIT_BREAKER_*` settings.

Backoff uses **full jitter**: the delay is uniformly random in
`[0, min(maxBackoffMs, initialBackoffMs · 2^(attempt-1)))`, so concurrent
retries after a shared outage spread out instead of arriving in a thundering
herd.

### Retry budget accounting

The shared `sorobanRetryBudget` records **one outcome per orchestrated
operation**, not one per attempt — matching `retryWithBudget` in
`sorobanClient.ts`. Recording each mid-chain retry would drive the budget's own
success ratio below its threshold and starve the chain that is still running.
Terminal failures are not recorded at all: they are the caller's fault, not a
signal about dependency health.

## Metrics

| Metric | Type | Labels |
|---|---|---|
| `transfer_orchestration_total` | Counter | `operation`, `outcome` (`submitted`, `terminal`, `exhausted`, `indeterminate`, `circuit_open`, `conflict`) |
| `transfer_orchestration_attempt_total` | Counter | `operation`, `classification` (`ok`, `retryable`, `terminal`, `indeterminate`, `circuit_open`) |
| `transfer_orchestration_retry_total` | Counter | `operation` |
| `transfer_orchestration_replay_total` | Counter | `operation`, `source` (`idempotency-store`, `journal`) |
| `transfer_orchestration_duration_seconds` | Histogram | `operation`, `outcome` |
| `transfer_orchestration_pending_reconciliation` | Gauge | — |

**Alert on `transfer_orchestration_pending_reconciliation > 0`.** Every unit is a
transfer that may have moved funds without the ledger knowing.

## Structured log events

`transfer_submitted`, `transfer_retry`, `transfer_retries_exhausted`,
`transfer_terminal_failure`, `transfer_indeterminate`, `transfer_circuit_open`,
`transfer_journal_replay`, `transfer_journal_evicted`,
`transfer_journal_sink_error`.

Each carries the idempotency key, operation type, attempt number and the current
trace ID.

## Related

- [Withdrawal Partial-Failure Recovery](./WITHDRAWAL_PARTIAL_FAILURE_RECOVERY.md) —
  multi-step saga recovery for the withdrawal path
- [`backend/src/idempotency.ts`](../src/idempotency.ts) — the Redis-backed key store
- [`backend/src/circuitBreaker.ts`](../src/circuitBreaker.ts),
  [`backend/src/retryBudget.ts`](../src/retryBudget.ts) — shared resilience primitives
