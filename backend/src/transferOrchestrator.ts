/**
 * @file transferOrchestrator.ts
 * Idempotent, retry-safe transfer orchestration service (Issue #1043).
 *
 * A vault transfer is the one operation in this backend that cannot be undone:
 * once `submitVaultOperation` broadcasts an envelope, value has moved. That makes
 * the naive "wrap the RPC call and retry on error" shape actively dangerous —
 * each orchestrator attempt rebuilds the transaction from a fresh account
 * sequence number, so a retry produces a *different* envelope rather than a
 * duplicate of the first one. Retrying an attempt that may already have been
 * broadcast is therefore a double-spend, not a no-op.
 *
 * This module keeps the retry safety and drops the hazard:
 *
 *   1. **Idempotency.** Every transfer is keyed by a caller-supplied idempotency
 *      key and executed through `IdempotencyStore`, so concurrent duplicates
 *      coalesce onto one in-flight submission and completed transfers replay the
 *      original transaction hash. A key reused with different parameters is
 *      rejected as a conflict rather than silently moving different funds.
 *   2. **Write-ahead attempt journal.** Each attempt is journalled as
 *      `in_flight` *before* the submission starts, and the transition is written
 *      as soon as the outcome is known. A crash therefore always leaves evidence
 *      of how far the transfer got, and a replay after the idempotency entry is
 *      lost (pod recycle, Redis eviction) resumes from the journal instead of
 *      submitting a second transaction.
 *   3. **Failure classification.** Failures are classified `terminal`,
 *      `retryable`, or `indeterminate`. Only `retryable` failures — those that
 *      provably left nothing on chain — are retried. `indeterminate` failures
 *      (submission timed out, unexpected post-send status) stop immediately and
 *      are parked for reconciliation, because the safe move when a submission
 *      *may* have landed is to look before submitting again.
 *   4. **Bounded retries.** Capped exponential backoff with full jitter, gated
 *      by the shared Soroban retry budget so a struggling RPC endpoint is not
 *      stampeded, and by the shared circuit breaker so an open circuit fails
 *      fast instead of burning the request's timeout.
 *   5. **Per-attempt timeout.** A hung RPC call is abandoned after
 *      `attemptTimeoutMs` instead of holding the request open indefinitely — and
 *      is treated as indeterminate, never retried.
 *   6. **Terminal-outcome caching.** `IdempotencyStore` only caches successes,
 *      so a doomed key would otherwise re-run its doomed submission on every
 *      retry. The journal replays terminal failures instead.
 *   7. **Observability.** Prometheus counters/histograms plus structured logs
 *      carrying the idempotency key, attempt number and classification.
 *
 * Durability note: like the withdrawal recovery journal and the write-ahead
 * audit log, the attempt journal lives in a bounded in-process map. A
 * `TransferJournalSink` hook is provided so deployments can mirror every
 * transition to durable storage without touching this module.
 *
 * Integration note: this service owns idempotency for the *on-chain submission*.
 * `handleVaultOperation` in `vaultEndpoints.ts` separately wraps the whole HTTP
 * request in `idempotencyStore.execute` under the raw `Idempotency-Key`. Calling
 * this service with that same raw key from inside that wrapper would deadlock on
 * a fingerprint conflict against the request-level entry, so a caller nesting
 * the two must derive a distinct key (for example `` `${key}:onchain` ``). See
 * `backend/docs/TRANSFER_ORCHESTRATION.md`.
 */

import { submitVaultOperation } from './sorobanClient';
import {
  idempotencyStore,
  buildIdempotencyFingerprint,
  IdempotencyStore,
  IdempotentOperationResult,
} from './idempotency';
import { sorobanCircuitBreaker, CircuitOpenError, CircuitBreaker } from './circuitBreaker';
import { sorobanRetryBudget, RetryBudgetService } from './retryBudget';
import { normalizeWalletAddress } from './walletUtils';
import { logger } from './middleware/structuredLogging';
import { getCurrentTraceId } from './tracing';
import {
  transferOrchestrationTotal,
  transferOrchestrationAttemptTotal,
  transferOrchestrationRetryTotal,
  transferOrchestrationReplayTotal,
  transferOrchestrationDuration,
  transferOrchestrationPendingReconciliation,
} from './metrics';

// ─── Types ────────────────────────────────────────────────────────────────────

export type TransferOperationType = 'deposit' | 'withdrawal';

export interface TransferParams {
  operationType: TransferOperationType;
  walletAddress: string;
  amount: string;
  asset: string;
}

/**
 * How a submission failure may be handled.
 *
 * - `retryable`     – provably left nothing on chain; safe to submit again.
 * - `terminal`      – will fail identically however often it is repeated.
 * - `indeterminate` – the submission may have been broadcast. Never retried
 *                     automatically; parked for reconciliation instead.
 */
export type TransferFailureClass = 'retryable' | 'terminal' | 'indeterminate';

/** Journal state of a single transfer attempt chain. */
export type TransferAttemptStatus =
  /** A submission is running right now. */
  | 'in_flight'
  /** The transaction was accepted by the network. */
  | 'submitted'
  /** Every attempt failed with a classification that cannot succeed. */
  | 'failed'
  /** Submission outcome unknown; an operator or reconciler must resolve it. */
  | 'indeterminate'
  /** The circuit breaker refused the submission; nothing was attempted. */
  | 'blocked';

export interface TransferFailureDetail {
  message: string;
  code?: string;
  classification: TransferFailureClass;
  at: string;
}

/** Write-ahead journal entry for one idempotency key. */
export interface TransferJournalEntry {
  idempotencyKey: string;
  fingerprint: string;
  operationType: TransferOperationType;
  walletAddress: string;
  amount: string;
  asset: string;
  status: TransferAttemptStatus;
  /** Number of submissions started (including the one in flight). */
  attempts: number;
  maxAttempts: number;
  /** Set once the network has accepted the transaction. */
  txHash: string | null;
  correlationId: string | null;
  traceId: string | null;
  createdAt: string;
  updatedAt: string;
  lastError: TransferFailureDetail | null;
}

/** Mirror hook so deployments can persist journal transitions durably. */
export type TransferJournalSink = (entry: TransferJournalEntry) => void | Promise<void>;

export interface TransferResult {
  txHash: string;
  /** Whether the hash came from a previous execution rather than a new submission. */
  replayed: boolean;
  /** Where a replayed hash was sourced from. `none` when freshly submitted. */
  replaySource: 'none' | 'idempotency-store' | 'journal';
  /** Submissions started by *this* call (0 when fully replayed). */
  attempts: number;
}

export type TransferSubmitFn = (
  operationType: TransferOperationType,
  walletAddress: string,
  amount: string,
  asset: string,
) => Promise<string>;

export interface TransferOrchestratorOptions {
  maxAttempts?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
  attemptTimeoutMs?: number;
  journalTtlMs?: number;
  journalMaxEntries?: number;
  /** Injected for tests and for callers that wrap the RPC differently. */
  submit?: TransferSubmitFn;
  store?: IdempotencyStore;
  circuitBreaker?: CircuitBreaker;
  retryBudget?: RetryBudgetService;
  journalSink?: TransferJournalSink;
  classifyError?: (err: unknown) => TransferFailureClass | undefined;
  /** Overridable clock/sleep/jitter so tests stay deterministic. */
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

export interface TransferOrchestrationContext {
  correlationId?: string | null;
}

// ─── Errors ───────────────────────────────────────────────────────────────────

/** The idempotency key itself is unusable; nothing was attempted. */
export class TransferKeyError extends Error {
  public readonly statusCode = 400;
  public readonly code = 'INVALID_IDEMPOTENCY_KEY';

  constructor(message: string) {
    super(message);
    this.name = 'TransferKeyError';
  }
}

/** Every attempt failed with a classification that cannot succeed. */
export class TransferFailedError extends Error {
  public readonly statusCode: number;
  public readonly code: string;
  public readonly classification: TransferFailureClass;
  public readonly attempts: number;

  constructor(
    message: string,
    classification: TransferFailureClass,
    attempts: number,
    code = 'TRANSFER_FAILED',
    statusCode = 502,
  ) {
    super(message);
    this.name = 'TransferFailedError';
    this.classification = classification;
    this.attempts = attempts;
    this.code = code;
    this.statusCode = statusCode;
  }
}

/**
 * The submission may or may not have reached the network. Retrying would risk a
 * second transfer, so the transfer is parked and the caller is told to
 * reconcile. Deliberately a 409 rather than a 5xx: the request is not safe to
 * repeat blindly, which is exactly what a 5xx invites clients to do.
 */
export class TransferIndeterminateError extends Error {
  public readonly statusCode = 409;
  public readonly code = 'TRANSFER_OUTCOME_UNKNOWN';
  public readonly idempotencyKey: string;
  public readonly attempts: number;

  constructor(message: string, idempotencyKey: string, attempts: number) {
    super(message);
    this.name = 'TransferIndeterminateError';
    this.idempotencyKey = idempotencyKey;
    this.attempts = attempts;
  }
}

/** A submission exceeded `attemptTimeoutMs`. Always indeterminate. */
export class TransferTimeoutError extends Error {
  public readonly code = 'TRANSFER_ATTEMPT_TIMEOUT';

  constructor(timeoutMs: number) {
    super(`Transfer submission exceeded ${timeoutMs}ms`);
    this.name = 'TransferTimeoutError';
  }
}

// ─── Configuration ────────────────────────────────────────────────────────────

const MAX_IDEMPOTENCY_KEY_LENGTH = 255;
/** Printable ASCII without whitespace — safe as a Redis key segment. */
const IDEMPOTENCY_KEY_PATTERN = /^[\x21-\x7e]+$/;

function intFromEnv(name: string, fallback: number): number {
  const parsed = parseInt(process.env[name] || '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * Terminal Soroban error codes: the contract or the request itself is wrong, so
 * every repeat produces the same rejection. Nothing was broadcast.
 */
const TERMINAL_SOROBAN_CODES = new Set(['INVALID_ADDRESS', 'INVALID_AMOUNT', 'SIMULATION_ERROR']);

/**
 * Indeterminate Soroban error codes: the failure happened at or after the
 * `sendTransaction` boundary, so the envelope may already be in the network's
 * hands. These must never be auto-retried.
 */
const INDETERMINATE_SOROBAN_CODES = new Set(['SUBMISSION_FAILED']);

/**
 * Classify a submission failure.
 *
 * Unknown failures default to `retryable`: `submitVaultOperation` wraps
 * everything that happens at or after the send boundary into the explicit
 * `SUBMISSION_FAILED` / `RPC_ERROR` codes, so an unrecognised error is almost
 * always a pre-send fault (account load, simulation transport, config). Callers
 * that cannot accept that default can narrow it with
 * `TransferOrchestratorOptions.classifyError`.
 */
export function classifyTransferError(err: unknown): TransferFailureClass {
  if (err instanceof TransferFailedError) return err.classification;

  // Classification is deliberately structural rather than `instanceof`-only.
  // `SorobanSimulationError` reaches this module through more than one
  // resolution path (the test harness remaps the `./sorobanClient` specifier,
  // and a bundler or duplicated dependency can do the same in production), so an
  // identity check on the constructor is not a safe basis for deciding whether
  // funds may already have moved.
  const name = errorNameOf(err);
  if (name === 'TransferTimeoutError' || name === 'TransferIndeterminateError') {
    return 'indeterminate';
  }
  if (name === 'TransferKeyError') return 'terminal';

  const code = errorCodeOf(err);
  const statusCode = errorStatusOf(err);

  if (code && INDETERMINATE_SOROBAN_CODES.has(code)) return 'indeterminate';
  if (statusCode === 422 || (code && TERMINAL_SOROBAN_CODES.has(code))) return 'terminal';

  return 'retryable';
}

function errorCodeOf(err: unknown): string | undefined {
  if (err && typeof err === 'object' && 'code' in err) {
    const code = (err as { code?: unknown }).code;
    if (typeof code === 'string') return code;
  }
  return undefined;
}

function errorStatusOf(err: unknown): number | undefined {
  if (err && typeof err === 'object' && 'statusCode' in err) {
    const statusCode = (err as { statusCode?: unknown }).statusCode;
    if (typeof statusCode === 'number') return statusCode;
  }
  return undefined;
}

function errorNameOf(err: unknown): string | undefined {
  if (err && typeof err === 'object' && 'name' in err) {
    const name = (err as { name?: unknown }).name;
    if (typeof name === 'string') return name;
  }
  return undefined;
}

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// ─── Orchestrator ─────────────────────────────────────────────────────────────

export class TransferOrchestrator {
  private readonly maxAttempts: number;
  private readonly initialBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly attemptTimeoutMs: number;
  private readonly journalTtlMs: number;
  private readonly journalMaxEntries: number;

  private readonly submit: TransferSubmitFn;
  private readonly store: IdempotencyStore;
  private readonly circuitBreaker: CircuitBreaker;
  private readonly retryBudget: RetryBudgetService;
  private readonly journalSink?: TransferJournalSink;
  private readonly classifyOverride?: (err: unknown) => TransferFailureClass | undefined;
  private readonly now: () => number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly random: () => number;

  /** Insertion-ordered so eviction can drop the oldest entry cheaply. */
  private readonly journal = new Map<string, TransferJournalEntry>();

  constructor(options: TransferOrchestratorOptions = {}) {
    this.maxAttempts = options.maxAttempts ?? intFromEnv('TRANSFER_MAX_ATTEMPTS', 3);
    this.initialBackoffMs =
      options.initialBackoffMs ?? intFromEnv('TRANSFER_RETRY_INITIAL_DELAY_MS', 250);
    this.maxBackoffMs = options.maxBackoffMs ?? intFromEnv('TRANSFER_RETRY_MAX_DELAY_MS', 4000);
    this.attemptTimeoutMs =
      options.attemptTimeoutMs ?? intFromEnv('TRANSFER_ATTEMPT_TIMEOUT_MS', 20000);
    this.journalTtlMs = options.journalTtlMs ?? intFromEnv('TRANSFER_JOURNAL_TTL_MS', 86400000);
    this.journalMaxEntries =
      options.journalMaxEntries ?? intFromEnv('TRANSFER_JOURNAL_MAX_ENTRIES', 5000);

    this.submit = options.submit ?? submitVaultOperation;
    this.store = options.store ?? idempotencyStore;
    this.circuitBreaker = options.circuitBreaker ?? sorobanCircuitBreaker;
    this.retryBudget = options.retryBudget ?? sorobanRetryBudget;
    this.journalSink = options.journalSink;
    this.classifyOverride = options.classifyError;
    this.now = options.now ?? (() => Date.now());
    this.sleep =
      options.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
    this.random = options.random ?? Math.random;
  }

  // ─── Public API ─────────────────────────────────────────────────────────────

  /**
   * Orchestrate a vault transfer.
   *
   * @param params - transfer details; canonicalised before fingerprinting so
   *                 cosmetic differences (whitespace, wallet casing) reuse the
   *                 same idempotency entry instead of double-submitting.
   * @param idempotencyKey - caller-supplied unique key (e.g. a UUID).
   * @throws TransferKeyError on an unusable key.
   * @throws IdempotencyConflictError when the key was used for other params.
   * @throws TransferIndeterminateError when a submission may have landed.
   * @throws TransferFailedError when every attempt failed recoverably-but-finally.
   */
  async orchestrate(
    params: TransferParams,
    idempotencyKey: string,
    context: TransferOrchestrationContext = {},
  ): Promise<TransferResult> {
    const key = this.assertUsableKey(idempotencyKey);
    const canonical = canonicalizeTransferParams(params);
    const fingerprint = buildIdempotencyFingerprint(canonical);
    const startedAt = this.now();
    const operation = canonical.operationType;

    this.pruneJournal();

    let attemptsUsed = 0;

    try {
      // A journalled outcome outranks the idempotency store: it survives store
      // eviction and, unlike the store, it also records terminal and
      // indeterminate outcomes. Inside the try so a replayed failure is counted
      // under the same outcome label as the original.
      const journalled = this.replayFromJournal(key, fingerprint, operation);
      if (journalled) return journalled;

      const { result, replayed } = await this.store.execute<string>(
        key,
        fingerprint,
        async (): Promise<IdempotentOperationResult<string>> => {
          const submitted = await this.submitWithRetries(key, fingerprint, canonical, context);
          attemptsUsed = submitted.attempts;
          return { statusCode: 200, body: submitted.txHash };
        },
      );

      const outcome: TransferResult = {
        txHash: result.body,
        replayed,
        replaySource: replayed ? 'idempotency-store' : 'none',
        attempts: attemptsUsed,
      };

      if (replayed) {
        transferOrchestrationReplayTotal.inc({ operation, source: 'idempotency-store' });
      } else {
        transferOrchestrationTotal.inc({ operation, outcome: 'submitted' });
        transferOrchestrationDuration.observe(
          { operation, outcome: 'submitted' },
          (this.now() - startedAt) / 1000,
        );
      }

      return outcome;
    } catch (err) {
      const outcome =
        err instanceof TransferIndeterminateError
          ? 'indeterminate'
          : err instanceof CircuitOpenError
            ? 'circuit_open'
            : errorNameOf(err) === 'IdempotencyConflictError'
              ? 'conflict'
              : this.classify(err) === 'terminal'
                ? 'terminal'
                : 'exhausted';

      transferOrchestrationTotal.inc({ operation, outcome });
      transferOrchestrationDuration.observe(
        { operation, outcome },
        (this.now() - startedAt) / 1000,
      );
      throw err;
    }
  }

  /**
   * Transfers whose submission outcome is unknown. Operators (or a reconciler
   * job) must confirm on chain whether the transaction landed before the key is
   * released for another attempt.
   */
  pendingReconciliation(): TransferJournalEntry[] {
    this.pruneJournal();
    return [...this.journal.values()]
      .filter((entry) => entry.status === 'indeterminate')
      .map((entry) => ({ ...entry }));
  }

  /** Read-only journal snapshot for a single key. */
  inspect(idempotencyKey: string): TransferJournalEntry | null {
    const entry = this.journal.get(idempotencyKey);
    return entry ? { ...entry } : null;
  }

  /** Full journal snapshot, newest last. */
  inspectAll(): TransferJournalEntry[] {
    this.pruneJournal();
    return [...this.journal.values()].map((entry) => ({ ...entry }));
  }

  /**
   * Release a journalled key after a human or reconciler has established what
   * really happened on chain. Also drops the idempotency entry so a corrected
   * retry is not answered from cache.
   *
   * @returns true when a journal entry was removed.
   */
  async resolve(idempotencyKey: string): Promise<boolean> {
    const existed = this.journal.delete(idempotencyKey);
    await this.store.deleteKey(idempotencyKey);
    this.publishPendingGauge();
    return existed;
  }

  /** Drop all journal state. Test and admin use only. */
  clearJournal(): void {
    this.journal.clear();
    this.publishPendingGauge();
  }

  // ─── Submission with retries ────────────────────────────────────────────────

  private async submitWithRetries(
    key: string,
    fingerprint: string,
    params: TransferParams,
    context: TransferOrchestrationContext,
  ): Promise<{ txHash: string; attempts: number }> {
    const operation = params.operationType;
    let lastError: unknown;
    let attempts = 0;

    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      attempts = attempt;

      // Write-ahead: the journal records that a submission is about to start, so
      // a crash mid-submission is always visible afterwards.
      this.writeJournal(key, {
        idempotencyKey: key,
        fingerprint,
        operationType: operation,
        walletAddress: params.walletAddress,
        amount: params.amount,
        asset: params.asset,
        status: 'in_flight',
        attempts: attempt,
        maxAttempts: this.maxAttempts,
        txHash: null,
        correlationId: context.correlationId ?? null,
        traceId: getCurrentTraceId() ?? null,
        lastError: null,
      });

      try {
        const txHash = await this.circuitBreaker.execute(() => this.submitWithTimeout(params));

        this.retryBudget.recordAttempt(true);
        transferOrchestrationAttemptTotal.inc({ operation, classification: 'ok' });
        this.patchJournal(key, { status: 'submitted', txHash, lastError: null });

        logger.log('info', 'Transfer submitted', {
          event: 'transfer_submitted',
          idempotencyKey: key,
          operation,
          walletAddress: params.walletAddress,
          attempt,
          txHash,
          traceId: getCurrentTraceId(),
        });

        return { txHash, attempts: attempt };
      } catch (err) {
        lastError = err;

        // An open circuit means no call was made, so nothing is in doubt — but
        // retrying inside this request cannot help either, because the cooldown
        // outlives it. Fail fast and let the caller honour Retry-After.
        if (err instanceof CircuitOpenError) {
          transferOrchestrationAttemptTotal.inc({ operation, classification: 'circuit_open' });
          this.patchJournal(key, {
            status: 'blocked',
            lastError: this.failureDetail(err, 'retryable'),
          });
          logger.log('warn', 'Transfer blocked by open circuit', {
            event: 'transfer_circuit_open',
            idempotencyKey: key,
            operation,
            attempt,
            retryAfterMs: err.retryAfterMs,
            traceId: getCurrentTraceId(),
          });
          throw err;
        }

        const classification = this.classify(err);
        transferOrchestrationAttemptTotal.inc({ operation, classification });

        // Budget accounting mirrors `retryWithBudget` in sorobanClient: the
        // shared budget records one outcome per orchestrated operation, not one
        // per attempt, so an in-flight retry chain cannot drive the success
        // ratio below its own threshold and starve itself. Terminal failures are
        // the caller's fault, not the dependency's, so they are not recorded.
        if (classification === 'indeterminate') {
          this.retryBudget.recordAttempt(false);
          this.patchJournal(key, {
            status: 'indeterminate',
            lastError: this.failureDetail(err, classification),
          });
          logger.log('error', 'Transfer outcome unknown; parked for reconciliation', {
            event: 'transfer_indeterminate',
            idempotencyKey: key,
            operation,
            walletAddress: params.walletAddress,
            attempt,
            error: messageOf(err),
            errorCode: errorCodeOf(err),
            traceId: getCurrentTraceId(),
          });
          throw new TransferIndeterminateError(
            `Transfer submission outcome is unknown and must be reconciled before retrying: ${messageOf(err)}`,
            key,
            attempt,
          );
        }

        if (classification === 'terminal') {
          this.patchJournal(key, {
            status: 'failed',
            lastError: this.failureDetail(err, classification),
          });
          logger.log('warn', 'Transfer failed terminally', {
            event: 'transfer_terminal_failure',
            idempotencyKey: key,
            operation,
            attempt,
            error: messageOf(err),
            errorCode: errorCodeOf(err),
            traceId: getCurrentTraceId(),
          });
          throw err;
        }

        const budgetExhausted = !this.retryBudget.canRetry();
        const lastAttempt = attempt === this.maxAttempts;

        if (lastAttempt || budgetExhausted) {
          this.retryBudget.recordAttempt(false);
          this.patchJournal(key, {
            status: 'failed',
            lastError: this.failureDetail(err, classification),
          });
          logger.log('error', 'Transfer exhausted its retries', {
            event: 'transfer_retries_exhausted',
            idempotencyKey: key,
            operation,
            attempts: attempt,
            maxAttempts: this.maxAttempts,
            budgetExhausted,
            error: messageOf(err),
            retryBudget: this.retryBudget.getStats(),
            traceId: getCurrentTraceId(),
          });
          throw new TransferFailedError(
            budgetExhausted
              ? `Transfer abandoned after ${attempt} attempt(s): retry budget exhausted (${messageOf(err)})`
              : `Transfer failed after ${attempt} attempt(s): ${messageOf(err)}`,
            classification,
            attempt,
            budgetExhausted ? 'TRANSFER_RETRY_BUDGET_EXHAUSTED' : 'TRANSFER_RETRIES_EXHAUSTED',
            503,
          );
        }

        const delayMs = this.backoffFor(attempt);
        transferOrchestrationRetryTotal.inc({ operation });
        this.patchJournal(key, { lastError: this.failureDetail(err, classification) });
        logger.log('warn', 'Transfer attempt failed; retrying', {
          event: 'transfer_retry',
          idempotencyKey: key,
          operation,
          attempt,
          maxAttempts: this.maxAttempts,
          delayMs,
          error: messageOf(err),
          errorCode: errorCodeOf(err),
          traceId: getCurrentTraceId(),
        });
        await this.sleep(delayMs);
      }
    }

    // Unreachable: the loop always throws on its final attempt.
    throw new TransferFailedError(
      `Transfer failed after ${attempts} attempt(s): ${messageOf(lastError)}`,
      'retryable',
      attempts,
      'TRANSFER_RETRIES_EXHAUSTED',
      503,
    );
  }

  /**
   * Run one submission under a hard deadline. A timeout is deliberately *not*
   * cancellable — the underlying RPC call may still be in flight, which is
   * precisely why the outcome is indeterminate.
   */
  private async submitWithTimeout(params: TransferParams): Promise<string> {
    let timer: NodeJS.Timeout | undefined;

    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(
        () => reject(new TransferTimeoutError(this.attemptTimeoutMs)),
        this.attemptTimeoutMs,
      );
      // Never hold the event loop open for a deadline nobody is waiting on.
      timer.unref?.();
    });

    try {
      return await Promise.race([
        this.submit(
          params.operationType,
          params.walletAddress,
          params.amount,
          params.asset,
        ),
        timeout,
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  /** Capped exponential backoff with full jitter. */
  private backoffFor(attempt: number): number {
    const exponential = Math.min(this.maxBackoffMs, this.initialBackoffMs * 2 ** (attempt - 1));
    return Math.max(0, Math.floor(exponential * this.random()));
  }

  private classify(err: unknown): TransferFailureClass {
    return this.classifyOverride?.(err) ?? classifyTransferError(err);
  }

  private failureDetail(err: unknown, classification: TransferFailureClass): TransferFailureDetail {
    return {
      message: messageOf(err),
      code: errorCodeOf(err),
      classification,
      at: new Date(this.now()).toISOString(),
    };
  }

  // ─── Key validation ─────────────────────────────────────────────────────────

  private assertUsableKey(idempotencyKey: string): string {
    if (typeof idempotencyKey !== 'string') {
      throw new TransferKeyError('Idempotency key must be a string');
    }
    const key = idempotencyKey.trim();
    if (!key) {
      throw new TransferKeyError('Idempotency key must not be empty');
    }
    if (key.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
      throw new TransferKeyError(
        `Idempotency key must be at most ${MAX_IDEMPOTENCY_KEY_LENGTH} characters`,
      );
    }
    if (!IDEMPOTENCY_KEY_PATTERN.test(key)) {
      throw new TransferKeyError(
        'Idempotency key must contain only printable non-whitespace ASCII characters',
      );
    }
    return key;
  }

  // ─── Journal ────────────────────────────────────────────────────────────────

  /**
   * Answer a repeat call from the journal when the previous outcome is already
   * known. Guards the case the idempotency store cannot: a completed submission
   * whose cached response was evicted, and terminal/indeterminate outcomes the
   * store never caches at all.
   */
  private replayFromJournal(
    key: string,
    fingerprint: string,
    operation: TransferOperationType,
  ): TransferResult | null {
    const entry = this.journal.get(key);
    if (!entry) return null;

    // A different payload under the same key is a conflict, not a replay. Let
    // IdempotencyStore raise it so the error surface stays in one place.
    if (entry.fingerprint !== fingerprint) return null;

    if (entry.status === 'submitted' && entry.txHash) {
      transferOrchestrationReplayTotal.inc({ operation, source: 'journal' });
      logger.log('info', 'Transfer replayed from journal', {
        event: 'transfer_journal_replay',
        idempotencyKey: key,
        operation,
        txHash: entry.txHash,
        traceId: getCurrentTraceId(),
      });
      return {
        txHash: entry.txHash,
        replayed: true,
        replaySource: 'journal',
        attempts: 0,
      };
    }

    if (entry.status === 'indeterminate') {
      transferOrchestrationReplayTotal.inc({ operation, source: 'journal' });
      throw new TransferIndeterminateError(
        'A previous submission for this idempotency key has an unknown outcome and must be reconciled before retrying',
        key,
        entry.attempts,
      );
    }

    if (entry.status === 'failed' && entry.lastError?.classification === 'terminal') {
      transferOrchestrationReplayTotal.inc({ operation, source: 'journal' });
      throw new TransferFailedError(
        `Transfer previously failed terminally: ${entry.lastError.message}`,
        'terminal',
        entry.attempts,
        entry.lastError.code || 'TRANSFER_FAILED',
        422,
      );
    }

    // `in_flight`, `blocked`, and retryable `failed` entries are all safe to
    // attempt again: nothing landed, or IdempotencyStore will coalesce onto the
    // submission that is still running.
    return null;
  }

  private writeJournal(
    key: string,
    entry: Omit<TransferJournalEntry, 'createdAt' | 'updatedAt'>,
  ): void {
    const timestamp = new Date(this.now()).toISOString();
    const existing = this.journal.get(key);
    const next: TransferJournalEntry = {
      ...entry,
      createdAt: existing?.createdAt ?? timestamp,
      updatedAt: timestamp,
    };
    this.journal.set(key, next);
    this.enforceJournalBound();
    this.emit(next);
  }

  private patchJournal(key: string, patch: Partial<TransferJournalEntry>): void {
    const existing = this.journal.get(key);
    if (!existing) return;
    const next: TransferJournalEntry = {
      ...existing,
      ...patch,
      updatedAt: new Date(this.now()).toISOString(),
    };
    this.journal.set(key, next);
    this.emit(next);
  }

  private emit(entry: TransferJournalEntry): void {
    this.publishPendingGauge();
    if (!this.journalSink) return;
    try {
      const maybePromise = this.journalSink({ ...entry });
      if (maybePromise && typeof (maybePromise as Promise<void>).catch === 'function') {
        // Mirroring is best-effort: a failing sink must never fail a transfer.
        (maybePromise as Promise<void>).catch((err: unknown) => {
          logger.log('warn', 'Transfer journal sink rejected', {
            event: 'transfer_journal_sink_error',
            idempotencyKey: entry.idempotencyKey,
            error: messageOf(err),
          });
        });
      }
    } catch (err) {
      logger.log('warn', 'Transfer journal sink threw', {
        event: 'transfer_journal_sink_error',
        idempotencyKey: entry.idempotencyKey,
        error: messageOf(err),
      });
    }
  }

  /**
   * Drop entries past their TTL — except indeterminate ones, which represent
   * possible unreconciled value movement and must not disappear on a timer.
   * An `in_flight` entry older than the TTL cannot really be in flight
   * (`attemptTimeoutMs` bounds every attempt), so it is prunable like the rest.
   */
  private pruneJournal(): void {
    const cutoff = this.now() - this.journalTtlMs;
    for (const [key, entry] of this.journal) {
      if (entry.status === 'indeterminate') continue;
      if (Date.parse(entry.updatedAt) < cutoff) this.journal.delete(key);
    }
    this.publishPendingGauge();
  }

  private enforceJournalBound(): void {
    while (this.journal.size > this.journalMaxEntries) {
      // Map iteration is insertion-ordered, so this is the oldest key. Skip
      // entries that still need attention rather than losing them silently.
      const evictable = [...this.journal.entries()].find(
        ([, entry]) => entry.status !== 'indeterminate' && entry.status !== 'in_flight',
      );
      if (!evictable) break;
      this.journal.delete(evictable[0]);
      logger.log('warn', 'Transfer journal evicted an entry at capacity', {
        event: 'transfer_journal_evicted',
        idempotencyKey: evictable[0],
        capacity: this.journalMaxEntries,
      });
    }
  }

  private publishPendingGauge(): void {
    let pending = 0;
    for (const entry of this.journal.values()) {
      if (entry.status === 'indeterminate') pending++;
    }
    transferOrchestrationPendingReconciliation.set(pending);
  }
}

// ─── Canonicalisation ─────────────────────────────────────────────────────────

/**
 * Canonical form used for fingerprinting. Stellar addresses are
 * case-insensitive, so `gabc…` and `GABC…` are the same account and must not be
 * able to submit twice under one key. Amount and asset are trimmed but
 * otherwise left verbatim: reinterpreting a caller's amount here would be a
 * silent change to how much money moves.
 */
export function canonicalizeTransferParams(params: TransferParams): TransferParams {
  return {
    operationType: params.operationType,
    walletAddress: normalizeWalletAddress(params.walletAddress),
    amount: String(params.amount ?? '').trim(),
    asset: String(params.asset ?? '').trim(),
  };
}

// ─── Singleton + back-compatible helper ───────────────────────────────────────

export const transferOrchestrator = new TransferOrchestrator();

/**
 * Orchestrates a vault transfer with idempotency.
 *
 * Retained for callers written against the original helper signature. New code
 * should prefer {@link transferOrchestrator} so it can read `replaySource` and
 * `attempts`.
 *
 * @param params - transfer details
 * @param idempotencyKey - unique key supplied by the client (e.g., UUID)
 * @returns the vault operation result and a flag indicating whether the result
 *          was replayed from a previous execution.
 */
export async function orchestrateTransfer(
  params: TransferParams,
  idempotencyKey: string,
): Promise<{ result: IdempotentOperationResult<string>; replayed: boolean }> {
  const outcome = await transferOrchestrator.orchestrate(params, idempotencyKey);
  return {
    result: { statusCode: 200, body: outcome.txHash },
    replayed: outcome.replayed,
  };
}
