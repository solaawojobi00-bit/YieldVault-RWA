// src/__tests__/transferOrchestrator.test.ts
/**
 * Tests for the idempotent, retry-safe transfer orchestration service (#1043).
 *
 * The submit function, clock, sleep and jitter are injected rather than module
 * mocked. jest.config.js remaps the `./sorobanClient` specifier to a fixture, so
 * a `jest.mock('../sorobanClient')` in this file would install a spy on a module
 * the orchestrator never loads — which is exactly why the previous version of
 * this suite could not observe submissions.
 */
import {
  TransferOrchestrator,
  TransferFailedError,
  TransferIndeterminateError,
  TransferKeyError,
  TransferTimeoutError,
  classifyTransferError,
  canonicalizeTransferParams,
  orchestrateTransfer,
  type TransferParams,
  type TransferSubmitFn,
  type TransferJournalEntry,
} from '../transferOrchestrator';
import { IdempotencyStore, IdempotencyConflictError } from '../idempotency';
import { CircuitBreaker, CircuitOpenError } from '../circuitBreaker';
import { RetryBudgetService } from '../retryBudget';
import { SorobanSimulationError } from '../sorobanClient';

const PARAMS: TransferParams = {
  operationType: 'deposit',
  walletAddress: 'GABCDEF1234567890',
  amount: '1000',
  asset: 'USDC',
};

/** Fresh collaborators per test so no state leaks between cases. */
function build(
  submit: TransferSubmitFn,
  overrides: Partial<ConstructorParameters<typeof TransferOrchestrator>[0]> = {},
) {
  const sleeps: number[] = [];
  const orchestrator = new TransferOrchestrator({
    submit,
    store: new IdempotencyStore(60_000),
    circuitBreaker: new CircuitBreaker({ failureThreshold: 1000 }),
    retryBudget: new RetryBudgetService({
      maxRetries: 100,
      failureThreshold: 100,
      minSuccessRate: 0,
    }),
    maxAttempts: 3,
    initialBackoffMs: 100,
    maxBackoffMs: 1000,
    // Deterministic: full jitter always picks the top of the range.
    random: () => 1,
    sleep: async (ms: number) => {
      sleeps.push(ms);
    },
    ...overrides,
  });
  return { orchestrator, sleeps };
}

function sorobanError(code: string, statusCode = 502) {
  return new SorobanSimulationError(`boom: ${code}`, code, statusCode);
}

describe('classifyTransferError', () => {
  it.each([
    { code: 'INVALID_ADDRESS', statusCode: 422, expected: 'terminal' },
    { code: 'INVALID_AMOUNT', statusCode: 422, expected: 'terminal' },
    { code: 'SIMULATION_ERROR', statusCode: 502, expected: 'terminal' },
    { code: 'RESTORE_REQUIRED', statusCode: 503, expected: 'retryable' },
    { code: 'RPC_ERROR', statusCode: 502, expected: 'retryable' },
    { code: 'INTERNAL_ERROR', statusCode: 502, expected: 'retryable' },
    { code: 'SUBMISSION_FAILED', statusCode: 502, expected: 'indeterminate' },
  ])('classifies $code as $expected', ({ code, statusCode, expected }) => {
    expect(classifyTransferError(sorobanError(code, statusCode))).toBe(expected);
  });

  it('treats a submission timeout as indeterminate, never retryable', () => {
    expect(classifyTransferError(new TransferTimeoutError(10))).toBe('indeterminate');
  });

  it('defaults unknown failures to retryable', () => {
    expect(classifyTransferError(new Error('socket hang up'))).toBe('retryable');
  });

  it('preserves the classification carried by a TransferFailedError', () => {
    const err = new TransferFailedError('nope', 'terminal', 2);
    expect(classifyTransferError(err)).toBe('terminal');
  });
});

describe('canonicalizeTransferParams', () => {
  it('upper-cases the wallet address and trims surrounding whitespace', () => {
    expect(
      canonicalizeTransferParams({
        operationType: 'withdrawal',
        walletAddress: '  gabcdef1234567890 ',
        amount: ' 250 ',
        asset: ' USDC ',
      }),
    ).toEqual({
      operationType: 'withdrawal',
      walletAddress: 'GABCDEF1234567890',
      amount: '250',
      asset: 'USDC',
    });
  });

  it('does not reinterpret the amount', () => {
    const canonical = canonicalizeTransferParams({ ...PARAMS, amount: '1000.500' });
    expect(canonical.amount).toBe('1000.500');
  });
});

describe('TransferOrchestrator — happy path', () => {
  it('submits once and returns the transaction hash', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit);

    const outcome = await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(submit).toHaveBeenCalledTimes(1);
    expect(submit).toHaveBeenCalledWith('deposit', 'GABCDEF1234567890', '1000', 'USDC');
    expect(outcome).toEqual({
      txHash: 'txhash-123',
      replayed: false,
      replaySource: 'none',
      attempts: 1,
    });
  });

  it('journals the submission as completed with its hash', async () => {
    const { orchestrator } = build(jest.fn().mockResolvedValue('txhash-123'));

    await orchestrator.orchestrate(PARAMS, 'key-1', { correlationId: 'corr-9' });

    const entry = orchestrator.inspect('key-1') as TransferJournalEntry;
    expect(entry.status).toBe('submitted');
    expect(entry.txHash).toBe('txhash-123');
    expect(entry.attempts).toBe(1);
    expect(entry.correlationId).toBe('corr-9');
    expect(entry.walletAddress).toBe('GABCDEF1234567890');
  });

  it('trims the idempotency key before use', async () => {
    const { orchestrator } = build(jest.fn().mockResolvedValue('txhash-123'));

    await orchestrator.orchestrate(PARAMS, '  key-1  ');

    expect(orchestrator.inspect('key-1')?.txHash).toBe('txhash-123');
  });
});

describe('TransferOrchestrator — idempotency', () => {
  it('does not submit twice for a repeated key', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit);

    const first = await orchestrator.orchestrate(PARAMS, 'key-1');
    const second = await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(submit).toHaveBeenCalledTimes(1);
    expect(second.txHash).toBe(first.txHash);
    expect(second.replayed).toBe(true);
    expect(second.replaySource).toBe('journal');
    expect(second.attempts).toBe(0);
  });

  it('replays from the idempotency store when the journal has been cleared', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit);

    await orchestrator.orchestrate(PARAMS, 'key-1');
    orchestrator.clearJournal();
    const replay = await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(submit).toHaveBeenCalledTimes(1);
    expect(replay).toEqual({
      txHash: 'txhash-123',
      replayed: true,
      replaySource: 'idempotency-store',
      attempts: 0,
    });
  });

  it('replays from the journal when the idempotency entry was evicted', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const store = new IdempotencyStore(60_000);
    const { orchestrator } = build(submit, { store });

    await orchestrator.orchestrate(PARAMS, 'key-1');
    // Simulate a pod recycle / Redis eviction: the cached response is gone but
    // the on-chain transaction very much is not.
    await store.deleteKey('key-1');

    const replay = await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(submit).toHaveBeenCalledTimes(1);
    expect(replay.txHash).toBe('txhash-123');
    expect(replay.replaySource).toBe('journal');
  });

  it('coalesces concurrent duplicates onto a single submission', async () => {
    let release: (hash: string) => void = () => undefined;
    const submit = jest.fn(
      () =>
        new Promise<string>((resolve) => {
          release = resolve;
        }),
    );
    const { orchestrator } = build(submit);

    const inflight = Promise.all([
      orchestrator.orchestrate(PARAMS, 'key-1'),
      orchestrator.orchestrate(PARAMS, 'key-1'),
    ]);
    // Let both calls reach the store before the submission settles.
    await Promise.resolve();
    release('txhash-123');
    const [a, b] = await inflight;

    expect(submit).toHaveBeenCalledTimes(1);
    expect(a.txHash).toBe('txhash-123');
    expect(b.txHash).toBe('txhash-123');
  });

  it('treats a differently-cased wallet address as the same transfer', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit);

    await orchestrator.orchestrate(PARAMS, 'key-1');
    await orchestrator.orchestrate(
      { ...PARAMS, walletAddress: PARAMS.walletAddress.toLowerCase() },
      'key-1',
    );

    expect(submit).toHaveBeenCalledTimes(1);
  });

  it('rejects a key reused for a different amount', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit);

    await orchestrator.orchestrate(PARAMS, 'key-1');

    await expect(
      orchestrator.orchestrate({ ...PARAMS, amount: '9999' }, 'key-1'),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(submit).toHaveBeenCalledTimes(1);
  });

  it('rejects a key reused for a different operation type', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit);

    await orchestrator.orchestrate(PARAMS, 'key-1');

    await expect(
      orchestrator.orchestrate({ ...PARAMS, operationType: 'withdrawal' }, 'key-1'),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(submit).toHaveBeenCalledTimes(1);
  });
});

describe('TransferOrchestrator — key validation', () => {
  it.each([
    ['an empty key', ''],
    ['a whitespace-only key', '   '],
    ['a key with an embedded space', 'key with space'],
    ['an over-long key', 'k'.repeat(256)],
  ])('rejects %s without submitting', async (_label, key) => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit);

    await expect(orchestrator.orchestrate(PARAMS, key)).rejects.toBeInstanceOf(TransferKeyError);
    expect(submit).not.toHaveBeenCalled();
  });

  it('accepts a key at the length limit', async () => {
    const { orchestrator } = build(jest.fn().mockResolvedValue('txhash-123'));
    await expect(orchestrator.orchestrate(PARAMS, 'k'.repeat(255))).resolves.toMatchObject({
      txHash: 'txhash-123',
    });
  });
});

describe('TransferOrchestrator — retries', () => {
  it('retries a retryable failure and succeeds', async () => {
    const submit = jest
      .fn()
      .mockRejectedValueOnce(sorobanError('RPC_ERROR'))
      .mockResolvedValue('txhash-123');
    const { orchestrator, sleeps } = build(submit);

    const outcome = await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(submit).toHaveBeenCalledTimes(2);
    expect(outcome.txHash).toBe('txhash-123');
    expect(outcome.attempts).toBe(2);
    expect(sleeps).toEqual([100]);
  });

  it('backs off exponentially, capped at maxBackoffMs', async () => {
    const submit = jest
      .fn()
      .mockRejectedValueOnce(sorobanError('RPC_ERROR'))
      .mockRejectedValueOnce(sorobanError('RPC_ERROR'))
      .mockRejectedValueOnce(sorobanError('RPC_ERROR'))
      .mockResolvedValue('txhash-123');
    const { orchestrator, sleeps } = build(submit, {
      maxAttempts: 4,
      initialBackoffMs: 100,
      maxBackoffMs: 250,
    });

    await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(sleeps).toEqual([100, 200, 250]);
  });

  it('applies jitter below the full delay', async () => {
    const submit = jest
      .fn()
      .mockRejectedValueOnce(sorobanError('RPC_ERROR'))
      .mockResolvedValue('txhash-123');
    const { orchestrator, sleeps } = build(submit, { random: () => 0.25 });

    await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(sleeps).toEqual([25]);
  });

  it('gives up after maxAttempts and reports how many it made', async () => {
    const submit = jest.fn().mockRejectedValue(sorobanError('RPC_ERROR'));
    const { orchestrator } = build(submit);

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toMatchObject({
      name: 'TransferFailedError',
      code: 'TRANSFER_RETRIES_EXHAUSTED',
      attempts: 3,
      statusCode: 503,
    });
    expect(submit).toHaveBeenCalledTimes(3);
    expect(orchestrator.inspect('key-1')?.status).toBe('failed');
  });

  it('stops retrying when the shared retry budget is exhausted', async () => {
    const submit = jest.fn().mockRejectedValue(sorobanError('RPC_ERROR'));
    const retryBudget = new RetryBudgetService({
      maxRetries: 100,
      failureThreshold: 100,
      minSuccessRate: 0,
    });
    jest.spyOn(retryBudget, 'canRetry').mockReturnValue(false);
    const { orchestrator } = build(submit, { retryBudget });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toMatchObject({
      code: 'TRANSFER_RETRY_BUDGET_EXHAUSTED',
      attempts: 1,
    });
    expect(submit).toHaveBeenCalledTimes(1);
  });

  it('does not let a retry chain starve itself on the shared budget', async () => {
    // Default budget settings would fail the success-rate check after a single
    // recorded failure; the orchestrator must not record mid-chain retries.
    const retryBudget = new RetryBudgetService({ minSuccessRate: 0.5, maxRetries: 10 });
    const submit = jest
      .fn()
      .mockRejectedValueOnce(sorobanError('RPC_ERROR'))
      .mockResolvedValue('txhash-123');
    const { orchestrator } = build(submit, { retryBudget });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).resolves.toMatchObject({
      txHash: 'txhash-123',
    });
    expect(submit).toHaveBeenCalledTimes(2);
  });

  it('retries a failed transfer under a fresh call because nothing landed', async () => {
    const submit = jest.fn().mockRejectedValueOnce(sorobanError('RPC_ERROR'));
    const { orchestrator } = build(submit, { maxAttempts: 1 });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      TransferFailedError,
    );

    submit.mockResolvedValue('txhash-123');
    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).resolves.toMatchObject({
      txHash: 'txhash-123',
      replayed: false,
    });
  });
});

describe('TransferOrchestrator — terminal failures', () => {
  it('does not retry a terminal validation failure', async () => {
    const submit = jest.fn().mockRejectedValue(sorobanError('INVALID_ADDRESS', 422));
    const { orchestrator } = build(submit);

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      SorobanSimulationError,
    );
    expect(submit).toHaveBeenCalledTimes(1);
    expect(orchestrator.inspect('key-1')?.status).toBe('failed');
  });

  it('replays a terminal failure instead of re-running a doomed submission', async () => {
    const submit = jest.fn().mockRejectedValue(sorobanError('INVALID_AMOUNT', 422));
    const { orchestrator } = build(submit);

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      SorobanSimulationError,
    );
    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toMatchObject({
      name: 'TransferFailedError',
      classification: 'terminal',
      statusCode: 422,
    });

    expect(submit).toHaveBeenCalledTimes(1);
  });
});

describe('TransferOrchestrator — indeterminate outcomes', () => {
  it('never retries a submission that may have been broadcast', async () => {
    const submit = jest.fn().mockRejectedValue(sorobanError('SUBMISSION_FAILED'));
    const { orchestrator } = build(submit);

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      TransferIndeterminateError,
    );
    expect(submit).toHaveBeenCalledTimes(1);
  });

  it('treats an attempt timeout as indeterminate rather than retryable', async () => {
    const submit = jest.fn(() => new Promise<string>(() => undefined));
    const { orchestrator } = build(submit, { attemptTimeoutMs: 5 });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toMatchObject({
      name: 'TransferIndeterminateError',
      code: 'TRANSFER_OUTCOME_UNKNOWN',
      statusCode: 409,
    });
    expect(submit).toHaveBeenCalledTimes(1);
  });

  it('parks the transfer for reconciliation and refuses further attempts', async () => {
    const submit = jest.fn().mockRejectedValue(sorobanError('SUBMISSION_FAILED'));
    const { orchestrator } = build(submit);

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      TransferIndeterminateError,
    );

    expect(orchestrator.pendingReconciliation()).toHaveLength(1);
    expect(orchestrator.pendingReconciliation()[0]).toMatchObject({
      idempotencyKey: 'key-1',
      status: 'indeterminate',
    });

    // Even with a healthy dependency the key stays blocked until reconciled.
    submit.mockResolvedValue('txhash-123');
    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      TransferIndeterminateError,
    );
    expect(submit).toHaveBeenCalledTimes(1);
  });

  it('allows a resolved key to be attempted again', async () => {
    const submit = jest.fn().mockRejectedValue(sorobanError('SUBMISSION_FAILED'));
    const { orchestrator } = build(submit);

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      TransferIndeterminateError,
    );

    await expect(orchestrator.resolve('key-1')).resolves.toBe(true);
    expect(orchestrator.pendingReconciliation()).toHaveLength(0);

    submit.mockResolvedValue('txhash-123');
    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).resolves.toMatchObject({
      txHash: 'txhash-123',
    });
  });
});

describe('TransferOrchestrator — circuit breaker', () => {
  it('fails fast without submitting when the circuit is open', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const circuitBreaker = new CircuitBreaker({ failureThreshold: 1, cooldownMs: 60_000 });
    // Trip the breaker on an unrelated call.
    await expect(
      circuitBreaker.execute(async () => {
        throw new Error('rpc down');
      }),
    ).rejects.toThrow('rpc down');

    const { orchestrator } = build(submit, { circuitBreaker });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      CircuitOpenError,
    );
    expect(submit).not.toHaveBeenCalled();
    expect(orchestrator.inspect('key-1')?.status).toBe('blocked');
  });

  it('allows a later attempt once the circuit closes again', async () => {
    const submit = jest.fn().mockResolvedValue('txhash-123');
    const circuitBreaker = new CircuitBreaker({ failureThreshold: 1, cooldownMs: 0 });
    await expect(
      circuitBreaker.execute(async () => {
        throw new Error('rpc down');
      }),
    ).rejects.toThrow('rpc down');

    const { orchestrator } = build(submit, { circuitBreaker });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).resolves.toMatchObject({
      txHash: 'txhash-123',
    });
  });
});

describe('TransferOrchestrator — journal hygiene', () => {
  it('mirrors every transition to the journal sink', async () => {
    const seen: Array<{ status: string; attempts: number }> = [];
    const { orchestrator } = build(
      jest
        .fn()
        .mockRejectedValueOnce(sorobanError('RPC_ERROR'))
        .mockResolvedValue('txhash-123'),
      { journalSink: (entry) => seen.push({ status: entry.status, attempts: entry.attempts }) },
    );

    await orchestrator.orchestrate(PARAMS, 'key-1');

    expect(seen.map((s) => s.status)).toEqual([
      'in_flight',
      'in_flight',
      'in_flight',
      'submitted',
    ]);
  });

  it('does not fail a transfer when the journal sink throws', async () => {
    const { orchestrator } = build(jest.fn().mockResolvedValue('txhash-123'), {
      journalSink: () => {
        throw new Error('sink down');
      },
    });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).resolves.toMatchObject({
      txHash: 'txhash-123',
    });
  });

  it('prunes entries past the retention window', async () => {
    let clock = 1_000_000;
    const { orchestrator } = build(jest.fn().mockResolvedValue('txhash-123'), {
      journalTtlMs: 1000,
      now: () => clock,
    });

    await orchestrator.orchestrate(PARAMS, 'key-1');
    expect(orchestrator.inspectAll()).toHaveLength(1);

    clock += 5000;
    expect(orchestrator.inspectAll()).toHaveLength(0);
  });

  it('keeps indeterminate entries past the retention window', async () => {
    let clock = 1_000_000;
    const { orchestrator } = build(jest.fn().mockRejectedValue(sorobanError('SUBMISSION_FAILED')), {
      journalTtlMs: 1000,
      now: () => clock,
    });

    await expect(orchestrator.orchestrate(PARAMS, 'key-1')).rejects.toBeInstanceOf(
      TransferIndeterminateError,
    );

    clock += 5000;
    expect(orchestrator.pendingReconciliation()).toHaveLength(1);
  });

  it('bounds the journal at its configured capacity', async () => {
    const { orchestrator } = build(jest.fn().mockResolvedValue('txhash-123'), {
      journalMaxEntries: 2,
    });

    for (const key of ['key-1', 'key-2', 'key-3']) {
      await orchestrator.orchestrate({ ...PARAMS, amount: key }, key);
    }

    expect(orchestrator.inspectAll().map((e) => e.idempotencyKey)).toEqual(['key-2', 'key-3']);
  });
});

describe('orchestrateTransfer (compatibility helper)', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('executes the operation and returns the stored result', async () => {
    const { transferOrchestrator } = jest.requireActual('../transferOrchestrator');
    jest.spyOn(transferOrchestrator, 'orchestrate').mockResolvedValue({
      txHash: 'txhash-123',
      replayed: false,
      replaySource: 'none',
      attempts: 1,
    });

    const { result, replayed } = await orchestrateTransfer(PARAMS, 'test-idempotency-key');

    expect(transferOrchestrator.orchestrate).toHaveBeenCalledWith(
      PARAMS,
      'test-idempotency-key',
    );
    expect(result).toEqual({ statusCode: 200, body: 'txhash-123' });
    expect(replayed).toBe(false);
  });

  it('returns the replayed flag when the transfer was already executed', async () => {
    const { transferOrchestrator } = jest.requireActual('../transferOrchestrator');
    jest.spyOn(transferOrchestrator, 'orchestrate').mockResolvedValue({
      txHash: 'txhash-123',
      replayed: true,
      replaySource: 'idempotency-store',
      attempts: 0,
    });

    const { replayed } = await orchestrateTransfer(PARAMS, 'test-idempotency-key');

    expect(replayed).toBe(true);
  });
});
