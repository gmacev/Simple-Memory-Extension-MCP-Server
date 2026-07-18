import { type ChildProcessWithoutNullStreams, spawn } from 'node:child_process';
import { createInterface, type Interface } from 'node:readline';
import * as z from 'zod/v4';
import type { AppConfig } from '../config.js';
import type { Logger } from '../logger.js';

const workerResponseSchema = z.discriminatedUnion('ok', [
  z.object({ id: z.string(), ok: z.literal(true), result: z.unknown() }),
  z.object({
    id: z.string(),
    ok: z.literal(false),
    error: z.object({ type: z.string().optional(), message: z.string().optional() }),
  }),
]);

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

const modelHealthSchema = z.object({
  status: z.string(),
  pid: z.number().int(),
  embedding_model: z.string(),
  embedding_revision: z.string(),
  reranker_model: z.string(),
  reranker_revision: z.string(),
  query_instruction_hash: z.string(),
  rerank_instruction_hash: z.string(),
  device: z.string(),
  device_name: z.string(),
  torch_version: z.string(),
  torch_cuda_version: z.string().nullable(),
  embedding_dimension: z.number().int().nullable(),
  embedding_loaded: z.boolean(),
  reranker_loaded: z.boolean(),
});

export type ModelHealth = z.infer<typeof modelHealthSchema>;

export class ModelClient {
  private process: ChildProcessWithoutNullStreams | null = null;
  private lines: Interface | null = null;
  private readonly pending = new Map<string, PendingRequest>();
  private requestCounter = 0;
  private failureCount = 0;
  private circuitOpenUntil = 0;
  private starts = 0;
  private actualWorkerPid: number | null = null;

  public constructor(
    private readonly config: AppConfig,
    private readonly logger: Logger,
  ) {}

  public get processStarts(): number {
    return this.starts;
  }

  public get launcherPid(): number | null {
    return this.process?.pid ?? null;
  }

  public get workerPid(): number | null {
    return this.actualWorkerPid;
  }

  private ensureStarted(): void {
    if (!this.config.modelsEnabled) {
      throw new Error('Model inference is disabled by SIMPLE_MEMORY_MODELS=disabled');
    }
    if (Date.now() < this.circuitOpenUntil) {
      throw new Error('Model inference circuit is temporarily open after repeated failures');
    }
    if (this.process) return;

    const child = spawn(this.config.pythonExecutablePath, ['-m', 'simple_memory_models.worker'], {
      cwd: this.config.pythonProjectPath,
      env: { ...process.env, PYTHONUNBUFFERED: '1', PYTHONUTF8: '1' },
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });
    this.process = child;
    this.starts += 1;
    this.lines = createInterface({ input: child.stdout });
    this.lines.on('line', (line) => this.handleLine(line));
    child.stderr.on('data', (chunk: Buffer) => {
      const message = chunk.toString('utf8').trim();
      if (message) this.logger.debug('model-worker', message);
    });
    child.stdin.on('error', (error) => {
      this.logger.warn('Model worker stdin failed', { error: error.message });
    });
    child.once('error', (error) => this.handleExit(error));
    child.once('exit', (code, signal) => {
      this.handleExit(
        new Error(`Model worker exited code=${String(code)} signal=${String(signal)}`),
      );
    });
    this.logger.info('Started persistent model worker', { pid: child.pid });
  }

  private handleLine(line: string): void {
    let response: z.infer<typeof workerResponseSchema>;
    try {
      response = workerResponseSchema.parse(JSON.parse(line));
    } catch (error) {
      this.logger.error('Invalid JSON from model worker', { line, error: String(error) });
      return;
    }
    const pending = this.pending.get(response.id);
    if (!pending) return;
    clearTimeout(pending.timer);
    this.pending.delete(response.id);
    if (response.ok) {
      this.failureCount = 0;
      pending.resolve(response.result);
      return;
    }
    this.noteFailure();
    pending.reject(
      new Error(
        `${response.error?.type ?? 'ModelError'}: ${response.error?.message ?? 'Unknown model error'}`,
      ),
    );
  }

  private handleExit(error: Error): void {
    if (!this.process) return;
    this.logger.warn('Model worker stopped', { error: error.message });
    this.lines?.close();
    this.lines = null;
    this.process = null;
    this.actualWorkerPid = null;
    this.noteFailure();
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }

  private noteFailure(): void {
    this.failureCount += 1;
    if (this.failureCount >= 3) {
      this.circuitOpenUntil = Date.now() + 30_000;
      this.failureCount = 0;
    }
  }

  private request(operation: string, payload: Record<string, unknown> = {}): Promise<unknown> {
    this.ensureStarted();
    const process = this.process;
    if (!process) throw new Error('Model worker failed to start');
    const id = `${globalThis.process.pid}-${++this.requestCounter}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        this.noteFailure();
        reject(new Error(`Model request ${operation} timed out`));
      }, this.config.modelTimeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      process.stdin.write(`${JSON.stringify({ id, operation, ...payload })}\n`, 'utf8', (error) => {
        if (!error) return;
        const pending = this.pending.get(id);
        if (!pending) return;
        clearTimeout(pending.timer);
        this.pending.delete(id);
        this.noteFailure();
        pending.reject(error);
      });
    });
  }

  private waitForExit(child: ChildProcessWithoutNullStreams, timeoutMs = 5_000): Promise<void> {
    if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        child.kill();
        resolve();
      }, timeoutMs);
      child.once('exit', () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  public async health(): Promise<ModelHealth> {
    const health = modelHealthSchema.parse(await this.request('health'));
    this.actualWorkerPid = health.pid;
    return health;
  }

  public async embedDocuments(texts: string[]): Promise<number[][]> {
    const result = z
      .object({ vectors: z.array(z.array(z.number())) })
      .parse(await this.request('embed_documents', { texts }));
    return result.vectors;
  }

  public async countTokens(texts: string[]): Promise<number[]> {
    const result = z
      .object({ counts: z.array(z.number().int().nonnegative()) })
      .parse(await this.request('count_tokens', { texts }));
    return result.counts;
  }

  public async embedQuery(text: string): Promise<number[]> {
    const result = z
      .object({ vector: z.array(z.number()) })
      .parse(await this.request('embed_query', { text }));
    return result.vector;
  }

  public async rerank(query: string, documents: string[]): Promise<number[]> {
    const result = z
      .object({ scores: z.array(z.number()) })
      .parse(await this.request('rerank', { query, documents }));
    return result.scores;
  }

  public async stop(): Promise<void> {
    const child = this.process;
    if (!child) return;
    try {
      await this.request('shutdown');
      await this.waitForExit(child);
    } catch {
      child.kill();
      await this.waitForExit(child);
    }
  }
}
