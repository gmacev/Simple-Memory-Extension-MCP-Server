import { type ChildProcessWithoutNullStreams, spawn } from 'node:child_process';
import { createInterface, type Interface } from 'node:readline';
import * as z from 'zod/v4';
import type { AppConfig } from '../config.js';
import type { Logger } from '../logger.js';
import { compileSchema } from '../validation.js';
import { ModelWorkerFailureError } from './inference-errors.js';

const workerResponseSchema = compileSchema(
  z.discriminatedUnion('ok', [
    z.object({ id: z.string(), ok: z.literal(true), result: z.unknown() }),
    z.object({
      id: z.string(),
      ok: z.literal(false),
      error: z.object({ type: z.string().optional(), message: z.string().optional() }),
    }),
  ]),
);

interface PendingDispatch {
  id: string;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
}

export interface InferenceWorkerTransport {
  readonly launcherPid: number | null;
  readonly workerPid: number | null;
  readonly processStarts: number;
  dispatch(operation: string, payload?: Record<string, unknown>): Promise<unknown>;
  abort(error: Error): void;
  stop(): Promise<void>;
}

export class ModelWorkerTransport implements InferenceWorkerTransport {
  private process: ChildProcessWithoutNullStreams | null = null;
  private lines: Interface | null = null;
  private pending: PendingDispatch | null = null;
  private requestCounter = 0;
  private failureCount = 0;
  private circuitOpenUntil = 0;
  private starts = 0;
  private actualWorkerPid: number | null = null;
  private stopping = false;

  public constructor(
    private readonly config: AppConfig,
    private readonly logger: Logger,
    private readonly forwardWorkerStderr = false,
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

  public reportWorkerPid(pid: number): void {
    this.actualWorkerPid = pid;
  }

  private ensureStarted(): ChildProcessWithoutNullStreams {
    if (!this.config.modelsEnabled) {
      throw new ModelWorkerFailureError(
        'Model inference is disabled by SIMPLE_MEMORY_MODELS=disabled',
      );
    }
    if (Date.now() < this.circuitOpenUntil) {
      throw new ModelWorkerFailureError(
        'Model inference circuit is temporarily open after repeated failures',
      );
    }
    if (this.process) return this.process;

    const child = spawn(this.config.pythonExecutablePath, ['-m', 'simple_memory_models.worker'], {
      cwd: this.config.pythonProjectPath,
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
        PYTHONUTF8: '1',
        SIMPLE_MEMORY_DEVICE: this.config.device,
        SIMPLE_MEMORY_LOCAL_FILES_ONLY: String(this.config.localFilesOnly),
        SIMPLE_MEMORY_EMBEDDING_MODEL: this.config.embeddingModel,
        SIMPLE_MEMORY_EMBEDDING_REVISION: this.config.embeddingRevision,
        SIMPLE_MEMORY_RERANKER_MODEL: this.config.rerankerModel,
        SIMPLE_MEMORY_RERANKER_REVISION: this.config.rerankerRevision,
        SIMPLE_MEMORY_QUERY_INSTRUCTION: this.config.queryInstruction,
        SIMPLE_MEMORY_RERANK_INSTRUCTION: this.config.rerankInstruction,
        SIMPLE_MEMORY_EMBED_BATCH_SIZE: String(this.config.embeddingBatchSize),
        SIMPLE_MEMORY_RERANK_BATCH_SIZE: String(this.config.rerankBatchSize),
      },
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });
    this.stopping = false;
    this.process = child;
    this.actualWorkerPid = null;
    this.starts += 1;
    this.lines = createInterface({ input: child.stdout });
    this.lines.on('line', (line) => this.handleLine(line));
    child.stderr.on('data', (chunk: Buffer) => {
      if (this.forwardWorkerStderr) {
        process.stderr.write(chunk);
        return;
      }
      const message = chunk.toString('utf8').trim();
      if (message) this.logger.debug('model-worker', message);
    });
    child.stdin.on('error', (error) => {
      this.logger.warn('Model worker stdin failed', { error: error.message });
    });
    child.once('error', (error) => this.handleExit(error));
    child.once('exit', (code, signal) => {
      this.handleExit(
        new ModelWorkerFailureError(
          `Model worker exited code=${String(code)} signal=${String(signal)}`,
        ),
      );
    });
    this.logger.info('Started persistent model worker', { pid: child.pid });
    return child;
  }

  private handleLine(line: string): void {
    let response: z.infer<typeof workerResponseSchema>;
    try {
      response = workerResponseSchema.parse(JSON.parse(line));
    } catch (error) {
      this.logger.error('Invalid JSON from model worker', { line, error: String(error) });
      return;
    }
    const pending = this.pending;
    if (!pending || pending.id !== response.id) return;
    this.pending = null;
    if (response.ok) {
      this.failureCount = 0;
      pending.resolve(response.result);
      return;
    }
    this.noteFailure();
    pending.reject(
      new ModelWorkerFailureError(
        `${response.error.type ?? 'ModelError'}: ${response.error.message ?? 'Unknown model error'}`,
      ),
    );
  }

  private handleExit(error: Error): void {
    if (!this.process) return;
    const expected = this.stopping;
    this.stopping = false;
    this.lines?.close();
    this.lines = null;
    this.process = null;
    this.actualWorkerPid = null;
    const pending = this.pending;
    this.pending = null;
    if (expected) this.logger.debug('Model worker stopped normally');
    else {
      this.logger.warn('Model worker stopped', { error: error.message });
      this.noteFailure();
    }
    pending?.reject(error);
  }

  private noteFailure(): void {
    this.failureCount += 1;
    if (this.failureCount >= 3) {
      this.circuitOpenUntil = Date.now() + 30_000;
      this.failureCount = 0;
    }
  }

  public dispatch(operation: string, payload: Record<string, unknown> = {}): Promise<unknown> {
    if (this.pending) {
      throw new ModelWorkerFailureError('Model worker transport already has an in-flight request');
    }
    const child = this.ensureStarted();
    const id = `${process.pid}-${++this.requestCounter}`;
    return new Promise((resolve, reject) => {
      this.pending = { id, resolve, reject };
      child.stdin.write(`${JSON.stringify({ id, operation, ...payload })}\n`, 'utf8', (error) => {
        if (!error) return;
        const pending = this.pending;
        if (!pending || pending.id !== id) return;
        this.abort(new ModelWorkerFailureError(error.message, { cause: error }));
      });
    });
  }

  public abort(error: Error): void {
    const child = this.process;
    if (!child) {
      const pending = this.pending;
      this.pending = null;
      pending?.reject(error);
      return;
    }
    const pending = this.pending;
    this.pending = null;
    this.lines?.close();
    this.lines = null;
    this.process = null;
    this.actualWorkerPid = null;
    this.stopping = false;
    this.noteFailure();
    pending?.reject(error);
    child.kill();
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

  public async stop(): Promise<void> {
    const child = this.process;
    if (!child) return;
    if (this.pending) {
      this.abort(new ModelWorkerFailureError('Model worker stopped with work in flight'));
      await this.waitForExit(child);
      return;
    }
    this.stopping = true;
    try {
      await this.dispatch('shutdown');
      await this.waitForExit(child);
    } catch {
      child.kill();
      await this.waitForExit(child);
    }
  }
}
