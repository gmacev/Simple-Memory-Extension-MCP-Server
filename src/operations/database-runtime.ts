import { randomUUID } from 'node:crypto';
import { mkdir, open, readdir, readFile, rm } from 'node:fs/promises';
import path from 'node:path';

export interface DatabaseRuntimeLease {
  release(): Promise<void>;
}

function runtimeDirectory(databasePath: string): string {
  return `${path.resolve(databasePath)}.runtime`;
}

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error instanceof Error && 'code' in error && error.code === 'EPERM';
  }
}

async function liveRestoreLock(file: string): Promise<boolean> {
  try {
    const parsed = JSON.parse(await readFile(file, 'utf8')) as { pid?: unknown };
    const pid = typeof parsed.pid === 'number' ? parsed.pid : Number.NaN;
    if (Number.isSafeInteger(pid) && pid > 0 && processExists(pid)) return true;
    await rm(file, { force: true });
    return false;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
    // A lock that cannot yet be read is safer to treat as live. The restore
    // process may still be finishing its atomic marker write.
    return true;
  }
}

async function activeServerPids(directory: string): Promise<number[]> {
  const entries = await readdir(directory).catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  });
  const active: number[] = [];
  for (const entry of entries) {
    if (!entry.startsWith('server-') || !entry.endsWith('.json')) continue;
    const marker = path.join(directory, entry);
    try {
      const parsed = JSON.parse(await readFile(marker, 'utf8')) as { pid?: unknown };
      const pid = typeof parsed.pid === 'number' ? parsed.pid : Number.NaN;
      if (Number.isSafeInteger(pid) && pid > 0 && processExists(pid)) {
        active.push(pid);
      } else {
        await rm(marker, { force: true });
      }
    } catch {
      await rm(marker, { force: true });
    }
  }
  return [...new Set(active)].sort((left, right) => left - right);
}

export async function acquireDatabaseServerLease(
  databasePath: string,
): Promise<DatabaseRuntimeLease> {
  const directory = runtimeDirectory(databasePath);
  const restoreLock = path.join(directory, 'restore.lock');
  await mkdir(directory, { recursive: true });
  if (await liveRestoreLock(restoreLock)) {
    throw new Error('Database restore is in progress; Simple Memory cannot start');
  }
  const marker = path.join(directory, `server-${String(process.pid)}-${randomUUID()}.json`);
  const handle = await open(marker, 'wx');
  try {
    await handle.writeFile(
      JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }),
      'utf8',
    );
  } catch (error) {
    await rm(marker, { force: true }).catch(() => undefined);
    throw error;
  } finally {
    await handle.close();
  }
  if (await liveRestoreLock(restoreLock)) {
    await rm(marker, { force: true });
    throw new Error('Database restore began while Simple Memory was starting; retry after restore');
  }
  let released = false;
  return {
    release: async () => {
      if (released) return;
      released = true;
      await rm(marker, { force: true });
    },
  };
}

export async function acquireDatabaseRestoreLease(
  databasePath: string,
): Promise<DatabaseRuntimeLease> {
  const directory = runtimeDirectory(databasePath);
  await mkdir(directory, { recursive: true });
  const restoreLock = path.join(directory, 'restore.lock');
  let acquired = false;
  for (let attempt = 0; attempt < 2 && !acquired; attempt += 1) {
    let handle: Awaited<ReturnType<typeof open>> | undefined;
    let createdLock = false;
    try {
      handle = await open(restoreLock, 'wx');
      createdLock = true;
      await handle.writeFile(
        JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }),
        'utf8',
      );
      acquired = true;
    } catch (error) {
      if (
        !(error instanceof Error && 'code' in error && error.code === 'EEXIST') ||
        (await liveRestoreLock(restoreLock))
      ) {
        throw new Error('Another database restore is already in progress', { cause: error });
      }
    } finally {
      await handle?.close().catch(() => undefined);
      if (createdLock && !acquired) await rm(restoreLock, { force: true }).catch(() => undefined);
    }
  }
  if (!acquired) throw new Error('Could not acquire the database restore lock');
  try {
    const active = await activeServerPids(directory);
    if (active.length > 0) {
      throw new Error(
        `Cannot restore while Simple Memory is running (active process IDs: ${active.join(', ')}). Stop every MCP process and retry.`,
      );
    }
  } catch (error) {
    await rm(restoreLock, { force: true });
    throw error;
  }
  let released = false;
  return {
    release: async () => {
      if (released) return;
      released = true;
      await rm(restoreLock, { force: true });
    },
  };
}
