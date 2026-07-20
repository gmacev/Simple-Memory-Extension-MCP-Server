#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const mode = process.argv.slice(2).find((argument) => !argument.startsWith('--')) ?? 'setup';
if (mode !== 'setup' && mode !== 'update') {
  throw new Error(`Expected installation mode setup or update; received ${mode}`);
}
const skipModels = process.argv.includes('--skip-models');
const npmCliPath = process.env.npm_execpath;
const npmCommand = npmCliPath ? process.execPath : 'npm';

function npmArguments(arguments_) {
  return npmCliPath ? [npmCliPath, ...arguments_] : arguments_;
}

function heading(message) {
  process.stdout.write(`\n[simple-memory ${mode}] ${message}\n`);
}

function run(label, command, args, options = {}) {
  heading(label);
  const outcome = spawnSync(command, args, {
    cwd: root,
    env: process.env,
    stdio: 'inherit',
    windowsHide: true,
    ...options,
  });
  if (outcome.error) {
    throw new Error(`${label} could not start: ${outcome.error.message}`);
  }
  if (outcome.status !== 0) {
    if (label === 'Install locked Node dependencies') {
      throw new Error(
        `${label} failed. Stop any MCP client using Simple Memory, then rerun npm run ${mode}; loaded native dependencies cannot be replaced while the server is running`,
      );
    }
    throw new Error(`${label} failed with exit code ${String(outcome.status)}`);
  }
}

function commandWorks(command, args) {
  const outcome = spawnSync(command, args, {
    cwd: root,
    env: process.env,
    stdio: 'ignore',
    windowsHide: true,
  });
  return !outcome.error && outcome.status === 0;
}

function captured(command, args, options = {}) {
  return spawnSync(command, args, {
    cwd: root,
    env: process.env,
    encoding: 'utf8',
    maxBuffer: 10 * 1024 * 1024,
    windowsHide: true,
    ...options,
  });
}

function requireNode22() {
  const [majorText = '0', minorText = '0'] = process.versions.node.split('.');
  const major = Number.parseInt(majorText, 10);
  const minor = Number.parseInt(minorText, 10);
  if (
    !Number.isFinite(major) ||
    !Number.isFinite(minor) ||
    major < 22 ||
    (major === 22 && minor < 9)
  ) {
    throw new Error(`Node.js 22.9 or newer is required; found ${process.version}`);
  }
}

function requireNpm10() {
  const outcome = captured(npmCommand, npmArguments(['--version']));
  if (outcome.error || outcome.status !== 0) {
    throw new Error('npm 10 or newer is required but npm could not be started');
  }
  const version = outcome.stdout.trim();
  const major = Number.parseInt(version.split('.')[0] ?? '0', 10);
  if (!Number.isFinite(major) || major < 10) {
    throw new Error(`npm 10 or newer is required; found ${version || 'an unknown version'}`);
  }
  return version;
}

function locateUv() {
  if (process.env.SIMPLE_MEMORY_UV && commandWorks(process.env.SIMPLE_MEMORY_UV, ['--version'])) {
    return process.env.SIMPLE_MEMORY_UV;
  }
  if (commandWorks('uv', ['--version'])) return 'uv';
  const local = path.join(
    homedir(),
    '.local',
    'bin',
    process.platform === 'win32' ? 'uv.exe' : 'uv',
  );
  return existsSync(local) && commandWorks(local, ['--version']) ? local : undefined;
}

async function downloadUvInstaller(url, destination) {
  let response;
  try {
    response = await fetch(url, {
      redirect: 'follow',
      signal: AbortSignal.timeout(60_000),
    });
  } catch (error) {
    throw new Error(`Could not download the uv installer from ${url}: ${String(error)}`);
  }
  if (!response.ok) {
    throw new Error(`Could not download the uv installer: HTTP ${String(response.status)}`);
  }
  const declaredLength = Number.parseInt(response.headers.get('content-length') ?? '0', 10);
  const maximumBytes = 2 * 1024 * 1024;
  if (declaredLength > maximumBytes) {
    throw new Error(`The uv installer exceeded the ${String(maximumBytes)} byte safety limit`);
  }
  const installer = Buffer.from(await response.arrayBuffer());
  if (installer.length < 1_000 || installer.length > maximumBytes) {
    throw new Error(
      `The downloaded uv installer had an unexpected size: ${String(installer.length)}`,
    );
  }
  writeFileSync(destination, installer, { mode: 0o700 });
}

async function installUv() {
  heading("uv was not found; downloading Astral's official installer with Node.js");
  const installerDirectory = mkdtempSync(path.join(tmpdir(), 'simple-memory-uv-installer-'));
  const installDirectory = path.join(homedir(), '.local', 'bin');
  mkdirSync(installDirectory, { recursive: true });
  const installerName = process.platform === 'win32' ? 'install.ps1' : 'install.sh';
  const installerPath = path.join(installerDirectory, installerName);
  const installerUrl = `https://astral.sh/uv/${installerName}`;
  try {
    await downloadUvInstaller(installerUrl, installerPath);
    const installerEnvironment = {
      ...process.env,
      UV_UNMANAGED_INSTALL: installDirectory,
    };
    if (process.platform === 'win32') {
      run(
        'Install uv',
        'powershell.exe',
        ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', installerPath],
        { env: installerEnvironment },
      );
    } else {
      if (!commandWorks('sh', ['-c', 'exit 0'])) {
        throw new Error('A POSIX sh executable is required to run the official uv installer');
      }
      run('Install uv', 'sh', [installerPath], { env: installerEnvironment });
    }
  } finally {
    rmSync(installerDirectory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  }
  const uv = locateUv();
  if (!uv) {
    throw new Error(`uv installation completed but no executable was found in ${installDirectory}`);
  }
  return uv;
}

function pythonExecutablePath() {
  return path.join(
    root,
    'python',
    '.venv',
    process.platform === 'win32' ? 'Scripts' : 'bin',
    process.platform === 'win32' ? 'python.exe' : 'python',
  );
}

function selectTorchBackend() {
  const override = process.env.SIMPLE_MEMORY_TORCH_BACKEND?.trim();
  if (process.platform === 'darwin') {
    if (override && override !== 'auto' && override !== 'cpu') {
      throw new Error(
        `SIMPLE_MEMORY_TORCH_BACKEND=${override} is not supported on macOS; use auto or cpu`,
      );
    }
    return {
      backend: undefined,
      expectsAccelerator: false,
      reason: 'the standard macOS PyTorch wheel provides Apple MPS with CPU fallback',
    };
  }

  if (override) {
    return {
      backend: override,
      expectsAccelerator: override !== 'auto' && override !== 'cpu',
      reason: 'SIMPLE_MEMORY_TORCH_BACKEND override',
    };
  }

  const nvidia = captured('nvidia-smi', ['--query-gpu=name,compute_cap', '--format=csv,noheader']);
  if (!nvidia.error && nvidia.status === 0) {
    const capabilities = nvidia.stdout
      .trim()
      .split(/\r?\n/u)
      .map((line) => Number.parseFloat(line.split(',').at(-1)?.trim() ?? ''))
      .filter((value) => Number.isFinite(value));
    if (capabilities.some((capability) => capability < 7.5)) {
      return {
        backend: 'cu126',
        expectsAccelerator: true,
        reason: 'CUDA 12.6 preserves support for pre-Turing NVIDIA GPUs',
      };
    }
    return {
      backend: 'auto',
      expectsAccelerator: true,
      reason: 'uv will select the newest backend compatible with the NVIDIA driver',
    };
  }

  return {
    backend: 'auto',
    expectsAccelerator: false,
    reason: 'uv will detect AMD, Intel, Apple, or CPU capabilities',
  };
}

function installPreferredTorchBackend(uv) {
  const python = pythonExecutablePath();
  const selection = selectTorchBackend();
  if (selection.backend) {
    const versionResult = captured(python, [
      '-c',
      'from importlib.metadata import version; print(version("torch").split("+")[0])',
    ]);
    if (versionResult.error || versionResult.status !== 0) {
      throw new Error('Could not determine the locked PyTorch version after uv sync');
    }
    const torchVersion = versionResult.stdout.trim();
    run(`Install GPU-preferred PyTorch backend ${selection.backend} (${selection.reason})`, uv, [
      'pip',
      'install',
      '--python',
      python,
      '--torch-backend',
      selection.backend,
      '--reinstall-package',
      'torch',
      '--only-binary',
      'torch',
      `torch==${torchVersion}`,
    ]);
  } else {
    heading(`Use platform PyTorch installation (${selection.reason})`);
  }

  const probe = captured(python, [
    '-c',
    [
      'import json, torch',
      'xpu = getattr(torch, "xpu", None)',
      'mps = getattr(torch.backends, "mps", None)',
      'cuda = torch.cuda.is_available()',
      'xpu_available = bool(xpu and xpu.is_available())',
      'mps_available = bool(mps and mps.is_available())',
      'print(json.dumps({"torch": torch.__version__, "cuda": cuda, "xpu": xpu_available, "mps": mps_available, "accelerator": cuda or xpu_available or mps_available}))',
    ].join('; '),
  ]);
  if (probe.error || probe.status !== 0) {
    throw new Error('PyTorch was installed but could not be imported');
  }
  const health = JSON.parse(probe.stdout);
  if (selection.expectsAccelerator && health.accelerator !== true) {
    throw new Error(
      `PyTorch backend ${selection.backend} installed, but no compatible accelerator is available`,
    );
  }
  const accelerator = health.cuda
    ? 'CUDA'
    : health.xpu
      ? 'Intel XPU'
      : health.mps
        ? 'Apple MPS'
        : 'CPU';
  heading(`PyTorch ${health.torch} ready; selected runtime: ${accelerator}`);
}

async function main() {
  const verificationDataDir = mkdtempSync(path.join(tmpdir(), `simple-memory-${mode}-`));
  try {
    requireNode22();
    const npmVersion = requireNpm10();
    heading(`Using Node ${process.version} and npm ${npmVersion}`);
    if (mode === 'update') {
      heading(
        'Prerequisite: MCP clients using Simple Memory must be stopped while dependencies are updated',
      );
    }
    run('Install locked Node dependencies', npmCommand, npmArguments(['ci']));
    const uv = locateUv() ?? (await installUv());
    run('Synchronize locked Python model environment', uv, [
      'sync',
      '--project',
      path.join(root, 'python'),
      '--locked',
      '--no-dev',
    ]);
    installPreferredTorchBackend(uv);
    heading('Clear generated server output');
    rmSync(path.join(root, 'dist'), {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 250,
    });
    run('Build the MCP server', npmCommand, npmArguments(['run', 'build']));
    run(
      'Apply pending memory database migrations',
      process.execPath,
      [path.join(root, 'dist', 'cli.js'), 'migrate'],
      {
        env: {
          ...process.env,
          SIMPLE_MEMORY_MODELS: 'disabled',
        },
      },
    );
    if (skipModels) {
      heading('Skipping model prefetch because --skip-models was supplied');
    } else {
      const modelCommand = [path.join(root, 'dist', 'cli.js'), 'model', 'fetch'];
      const modelEnvironment = {
        ...process.env,
        SIMPLE_MEMORY_DATA_DIR: verificationDataDir,
        SIMPLE_MEMORY_MODELS: 'enabled',
        SIMPLE_MEMORY_LOCAL_FILES_ONLY: 'false',
      };
      run(
        'Prepare and verify pinned Qwen models (cached files are reused)',
        process.execPath,
        modelCommand,
        { env: modelEnvironment },
      );
    }
    run(
      'Run final environment doctor',
      process.execPath,
      [path.join(root, 'dist', 'cli.js'), 'doctor'],
      {
        env: {
          ...process.env,
          SIMPLE_MEMORY_DATA_DIR: verificationDataDir,
          SIMPLE_MEMORY_MODELS: 'disabled',
        },
      },
    );
    heading(
      `${mode === 'setup' ? 'Setup' : 'Update'} complete. Restart MCP clients that use Simple Memory.`,
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`\n[simple-memory ${mode}] ERROR: ${message}\n`);
    process.exitCode = 1;
  } finally {
    rmSync(verificationDataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
}

await main();
