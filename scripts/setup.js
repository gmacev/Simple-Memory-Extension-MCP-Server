#!/usr/bin/env node

/**
 * This script automates the setup process for the semantic search feature:
 * - Creates a Python virtual environment (if needed)
 * - Installs Python dependencies
 * - Pre-downloads the E5 model to avoid delays on first use
 */

import { spawn, execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';

// Get the directory name in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PYTHON_COMMAND = os.platform() === 'win32' ? 'python' : 'python3';
const VENV_DIR = path.join(__dirname, '..', 'venv');
const REQUIREMENTS_FILE = path.join(__dirname, '..', 'requirements.txt');
const PYTHON_SCRIPT_DIR = path.join(__dirname, '..', 'src', 'python');
const MODEL_DIR = path.join(os.homedir(), '.cache', 'huggingface', 'transformers');

// Ensure the Python script directory exists
if (!fs.existsSync(PYTHON_SCRIPT_DIR)) {
  fs.mkdirSync(PYTHON_SCRIPT_DIR, { recursive: true });
}

// Colors for console output
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  red: '\x1b[31m',
};

/**
 * Logs a message with color
 */
function log(message, color = colors.reset) {
  console.log(`${color}${message}${colors.reset}`);
}

/**
 * Checks if a command exists in PATH
 */
function commandExists(command) {
  try {
    const devNull = os.platform() === 'win32' ? 'NUL' : '/dev/null';
    execSync(`${command} --version`, { stdio: 'ignore' });
    return true;
  } catch (e) {
    return false;
  }
}

/**
 * Checks if virtual environment exists
 */
function venvExists() {
  const venvPython = os.platform() === 'win32' 
    ? path.join(VENV_DIR, 'Scripts', 'python.exe')
    : path.join(VENV_DIR, 'bin', 'python');
  return fs.existsSync(venvPython);
}

/**
 * Creates a Python virtual environment
 */
async function createVirtualEnv() {
  if (venvExists()) {
    log('Python virtual environment already exists.', colors.green);
    return;
  }

  log('Creating Python virtual environment...', colors.blue);
  return new Promise((resolve, reject) => {
    const proc = spawn(PYTHON_COMMAND, ['-m', 'venv', VENV_DIR]);
    
    proc.on('close', (code) => {
      if (code === 0) {
        log('Python virtual environment created successfully.', colors.green);
        resolve();
      } else {
        log(`Failed to create virtual environment (exit code ${code}).`, colors.red);
        reject(new Error(`Failed to create virtual environment (exit code ${code})`));
      }
    });
    
    proc.on('error', (err) => {
      log(`Error creating virtual environment: ${err.message}`, colors.red);
      reject(err);
    });
  });
}

/**
 * Installs Python dependencies
 */
async function installDependencies() {
  const pipCmd = os.platform() === 'win32' 
    ? path.join(VENV_DIR, 'Scripts', 'pip.exe')
    : path.join(VENV_DIR, 'bin', 'pip');
  
  log('Installing Python dependencies...', colors.blue);
  return new Promise((resolve, reject) => {
    const proc = spawn(pipCmd, ['install', '-r', REQUIREMENTS_FILE]);
    
    proc.stdout.on('data', (data) => {
      process.stdout.write(data.toString());
    });
    
    proc.stderr.on('data', (data) => {
      process.stderr.write(data.toString());
    });
    
    proc.on('close', (code) => {
      if (code === 0) {
        log('Python dependencies installed successfully.', colors.green);
        resolve();
      } else {
        log(`Failed to install dependencies (exit code ${code}).`, colors.red);
        reject(new Error(`Failed to install dependencies (exit code ${code})`));
      }
    });
    
    proc.on('error', (err) => {
      log(`Error installing dependencies: ${err.message}`, colors.red);
      reject(err);
    });
  });
}

/**
 * Pre-downloads the E5 model
 */
async function preDownloadModel() {
  const pythonCmd = os.platform() === 'win32'
      ? path.join(VENV_DIR, 'Scripts', 'python.exe')
      : path.join(VENV_DIR, 'bin', 'python');

  const downloadScriptPath = path.join(PYTHON_SCRIPT_DIR, 'download_model.py');

  // Check if the download script already exists
  if (!fs.existsSync(downloadScriptPath)) {
    // Create a simple script to download the model
    const downloadScript = `
import os
from transformers import AutoTokenizer, AutoModel

# Set the model name
MODEL_NAME = "intfloat/multilingual-e5-large-instruct"

print(f"Pre-downloading model: {MODEL_NAME}")
print("This might take a few minutes...")

# Download the model and tokenizer
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModel.from_pretrained(MODEL_NAME)

print(f"Model downloaded and cached at: {os.path.expanduser('~/.cache/huggingface/transformers')}")
print("Setup completed successfully!")
`;

    fs.writeFileSync(downloadScriptPath, downloadScript);
  }

  log('Pre-downloading the E5 model (this may take a few minutes)...', colors.blue);
  return new Promise((resolve, reject) => {
    const proc = spawn(pythonCmd, [downloadScriptPath]);

    proc.stdout.on('data', (data) => {
      process.stdout.write(data.toString());
    });

    proc.stderr.on('data', (data) => {
      process.stderr.write(data.toString());
    });

    proc.on('close', (code) => {
      if (code === 0) {
        log('E5 model downloaded successfully.', colors.green);
        resolve();
      } else {
        log(`Failed to download model (exit code ${code}).`, colors.red);
        // Don't reject since this is not critical - the model will be downloaded on first use
        resolve();
      }
    });

    proc.on('error', (err) => {
      log(`Error downloading model: ${err.message}`, colors.red);
      // Don't reject since this is not critical - the model will be downloaded on first use
      resolve();
    });
  });
}

/**
 * Main function
 */
async function main() {
  log('Starting setup for semantic search...', colors.blue);
  
  // Check if Python is installed
  if (!commandExists(PYTHON_COMMAND)) {
    log(`${PYTHON_COMMAND} not found. Please install Python 3.8 or later.`, colors.red);
    process.exit(1);
  }
  
  try {
    // Create virtual environment
    await createVirtualEnv();
    
    // Install dependencies
    await installDependencies();
    
    // Pre-download model (optional)
    await preDownloadModel();
    
    log('Setup completed successfully!', colors.green);
  } catch (error) {
    log(`Setup failed: ${error.message}`, colors.red);
    process.exit(1);
  }
}

// Run the main function
main(); 