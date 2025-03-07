#!/usr/bin/env node

/**
 * This script orchestrates the complete startup process:
 * 1. Checks if Python setup is needed (first run)
 * 2. Builds the TypeScript code
 * 3. Sets up Python environment and pre-downloads model if needed
 * 4. Starts the server
 */

import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';

// Get the directory name in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Important paths
const VENV_DIR = path.join(__dirname, '..', 'venv');
const PYTHON_SCRIPT_DIR = path.join(__dirname, '..', 'src', 'python');
const EMBEDDING_SCRIPT = path.join(PYTHON_SCRIPT_DIR, 'embedding_service.py');
const SERVER_PATH = path.join(__dirname, '..', 'dist', 'index.js');

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
 * Runs a command and returns a promise
 */
function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    log(`Running: ${command} ${args.join(' ')}`, colors.blue);
    
    const proc = spawn(command, args, {
      stdio: 'inherit',
      shell: true,
      ...options
    });
    
    proc.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Command failed with exit code ${code}`));
      }
    });
    
    proc.on('error', (err) => {
      reject(err);
    });
  });
}

/**
 * Checks if setup is needed
 */
function isSetupNeeded() {
  // Check if Python virtual environment exists
  const venvPython = os.platform() === 'win32' 
    ? path.join(VENV_DIR, 'Scripts', 'python.exe')
    : path.join(VENV_DIR, 'bin', 'python');
  
  const venvExists = fs.existsSync(venvPython);
  
  // Also make sure the embedding script exists
  const scriptExists = fs.existsSync(EMBEDDING_SCRIPT);
  
  // If either doesn't exist, setup is needed
  const setupNeeded = !venvExists || !scriptExists;
  
  if (setupNeeded) {
    log('First-time setup is needed', colors.yellow);
  } else {
    log('Setup already completed, skipping setup phase', colors.green);
  }
  
  return setupNeeded;
}

/**
 * Builds the TypeScript code
 */
async function buildTypeScript() {
  log('Building TypeScript code...', colors.blue);
  try {
    // On Windows use npm directly with shell: true to handle the npm batch file
    await runCommand('npm', ['run', 'build']);
    log('TypeScript build completed successfully.', colors.green);
  } catch (error) {
    log(`TypeScript build failed: ${error.message}`, colors.red);
    throw error;
  }
}

/**
 * Runs the Python setup if needed
 */
async function setupIfNeeded() {
  // Check if setup is needed
  if (!isSetupNeeded()) {
    return;
  }
  
  log('Setting up Python environment...', colors.blue);
  try {
    const setupScript = path.join(__dirname, 'setup.js');
    
    // Make setup script executable on Unix systems
    if (process.platform !== 'win32') {
      try {
        fs.chmodSync(setupScript, '755');
      } catch (error) {
        // Ignore errors here, the script will still run with Node
      }
    }
    
    // Use node with shell: true to handle scripts on Windows
    await runCommand('node', [setupScript]);
    log('Python setup completed successfully.', colors.green);
  } catch (error) {
    log(`Python setup failed: ${error.message}`, colors.red);
    throw error;
  }
}

/**
 * Starts the server
 */
async function startServer() {
  log('Starting server...', colors.blue);
  try {
    // Use node with shell: true to run the built server
    await runCommand('node', [SERVER_PATH]);
  } catch (error) {
    log(`Server failed to start: ${error.message}`, colors.red);
    throw error;
  }
}

/**
 * Main function
 */
async function main() {
  log('Starting launch sequence...', colors.yellow);
  
  try {
    // Step 1: Build TypeScript
    await buildTypeScript();
    
    // Step 2: Setup Python if needed
    await setupIfNeeded();
    
    // Step 3: Start server
    await startServer();
  } catch (error) {
    log(`Launch sequence failed: ${error.message}`, colors.red);
    process.exit(1);
  }
}

// Run the main function
main(); 