import { spawn } from 'child_process';
import path from 'path';
import os from 'os';
import { logger } from './logger.js';

/**
 * Service for generating embeddings using the E5 model
 */
export class EmbeddingService {
  private pythonProcess: any;
  private initialized: boolean = false;
  private initializationPromise: Promise<void> | null = null;
  private readonly pythonScriptPath: string;
  private readonly pythonCommand: string;
  private readonly taskDescription: string;

  /**
   * Creates a new embedding service
   * @param options Configuration options
   */
  constructor(
    options: {
      pythonScriptPath?: string;
      taskDescription?: string;
      useVenv?: boolean;
    } = {}
  ) {
    this.pythonScriptPath =
      options.pythonScriptPath || path.join(process.cwd(), 'src', 'python', 'embedding_service.py');
    this.taskDescription =
      options.taskDescription || 'Given a document, find semantically similar documents';

    // Determine which Python executable to use based on the useVenv option
    const useVenv = options.useVenv !== undefined ? options.useVenv : true;
    if (useVenv) {
      const venvDir = path.join(process.cwd(), 'venv');
      this.pythonCommand =
        os.platform() === 'win32'
          ? path.join(venvDir, 'Scripts', 'python.exe')
          : path.join(venvDir, 'bin', 'python');
    } else {
      this.pythonCommand = os.platform() === 'win32' ? 'python' : 'python3';
    }
  }

  /**
   * Initializes the embedding service
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    if (this.initializationPromise) {
      return this.initializationPromise;
    }

    this.initializationPromise = new Promise<void>((resolve, reject) => {
      try {
        logger.debug('Initializing embedding service', {
          scriptPath: this.pythonScriptPath,
          pythonCommand: this.pythonCommand,
        });

        // Spawn Python process
        this.pythonProcess = spawn(this.pythonCommand, [this.pythonScriptPath]);

        // Handle process exit
        this.pythonProcess.on('exit', (code: number) => {
          logger.error('Embedding service process exited', { code });
          this.initialized = false;
          this.pythonProcess = null;
        });

        // Handle process errors
        this.pythonProcess.on('error', (error: Error) => {
          logger.error('Error in embedding service process', error);
          reject(error);
        });

        // Log stderr output
        this.pythonProcess.stderr.on('data', (data: Buffer) => {
          logger.debug('Embedding service stderr:', { message: data.toString().trim() });
        });

        // Initialize the model
        this.sendCommand({ command: 'initialize' })
          .then(() => {
            this.initialized = true;
            logger.debug('Embedding service initialized successfully');
            resolve();
          })
          .catch((error) => {
            logger.error('Failed to initialize embedding service', error);
            reject(error);
          });
      } catch (error) {
        logger.error('Error initializing embedding service', error);
        reject(error);
      }
    });

    return this.initializationPromise;
  }

  /**
   * Generates an embedding for a single text
   * @param text The text to generate an embedding for
   * @returns The embedding vector
   */
  async generateEmbedding(text: string): Promise<number[]> {
    if (!this.initialized) {
      await this.initialize();
    }

    const result = await this.sendCommand({
      command: 'generate_embedding',
      text,
      task: this.taskDescription,
    });

    if (result.error) {
      throw new Error(`Embedding generation failed: ${result.error}`);
    }

    return result.embedding;
  }

  /**
   * Generate embeddings for multiple texts
   * @param texts Array of texts to generate embeddings for
   * @param options Options for embedding generation
   * @returns Promise that resolves to an array of embedding vectors
   */
  async generateEmbeddings(
    texts: string[],
    options?: { input_type?: 'query' | 'passage' }
  ): Promise<number[][]> {
    if (!this.initialized) {
      await this.initialize();
    }

    if (!texts || texts.length === 0) {
      throw new Error('No texts provided for embedding generation');
    }

    logger.debug(`Generating embeddings for ${texts.length} texts`);

    try {
      const command = {
        command: 'generate_embeddings',
        texts: texts,
        is_query: options?.input_type === 'query',
      };

      const result = await this.sendCommand(command);

      if (result.error) {
        throw new Error(`Batch embedding generation failed: ${result.error}`);
      }

      if (!result.embeddings || !Array.isArray(result.embeddings)) {
        throw new Error('Invalid response from embedding service');
      }

      return result.embeddings;
    } catch (error: any) {
      logger.error('Error generating embeddings', error);
      throw new Error(`Batch embedding generation failed: ${error.message}`);
    }
  }

  /**
   * Sends a command to the Python process
   * @param command The command to send
   * @returns The result from the Python process
   */
  private sendCommand(command: any): Promise<any> {
    return new Promise((resolve, reject) => {
      if (!this.pythonProcess) {
        reject(new Error('Python process not initialized'));
        return;
      }

      // Buffer to accumulate response chunks
      let responseBuffer = '';

      // Set up data handler that accumulates chunks
      const responseHandler = (data: Buffer) => {
        const chunk = data.toString();
        responseBuffer += chunk;

        try {
          // Try to parse the accumulated buffer
          const response = JSON.parse(responseBuffer);

          // If successful parsing, clean up and resolve
          this.pythonProcess.stdout.removeListener('data', responseHandler);
          resolve(response);
        } catch (error) {
          // Incomplete JSON, continue accumulating
          // This is expected for large responses split across chunks
          // We'll keep collecting chunks until we get valid JSON
        }
      };

      // Handle error and end events
      const errorHandler = (error: Error) => {
        this.pythonProcess.stdout.removeListener('data', responseHandler);
        reject(error);
      };

      // Set up event listeners
      this.pythonProcess.stdout.on('data', responseHandler);
      this.pythonProcess.stdout.once('error', errorHandler);

      // Send command to Python process
      this.pythonProcess.stdin.write(JSON.stringify(command) + '\n');

      // Set a reasonable timeout (30 seconds)
      setTimeout(() => {
        this.pythonProcess.stdout.removeListener('data', responseHandler);
        reject(new Error('Timeout waiting for embedding service response'));
      }, 30000);
    });
  }

  /**
   * Closes the embedding service
   */
  async close(): Promise<void> {
    if (this.pythonProcess) {
      this.pythonProcess.kill();
      this.pythonProcess = null;
      this.initialized = false;
      this.initializationPromise = null;
      logger.debug('Embedding service closed');
    }
  }
}
