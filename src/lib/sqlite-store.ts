import sqlite3 from 'sqlite3';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';
import { logger } from './logger.js';
import { DatabaseError, normalizeError } from './errors.js';
import { chunkTextBySemanticBoundaries } from './chunking-util.js';

// Define types for database operations
export interface ContextItem {
  namespace: string;
  key: string;
  value: any;
  created_at: string;
  updated_at: string;
}

export interface Database {
  run: (sql: string, ...params: any[]) => Promise<void>;
  get: (sql: string, ...params: any[]) => Promise<any>;
  all: (sql: string, ...params: any[]) => Promise<any[]>;
  close: () => Promise<void>;
}

export interface SQLiteDataStoreOptions {
  dbPath: string;
  // Add optional chunking flag
  enableChunking?: boolean;
}

// Define the DataStore interface
interface DataStore {
  storeDataItem(namespace: string, key: string, value: any): Promise<void>;
  retrieveDataItem(namespace: string, key: string): Promise<any | null>;
  deleteDataItem(namespace: string, key: string): Promise<boolean>;
  dataItemExists(namespace: string, key: string): Promise<boolean>;
  listAllNamespaces(): Promise<string[]>;
  listContextItemKeys(namespace: string): Promise<any[]>;
  createNamespace(namespace: string): Promise<boolean>;
  deleteNamespace(namespace: string): Promise<boolean>;
  // Optional method for non-chunked embeddings
  retrieveDataItemsByEmbeddingSimilarity?(
    namespace: string,
    queryText: string,
    embeddingService: any,
    options: any
  ): Promise<any[]>;
  retrieveAllItemsInNamespace(namespace: string): Promise<any[]>;
}

export class SQLiteDataStore implements DataStore {
  private dbPath: string;
  private db: sqlite3.Database | null = null;
  // Add chunking option
  private enableChunking: boolean;

  constructor(options: SQLiteDataStoreOptions) {
    this.dbPath = options.dbPath;
    // Default to false for backward compatibility
    this.enableChunking = options.enableChunking ?? false;
    logger.debug(
      `SQLite data store initialized, chunking ${this.enableChunking ? 'enabled' : 'disabled'}`
    );
  }

  /**
   * Get or create a database connection
   */
  async getDb(): Promise<sqlite3.Database> {
    if (!this.db) {
      logger.debug('Creating new database connection');
      this.db = new sqlite3.Database(this.dbPath);

      // Enable foreign key constraints
      const run = promisify(this.db.run.bind(this.db)) as (
        sql: string,
        ...params: any[]
      ) => Promise<void>;
      await run('PRAGMA foreign_keys = ON');
      logger.debug('Foreign key constraints enabled');

      await this.initializeDatabase();
    }
    return this.db;
  }

  /**
   * Check if a data item exists
   */
  async dataItemExists(namespace: string, key: string): Promise<boolean> {
    const db = await this.getDb();
    const get = promisify(db.get.bind(db)) as (sql: string, ...params: any[]) => Promise<any>;

    try {
      const result = await get(
        'SELECT 1 FROM context_items WHERE namespace = ? AND key = ?',
        namespace,
        key
      );
      return !!result;
    } catch (error) {
      logger.error(
        `Error checking if data item exists | Context: ${JSON.stringify({ namespace, key, error })}`
      );
      throw error;
    }
  }

  /**
   * Initializes the SQLite database and creates necessary tables if they don't exist
   * @param dbPath Path to the SQLite database file
   * @returns A database connection object
   */
  async initializeDatabase(): Promise<Database> {
    try {
      logger.info('Initializing database', { dbPath: this.dbPath });

      // Ensure the directory exists
      const dbDir = path.dirname(this.dbPath);
      if (!fs.existsSync(dbDir)) {
        logger.debug('Creating database directory', { dbDir });
        fs.mkdirSync(dbDir, { recursive: true });
      }

      // Create a new database connection
      const db = new sqlite3.Database(this.dbPath);
      logger.debug('Database connection established');

      // Promisify database methods with proper type assertions
      const run = promisify(db.run.bind(db)) as (sql: string, ...params: any[]) => Promise<void>;
      const get = promisify(db.get.bind(db)) as (sql: string, ...params: any[]) => Promise<any>;
      const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;
      const close = promisify(db.close.bind(db)) as () => Promise<void>;

      // Create tables if they don't exist
      logger.debug("Creating tables if they don't exist");
      await run(`
        CREATE TABLE IF NOT EXISTS namespaces (
          namespace TEXT PRIMARY KEY,
          created_at TEXT NOT NULL
        )
      `);

      await run(`
        CREATE TABLE IF NOT EXISTS context_items (
          namespace TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (namespace, key),
          FOREIGN KEY (namespace) REFERENCES namespaces(namespace) ON DELETE CASCADE
        )
      `);

      // Create embeddings table for chunked items
      await run(`
        CREATE TABLE IF NOT EXISTS embeddings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          namespace TEXT NOT NULL,
          item_key TEXT NOT NULL,
          chunk_index INTEGER NOT NULL,
          chunk_text TEXT NOT NULL,
          embedding TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY (namespace, item_key) REFERENCES context_items(namespace, key) ON DELETE CASCADE
        )
      `);

      // Create indexes for embeddings table
      await run(
        'CREATE INDEX IF NOT EXISTS idx_embeddings_namespace_item_key ON embeddings(namespace, item_key)'
      );
      await run(
        'CREATE INDEX IF NOT EXISTS idx_embeddings_has_embedding ON embeddings(namespace, embedding IS NOT NULL)'
      );

      // Create indexes for performance
      logger.debug("Creating indexes if they don't exist");
      await run(
        'CREATE INDEX IF NOT EXISTS idx_context_items_namespace ON context_items(namespace)'
      );

      logger.info('Database initialization completed successfully');
      return {
        run,
        get,
        all,
        close,
      };
    } catch (error) {
      const normalizedError = normalizeError(error);
      logger.error('Failed to initialize database', normalizedError, { dbPath: this.dbPath });
      throw new DatabaseError(`Failed to initialize database: ${normalizedError.message}`);
    }
  }

  /**
   * Store a data item with optional chunking support
   */
  async storeDataItem(namespace: string, key: string, value: any): Promise<void> {
    const db = await this.getDb();
    const run = promisify(db.run.bind(db)) as (sql: string, ...params: any[]) => Promise<void>;
    const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;

    logger.debug(`Storing data item | Context: ${JSON.stringify({ namespace, key })}`);

    // Check if the item already exists
    const exists = await this.dataItemExists(namespace, key);

    const timestamp = new Date().toISOString();

    try {
      logger.debug(
        `Beginning transaction for storing data item | Context: ${JSON.stringify({ namespace, key })}`
      );
      await run('BEGIN TRANSACTION');

      if (exists) {
        logger.debug(`Updating existing item | Context: ${JSON.stringify({ namespace, key })}`);
        await run(
          `UPDATE context_items
           SET value = ?, updated_at = ?
           WHERE namespace = ? AND key = ?`,
          JSON.stringify(value),
          timestamp,
          namespace,
          key
        );

        // If chunking is enabled, delete existing chunks before adding new ones
        if (this.enableChunking) {
          logger.debug(
            `Deleting existing chunks for updated item | Context: ${JSON.stringify({ namespace, key })}`
          );
          await run('DELETE FROM embeddings WHERE namespace = ? AND item_key = ?', namespace, key);
        }
      } else {
        logger.debug(`Inserting new item | Context: ${JSON.stringify({ namespace, key })}`);
        await run(
          `INSERT INTO context_items
           (namespace, key, value, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)`,
          namespace,
          key,
          JSON.stringify(value),
          timestamp,
          timestamp
        );
      }

      // Process chunking if enabled
      if (this.enableChunking) {
        // Extract the text value for chunking - stringify objects
        const textValue = typeof value === 'string' ? value : JSON.stringify(value);

        // Create chunks based on semantic boundaries
        const chunks = chunkTextBySemanticBoundaries(textValue);

        logger.debug(
          `Storing ${chunks.length} chunks for item | Context: ${JSON.stringify({ namespace, key })}`
        );

        // Store each chunk with its index
        for (let i = 0; i < chunks.length; i++) {
          await run(
            `INSERT INTO embeddings
             (namespace, item_key, chunk_index, chunk_text, created_at)
             VALUES (?, ?, ?, ?, ?)`,
            namespace,
            key,
            i,
            chunks[i],
            timestamp
          );
        }

        // Immediately generate embeddings for the newly created chunks
        try {
          // Import the embedding service dynamically to avoid circular dependencies
          const { EmbeddingService } = await import('./embedding-service.js');
          const embeddingService = new EmbeddingService();
          await embeddingService.initialize();

          logger.debug(
            `Generating embeddings for chunks | Context: ${JSON.stringify({ namespace, key, count: chunks.length })}`
          );

          // Get the chunk IDs for the newly inserted chunks
          const chunkIds = await all(
            `SELECT id, chunk_text FROM embeddings 
             WHERE namespace = ? AND item_key = ? AND embedding IS NULL
             ORDER BY chunk_index`,
            namespace,
            key
          );

          if (chunkIds.length > 0) {
            // Extract just the text for embedding generation
            const chunkTexts = chunkIds.map((chunk) => chunk.chunk_text);

            // Generate embeddings
            const embeddings = await embeddingService.generateEmbeddings(chunkTexts);

            // Update each chunk with its embedding
            for (let i = 0; i < chunkIds.length; i++) {
              const embedding = JSON.stringify(embeddings[i]);
              await run(
                `UPDATE embeddings SET embedding = ? WHERE id = ?`,
                embedding,
                chunkIds[i].id
              );
            }

            logger.debug(
              `Embeddings generated and stored for ${chunkIds.length} chunks | Context: ${JSON.stringify({ namespace, key })}`
            );
          }
        } catch (error) {
          // Log the error but don't fail the store operation
          logger.error(
            `Error generating embeddings for chunks | Context: ${JSON.stringify({ namespace, key, error })}`
          );
        }
      }

      await run('COMMIT');
      logger.debug(
        `Transaction committed successfully | Context: ${JSON.stringify({ namespace, key })}`
      );
    } catch (error) {
      logger.error(
        `Error storing data item | Context: ${JSON.stringify({ namespace, key, error })}`
      );
      await run('ROLLBACK');
      throw error;
    }
  }

  /**
   * Generate embeddings for all chunks that don't have them yet
   */
  async generateEmbeddingsForChunks(namespace: string, embeddingService: any): Promise<number> {
    if (!this.enableChunking) {
      logger.debug(`Chunking is disabled, skipping embedding generation`);
      return 0;
    }

    const db = await this.getDb();
    const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;
    const run = promisify(db.run.bind(db)) as (sql: string, ...params: any[]) => Promise<void>;

    // Find chunks without embeddings
    const chunksWithoutEmbeddings = await all(
      `SELECT id, namespace, item_key, chunk_index, chunk_text
       FROM embeddings
       WHERE namespace = ? AND embedding IS NULL
       ORDER BY item_key, chunk_index`,
      namespace
    );

    if (chunksWithoutEmbeddings.length === 0) {
      logger.debug(`No chunks found without embeddings for namespace: ${namespace}`);
      return 0;
    }

    logger.debug(
      `Generating embeddings for ${chunksWithoutEmbeddings.length} chunks | Context: { namespace: ${namespace} }`
    );

    let processedCount = 0;

    // Process chunks in batches to avoid memory issues
    const BATCH_SIZE = 10;
    for (let i = 0; i < chunksWithoutEmbeddings.length; i += BATCH_SIZE) {
      const batch = chunksWithoutEmbeddings.slice(i, i + BATCH_SIZE);
      const chunkTexts = batch.map((chunk) => chunk.chunk_text);

      try {
        // Generate embeddings for the batch
        const embeddings = await embeddingService.generateEmbeddings(chunkTexts);

        // Update each chunk with its embedding
        await run('BEGIN TRANSACTION');
        for (let j = 0; j < batch.length; j++) {
          const chunk = batch[j];
          const embedding = JSON.stringify(embeddings[j]);

          await run(
            `UPDATE embeddings
             SET embedding = ?
             WHERE id = ?`,
            embedding,
            chunk.id
          );
          processedCount++;
        }
        await run('COMMIT');

        logger.debug(
          `Generated embeddings for batch ${i / BATCH_SIZE + 1} | Context: { count: ${batch.length} }`
        );
      } catch (error) {
        logger.error(
          `Error generating embeddings for batch ${i / BATCH_SIZE + 1} | Context: ${JSON.stringify(error)}`
        );
        await run('ROLLBACK');
        throw error;
      }
    }

    logger.debug(`Completed embedding generation | Context: { processed: ${processedCount} }`);
    return processedCount;
  }

  /**
   * Retrieve items by semantic search, using the chunked embeddings if enabled
   */
  async retrieveDataItemsBySemanticSearch(
    namespace: string,
    queryText: string,
    embeddingService: any,
    options: {
      limit?: number;
      similarityThreshold?: number;
    } = {}
  ): Promise<any[]> {
    logger.debug(
      `Performing semantic search | Context: ${JSON.stringify({ namespace, queryText, options })}`
    );

    const { limit = 10, similarityThreshold = 0.5 } = options;

    // If chunking is not enabled, fall back to the legacy method if it exists
    if (!this.enableChunking) {
      logger.debug(`Chunking disabled, using legacy semantic search method`);
      return this.retrieveDataItemsByEmbeddingSimilarity(
        namespace,
        queryText,
        embeddingService,
        options
      );
    }

    const db = await this.getDb();
    const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;

    try {
      // Generate embedding for the query
      logger.debug(
        `Generating embedding for text | Context: ${JSON.stringify({ textLength: queryText.length })}`
      );
      const queryEmbedding = await embeddingService.generateEmbeddings([queryText], {
        input_type: 'query',
      });

      if (!queryEmbedding || !queryEmbedding[0] || queryEmbedding[0].length === 0) {
        logger.error(`Failed to generate query embedding`);
        return [];
      }

      // Get all chunks with embeddings
      const chunksWithEmbeddings = await all(
        `SELECT c.namespace, c.item_key, c.chunk_index, c.chunk_text, c.embedding,
                i.value, i.tags, i.description, i.created_at, i.updated_at
         FROM embeddings c
         JOIN context_items i ON c.namespace = i.namespace AND c.item_key = i.key
         WHERE c.namespace = ? AND c.embedding IS NOT NULL`,
        namespace
      );

      if (chunksWithEmbeddings.length === 0) {
        logger.debug(`No chunks with embeddings found for namespace: ${namespace}`);
        return [];
      }

      logger.debug(
        `Calculating similarities | Context: ${JSON.stringify({ itemCount: chunksWithEmbeddings.length })}`
      );

      // Calculate similarity scores
      const similarities = chunksWithEmbeddings.map((chunk) => {
        const chunkEmbedding = JSON.parse(chunk.embedding);
        const similarity = this.calculateCosineSimilarity(queryEmbedding[0], chunkEmbedding);
        return {
          ...chunk,
          similarity,
        };
      });

      // Filter by similarity threshold and sort by similarity
      const filteredResults = similarities
        .filter((item) => item.similarity >= similarityThreshold)
        .sort((a, b) => b.similarity - a.similarity);

      // Group by item to avoid duplicates, taking the highest similarity score
      const itemMap = new Map();
      for (const result of filteredResults) {
        const itemKey = `${result.namespace}:${result.item_key}`;

        if (!itemMap.has(itemKey) || itemMap.get(itemKey).similarity < result.similarity) {
          // Parse the value field
          try {
            result.value = JSON.parse(result.value);
          } catch (e) {
            // If parsing fails, keep the original value
          }

          // Parse tags
          try {
            result.tags = JSON.parse(result.tags);
          } catch (e) {
            result.tags = [];
          }

          itemMap.set(itemKey, result);
        }
      }

      // Convert back to array and take top results
      const results = Array.from(itemMap.values())
        .slice(0, limit)
        .map((item) => ({
          namespace: item.namespace,
          key: item.item_key,
          value: item.value,
          tags: item.tags,
          description: item.description,
          matchedChunk: item.chunk_text,
          similarity: item.similarity,
          created_at: item.created_at,
          updated_at: item.updated_at,
        }));

      logger.debug(
        `Retrieved items by semantic search | Context: ${JSON.stringify({ namespace, queryText, count: results.length })}`
      );

      return results;
    } catch (error) {
      logger.error(
        `Error retrieving items by semantic search | Context: ${JSON.stringify({ namespace, error })}`
      );
      throw error;
    }
  }

  /**
   * Legacy method for retrieving items by embedding similarity (without chunking)
   */
  async retrieveDataItemsByEmbeddingSimilarity(
    namespace: string,
    queryText: string,
    embeddingService: any,
    options: {
      limit?: number;
      similarityThreshold?: number;
    } = {}
  ): Promise<any[]> {
    logger.warn(`Legacy embedding similarity search called, but not implemented`);
    return []; // Empty implementation - this would be implemented in your system if needed
  }

  /**
   * Retrieve a data item by namespace and key
   */
  async retrieveDataItem(namespace: string, key: string): Promise<any | null> {
    const db = await this.getDb();
    const get = promisify(db.get.bind(db)) as (sql: string, ...params: any[]) => Promise<any>;

    try {
      logger.debug(`Retrieving data item | Context: ${JSON.stringify({ namespace, key })}`);

      const result = await get(
        'SELECT * FROM context_items WHERE namespace = ? AND key = ?',
        namespace,
        key
      );

      if (!result) {
        logger.debug(`Data item not found | Context: ${JSON.stringify({ namespace, key })}`);
        return null;
      }

      try {
        // Parse JSON value
        result.value = JSON.parse(result.value);
      } catch (e) {
        // If parsing fails, keep the original value
        logger.warn(
          `Failed to parse value as JSON | Context: ${JSON.stringify({ namespace, key })}`
        );
      }

      logger.debug(
        `Data item retrieved successfully | Context: ${JSON.stringify({ namespace, key })}`
      );
      return result;
    } catch (error) {
      logger.error(
        `Error retrieving data item | Context: ${JSON.stringify({ namespace, key, error })}`
      );
      throw error;
    }
  }

  /**
   * Delete a data item
   */
  async deleteDataItem(namespace: string, key: string): Promise<boolean> {
    const db = await this.getDb();
    const run = promisify(db.run.bind(db)) as (sql: string, ...params: any[]) => Promise<void>;

    try {
      logger.debug(`Deleting data item | Context: ${JSON.stringify({ namespace, key })}`);

      await run('BEGIN TRANSACTION');

      // First delete any associated chunks if chunking is enabled
      if (this.enableChunking) {
        await run('DELETE FROM embeddings WHERE namespace = ? AND item_key = ?', namespace, key);
      }

      // Then delete the main item
      const result = await run(
        'DELETE FROM context_items WHERE namespace = ? AND key = ?',
        namespace,
        key
      );

      await run('COMMIT');

      logger.debug(`Data item deleted | Context: ${JSON.stringify({ namespace, key, result })}`);
      return true; // SQLite doesn't return affected rows in the same way as other DBs
    } catch (error) {
      await run('ROLLBACK');
      logger.error(
        `Error deleting data item | Context: ${JSON.stringify({ namespace, key, error })}`
      );
      throw error;
    }
  }

  /**
   * List all namespaces
   */
  async listAllNamespaces(): Promise<string[]> {
    const db = await this.getDb();
    const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;

    try {
      logger.debug('Listing all namespaces');

      const results = await all('SELECT namespace FROM namespaces ORDER BY namespace');
      const namespaces = results.map((row) => row.namespace);

      logger.debug(`Retrieved ${namespaces.length} namespaces`);
      return namespaces;
    } catch (error) {
      logger.error(`Error listing namespaces | Context: ${JSON.stringify(error)}`);
      throw error;
    }
  }

  /**
   * List context item keys
   */
  async listContextItemKeys(namespace: string): Promise<any[]> {
    const db = await this.getDb();
    const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;

    try {
      logger.debug(`Listing context item keys | Context: ${JSON.stringify({ namespace })}`);

      const results = await all(
        'SELECT key, created_at, updated_at FROM context_items WHERE namespace = ? ORDER BY key',
        namespace
      );

      logger.debug(
        `Retrieved keys for ${results.length} items | Context: ${JSON.stringify({ namespace })}`
      );
      return results;
    } catch (error) {
      logger.error(
        `Error listing context item keys | Context: ${JSON.stringify({ namespace, error })}`
      );
      throw error;
    }
  }

  /**
   * Create a namespace
   */
  async createNamespace(namespace: string): Promise<boolean> {
    const db = await this.getDb();
    const get = promisify(db.get.bind(db)) as (sql: string, ...params: any[]) => Promise<any>;
    const run = promisify(db.run.bind(db)) as (sql: string, ...params: any[]) => Promise<void>;

    try {
      logger.debug(`Creating namespace | Context: ${JSON.stringify({ namespace })}`);

      // Check if namespace already exists
      const existingNamespace = await get(
        'SELECT 1 FROM namespaces WHERE namespace = ?',
        namespace
      );

      if (existingNamespace) {
        logger.debug(`Namespace already exists | Context: ${JSON.stringify({ namespace })}`);
        return false;
      }

      // Create the namespace
      await run(
        'INSERT INTO namespaces (namespace, created_at) VALUES (?, ?)',
        namespace,
        new Date().toISOString()
      );

      logger.debug(`Namespace created successfully | Context: ${JSON.stringify({ namespace })}`);
      return true;
    } catch (error) {
      logger.error(`Error creating namespace | Context: ${JSON.stringify({ namespace, error })}`);
      throw error;
    }
  }

  /**
   * Delete a namespace
   */
  async deleteNamespace(namespace: string): Promise<boolean> {
    const db = await this.getDb();
    const get = promisify(db.get.bind(db)) as (sql: string, ...params: any[]) => Promise<any>;
    const run = promisify(db.run.bind(db)) as (sql: string, ...params: any[]) => Promise<void>;

    try {
      logger.debug(`Attempting to delete namespace | Context: ${JSON.stringify({ namespace })}`);

      // Check if namespace exists
      const existingNamespace = await get(
        'SELECT 1 FROM namespaces WHERE namespace = ?',
        namespace
      );

      if (!existingNamespace) {
        logger.debug(
          `Namespace does not exist, nothing to delete | Context: ${JSON.stringify({ namespace })}`
        );
        return false;
      }

      // Delete using a transaction to ensure atomicity
      logger.debug(
        `Beginning transaction for namespace deletion | Context: ${JSON.stringify({ namespace })}`
      );
      await run('BEGIN TRANSACTION');

      // Delete all context items in namespace (cascades to embeddings due to foreign key)
      logger.debug(
        `Deleting all context items in namespace | Context: ${JSON.stringify({ namespace })}`
      );
      await run('DELETE FROM context_items WHERE namespace = ?', namespace);

      // Delete the namespace
      logger.debug(`Deleting namespace | Context: ${JSON.stringify({ namespace })}`);
      await run('DELETE FROM namespaces WHERE namespace = ?', namespace);

      await run('COMMIT');

      logger.debug(`Namespace deleted successfully | Context: ${JSON.stringify({ namespace })}`);
      return true;
    } catch (error) {
      await run('ROLLBACK');
      logger.error(`Error deleting namespace | Context: ${JSON.stringify({ namespace, error })}`);
      throw error;
    }
  }

  /**
   * Calculate cosine similarity between two embedding vectors
   */
  private calculateCosineSimilarity(vec1: number[], vec2: number[]): number {
    if (vec1.length !== vec2.length) {
      throw new Error(`Vector dimensions don't match: ${vec1.length} vs ${vec2.length}`);
    }

    let dotProduct = 0;
    let mag1 = 0;
    let mag2 = 0;

    for (let i = 0; i < vec1.length; i++) {
      dotProduct += vec1[i] * vec2[i];
      mag1 += vec1[i] * vec1[i];
      mag2 += vec2[i] * vec2[i];
    }

    mag1 = Math.sqrt(mag1);
    mag2 = Math.sqrt(mag2);

    if (mag1 === 0 || mag2 === 0) {
      return 0;
    }

    return dotProduct / (mag1 * mag2);
  }

  /**
   * Execute a SQL query with parameters and return a single row
   * (For compatibility with old Database interface)
   */
  async get(sql: string, ...params: any[]): Promise<any> {
    const db = await this.getDb();
    const get = promisify(db.get.bind(db)) as (sql: string, ...params: any[]) => Promise<any>;

    try {
      logger.debug(`Executing SQL get query: ${sql}`);
      const result = await get(sql, ...params);
      return result;
    } catch (error) {
      logger.error(`Error executing SQL get query: ${error}`);
      throw error;
    }
  }

  /**
   * Execute a SQL query with parameters and return all rows
   * (For compatibility with old Database interface)
   */
  async all(sql: string, ...params: any[]): Promise<any[]> {
    const db = await this.getDb();
    const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;

    try {
      logger.debug(`Executing SQL all query: ${sql}`);
      const results = await all(sql, ...params);
      return results;
    } catch (error) {
      logger.error(`Error executing SQL all query: ${error}`);
      throw error;
    }
  }

  /**
   * Execute a SQL query with parameters (no return value)
   * (For compatibility with old Database interface)
   */
  async run(sql: string, ...params: any[]): Promise<void> {
    const db = await this.getDb();
    const run = promisify(db.run.bind(db)) as (sql: string, ...params: any[]) => Promise<void>;

    try {
      logger.debug(`Executing SQL run query: ${sql}`);
      await run(sql, ...params);
    } catch (error) {
      logger.error(`Error executing SQL run query: ${error}`);
      throw error;
    }
  }

  /**
   * Close the database connection
   * (For compatibility with old Database interface)
   */
  async close(): Promise<void> {
    if (this.db) {
      const db = this.db;
      await new Promise<void>((resolve, reject) => {
        db.close((err) => {
          if (err) {
            reject(err);
          } else {
            this.db = null;
            resolve();
          }
        });
      });
    }
  }

  /**
   * Retrieve all items in a namespace
   */
  async retrieveAllItemsInNamespace(namespace: string): Promise<any[]> {
    const db = await this.getDb();
    const all = promisify(db.all.bind(db)) as (sql: string, ...params: any[]) => Promise<any[]>;

    const results = await all(
      'SELECT * FROM context_items WHERE namespace = ? ORDER BY key',
      namespace
    );

    return results.map((result) => {
      try {
        result.value = JSON.parse(result.value);
      } catch (e) {
        // If parsing fails, keep the original value
      }

      return result;
    });
  }
}
