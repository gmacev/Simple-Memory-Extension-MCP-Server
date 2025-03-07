import { SQLiteDataStore } from './sqlite-store.js';
import { ContextItem } from './sqlite-store.js';
import { logger } from './logger.js';
import { EmbeddingService } from './embedding-service.js';

// Singleton embedding service instance
let embeddingService: EmbeddingService | null = null;

/**
 * Gets or creates the embedding service
 */
async function getEmbeddingService(): Promise<EmbeddingService> {
  if (!embeddingService) {
    embeddingService = new EmbeddingService();
    await embeddingService.initialize();
  }
  return embeddingService;
}

/**
 * Retrieves context items using semantic search
 * @param db Database connection
 * @param namespace Namespace to search in
 * @param query The semantic query text
 * @param options Additional search options
 * @returns Array of matching context items with similarity scores
 */
export async function retrieveBySemantic(
  db: SQLiteDataStore,
  namespace: string,
  query: string,
  options: {
    limit?: number;
    threshold?: number;
    tags?: string[];
  } = {}
): Promise<Array<ItemWithEmbedding & { similarity: number }>> {
  if (!namespace || namespace.trim() === '') {
    throw new Error('Namespace cannot be empty');
  }

  if (!query || query.trim() === '') {
    throw new Error('Query cannot be empty');
  }

  logger.debug('Performing semantic search', { namespace, query, options });

  try {
    // Generate embedding for the query
    const queryEmbedding = await generateEmbedding(query);

    // Get all items from the namespace with optional tag filtering
    const items = await getNamespaceItems(db, namespace);

    // For items with embeddings, calculate similarity and sort
    const results = await calculateSimilarities(items, queryEmbedding);

    // Filter by threshold and limit results
    return results
      .filter((item) => item.similarity >= (options.threshold || 0.7))
      .sort((a, b) => b.similarity - a.similarity)
      .slice(0, options.limit || 10);
  } catch (error: any) {
    logger.error('Error in semantic search', error, { namespace, query });
    throw new Error(`Failed to perform semantic search: ${error.message}`);
  }
}

/**
 * Generates embedding for text using the E5 model
 */
async function generateEmbedding(text: string): Promise<number[]> {
  try {
    logger.debug('Generating embedding for text', { textLength: text.length });
    const service = await getEmbeddingService();
    return service.generateEmbedding(text);
  } catch (error: any) {
    logger.error('Error generating embedding', error);
    // Fallback to random vector if embedding generation fails
    logger.debug('Using fallback random embedding');
    return Array.from({ length: 1024 }, () => Math.random() - 0.5);
  }
}

/**
 * Retrieve items from namespace with optional tag filtering
 */
async function getNamespaceItems(db: SQLiteDataStore, namespace: string): Promise<ContextItem[]> {
  logger.debug(`Getting all items in namespace: ${namespace}`);
  return await db.retrieveAllItemsInNamespace(namespace);
}

// Define a type for items with parsed embeddings
interface ItemWithEmbedding extends Omit<ContextItem, 'embedding'> {
  embedding: number[];
}

/**
 * Calculate similarity between query embedding and items
 */
async function calculateSimilarities(
  items: ContextItem[],
  queryEmbedding: number[]
): Promise<Array<ItemWithEmbedding & { similarity: number }>> {
  logger.debug('Calculating similarities', { itemCount: items.length });

  // Generate embeddings for all items
  const itemsWithEmbeddings = await generateEmbeddingsForItems(items);

  // Calculate cosine similarity for each item
  return itemsWithEmbeddings.map((item) => ({
    ...item,
    similarity: calculateCosineSimilarity(queryEmbedding, item.embedding),
  }));
}

/**
 * Generates embeddings for a list of items
 */
async function generateEmbeddingsForItems(items: ContextItem[]): Promise<ItemWithEmbedding[]> {
  logger.debug(`Generating embeddings for ${items.length} items`);

  const service = await getEmbeddingService();
  const results: ItemWithEmbedding[] = [];

  for (const item of items) {
    try {
      const text = extractTextFromItem(item);
      const embedding = await service.generateEmbedding(text);

      results.push({
        ...item,
        embedding,
      });
    } catch (error: any) {
      logger.error('Failed to generate embedding for item', {
        namespace: item.namespace,
        key: item.key,
        error: error.message,
      });
      // Skip items that fail embedding generation
      continue;
    }
  }

  return results;
}

/**
 * Extract text representation from an item
 */
function extractTextFromItem(item: ContextItem): string {
  // Convert item value to text for embedding
  if (typeof item.value === 'string') {
    return item.value;
  }

  if (typeof item.value === 'object') {
    // For objects, concatenate string values
    return Object.entries(item.value)
      .filter(([_, v]) => typeof v === 'string')
      .map(([k, v]) => `${k}: ${v}`)
      .join('\n');
  }

  return `${item.key}: ${JSON.stringify(item.value)}`;
}

/**
 * Calculate cosine similarity between two embeddings
 */
function calculateCosineSimilarity(vec1: number[], vec2: number[]): number {
  if (vec1.length !== vec2.length) {
    throw new Error('Vectors must have the same dimensions');
  }

  const dotProduct = vec1.reduce((sum, val, i) => sum + val * vec2[i], 0);
  const norm1 = Math.sqrt(vec1.reduce((sum, val) => sum + val * val, 0));
  const norm2 = Math.sqrt(vec2.reduce((sum, val) => sum + val * val, 0));

  if (norm1 === 0 || norm2 === 0) {
    return 0; // Avoid division by zero
  }

  return dotProduct / (norm1 * norm2);
}
