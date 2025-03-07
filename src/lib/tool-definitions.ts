import { z } from 'zod';

// Define Zod schemas for tool parameters
const namespaceSchema = z
  .string()
  .min(1, 'Namespace must not be empty')
  .describe(
    'A unique identifier for a collection of related key-value pairs. Use logical naming patterns like "user_preferences", "conversation_context", or "task_data" to organize information.'
  );
const keySchema = z
  .string()
  .min(1, 'Key must not be empty')
  .describe(
    'A unique identifier for a value within a namespace. Use descriptive keys that indicate the data\'s purpose, like "last_search_query", "user_location", or "current_task_id".'
  );
const nSchema = z
  .number()
  .int('Number of turns must be an integer')
  .positive('Number of turns must be greater than 0')
  .describe(
    'A positive integer specifying the quantity of items to retrieve. Use smaller values for recent context, larger values for more comprehensive history.'
  );

// Define tool schemas
export const toolSchemas = {
  // Context Item Management
  retrieve_context_item_by_key: {
    name: 'retrieve_context_item_by_key',
    description:
      'Retrieves a stored value by its key from a specific namespace. Although values are stored as strings, they are automatically parsed and returned with their original data types. For example, JSON objects/arrays will be returned as structured data, numbers as numeric values, booleans as true/false, and null as null.',
    schema: z.object({
      namespace: namespaceSchema,
      key: keySchema,
    }),
  },

  store_context_item: {
    name: 'store_context_item',
    description:
      'Stores a NEW value associated with a key in a specified namespace. IMPORTANT: All values must be passed as strings, but the system will intelligently parse them to preserve their data types. This tool will fail if the key already exists - use update_context_item for modifying existing items.',
    schema: z.object({
      namespace: namespaceSchema,
      key: keySchema,
      value: z
        .string()
        .describe(
          'The value to store as a string. Examples: "{\"name\":\"John\"}" for objects, "[1,2,3]" for arrays, "42.5" for numbers, "true"/"false" for booleans, "null" for null. The system will automatically detect and preserve the appropriate data type when retrieving.'
        ),
    }),
  },

  update_context_item: {
    name: 'update_context_item',
    description:
      'Updates an EXISTING value associated with a key in a specified namespace. This tool will fail if the key does not exist - use store_context_item for creating new items. All values must be passed as strings but will be intelligently parsed to preserve data types.',
    schema: z.object({
      namespace: namespaceSchema,
      key: keySchema,
      value: z
        .string()
        .describe(
          'The new value to store as a string. Examples: "{\"name\":\"John\"}" for objects, "[1,2,3]" for arrays, "42.5" for numbers, "true"/"false" for booleans, "null" for null. The system will automatically detect and preserve the appropriate data type when retrieving.'
        ),
    }),
  },

  delete_context_item: {
    name: 'delete_context_item',
    description:
      'Deletes a key-value pair from a namespace. Use this to remove data that is no longer needed or to clean up temporary storage.',
    schema: z.object({
      namespace: namespaceSchema,
      key: keySchema,
    }),
  },

  // Namespace Management
  create_namespace: {
    name: 'create_namespace',
    description:
      'Creates a new namespace for storing key-value pairs. Use this before storing items in a new namespace. If the namespace already exists, this is a no-op and returns success.',
    schema: z.object({
      namespace: namespaceSchema,
    }),
  },

  delete_namespace: {
    name: 'delete_namespace',
    description:
      'Deletes an entire namespace and all key-value pairs within it. Use this for cleanup when all data in a namespace is no longer needed.',
    schema: z.object({
      namespace: namespaceSchema,
    }),
  },

  list_namespaces: {
    name: 'list_namespaces',
    description:
      'Lists all available namespaces. Use this to discover what namespaces exist before retrieving or storing data.',
    schema: z.object({}),
  },

  // Semantic search
  retrieve_context_items_by_semantic_search: {
    name: 'retrieve_context_items_by_semantic_search',
    description: 'Retrieves context items using semantic search based on query relevance',
    schema: z.object({
      namespace: namespaceSchema,
      query: z.string().describe('The semantic query to search for'),
      similarity_threshold: z
        .number()
        .optional()
        .describe('Minimum similarity score (0-1) to include in results'),
      limit: z.number().optional().describe('Maximum number of results to return'),
    }),
  },

  // Context Item Keys Listing
  list_context_item_keys: {
    name: 'list_context_item_keys',
    description:
      'Lists keys and timestamps for all items in a namespace without retrieving their values. Use this to discover what data is available before retrieving specific items.',
    schema: z.object({
      namespace: namespaceSchema,
    }),
  },
};
