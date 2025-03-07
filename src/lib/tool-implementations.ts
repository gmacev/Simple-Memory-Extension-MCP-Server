import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { SQLiteDataStore } from './sqlite-store.js';
import { createNamespace, deleteNamespace, listNamespaces } from './namespace-manager.js';
import {
  storeContextItem,
  retrieveContextItemByKey,
  deleteContextItem,
  listContextItemKeys,
} from './context-manager.js';
import { toolSchemas } from './tool-definitions.js';
import { logger } from './logger.js';
import { normalizeError } from './errors.js';
import { retrieveBySemantic } from './search-module.js';

/**
 * Registers all tool implementations with the MCP server
 * @param server MCP server instance
 * @param db Database connection
 */
export function registerTools(server: McpServer, db: SQLiteDataStore): void {
  server.tool(
    toolSchemas.retrieve_context_item_by_key.name,
    toolSchemas.retrieve_context_item_by_key.description,
    toolSchemas.retrieve_context_item_by_key.schema.shape,
    async ({ namespace, key }) => {
      try {
        logger.debug('Executing retrieve_context_item_by_key tool', { namespace, key });
        const result = await retrieveContextItemByKey(db, namespace, key);

        return {
          content: [{ type: 'text', text: JSON.stringify({ result }) }],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error retrieving context item', normalizedError, { namespace, key });
        return {
          isError: true,
          content: [
            { type: 'text', text: `Error retrieving context item: ${normalizedError.message}` },
          ],
        };
      }
    }
  );

  server.tool(
    toolSchemas.store_context_item.name,
    toolSchemas.store_context_item.description,
    toolSchemas.store_context_item.schema.shape,
    async ({ namespace, key, value }) => {
      try {
        logger.debug('Executing store_context_item tool', { namespace, key });

        // Try to parse the value as JSON or number before storing
        let parsedValue: any = value;

        // First try to parse as JSON
        try {
          // Only attempt to parse if the value looks like JSON (starts with { or [)
          if (value.trim().startsWith('{') || value.trim().startsWith('[')) {
            parsedValue = JSON.parse(value);
            logger.debug('Parsed value as JSON object', { namespace, key });
          }
        } catch (parseError: any) {
          // If JSON parsing fails, keep the original string value
          logger.debug('Failed to parse value as JSON, keeping as string', {
            namespace,
            key,
            error: parseError.message,
          });
        }

        // If it's still a string, try to parse as other types
        if (typeof parsedValue === 'string') {
          const trimmedValue = parsedValue.trim();

          // Check for number
          if (/^-?\d+(\.\d+)?$/.test(trimmedValue)) {
            const numValue = Number(trimmedValue);
            if (!isNaN(numValue)) {
              parsedValue = numValue;
              logger.debug('Parsed value as number', { namespace, key });
            }
          }
          // Check for boolean
          else if (trimmedValue === 'true' || trimmedValue === 'false') {
            parsedValue = trimmedValue === 'true';
            logger.debug('Parsed value as boolean', { namespace, key });
          }
          // Check for null
          else if (trimmedValue === 'null') {
            parsedValue = null;
            logger.debug('Parsed value as null', { namespace, key });
          }
        }

        // Check if the item already exists
        const existingItem = await retrieveContextItemByKey(db, namespace, key);
        if (existingItem) {
          // Item already exists, return as an error
          return {
            isError: true,
            content: [
              {
                type: 'text',
                text: `Error storing context item: An item with the key "${key}" already exists in namespace "${namespace}". Use update_context_item to modify existing items.`,
              },
            ],
          };
        }

        // Store the parsed value
        await storeContextItem(db, namespace, key, parsedValue);
        return {
          content: [{ type: 'text', text: JSON.stringify({ result: true }) }],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error storing context item', normalizedError, { namespace, key });
        return {
          isError: true,
          content: [
            { type: 'text', text: `Error storing context item: ${normalizedError.message}` },
          ],
        };
      }
    }
  );

  server.tool(
    toolSchemas.update_context_item.name,
    toolSchemas.update_context_item.description,
    toolSchemas.update_context_item.schema.shape,
    async ({ namespace, key, value }) => {
      try {
        logger.debug('Executing update_context_item tool', { namespace, key });

        // Check if the item exists
        const existingItem = await retrieveContextItemByKey(db, namespace, key);
        if (!existingItem) {
          // Item doesn't exist, return as an error
          return {
            isError: true,
            content: [
              {
                type: 'text',
                text: `Error updating context item: No item with the key "${key}" exists in namespace "${namespace}". Use store_context_item to create new items.`,
              },
            ],
          };
        }

        // Try to parse the value as JSON or number before storing
        let parsedValue: any = value;

        // First try to parse as JSON
        try {
          // Only attempt to parse if the value looks like JSON (starts with { or [)
          if (value.trim().startsWith('{') || value.trim().startsWith('[')) {
            parsedValue = JSON.parse(value);
            logger.debug('Parsed value as JSON object', { namespace, key });
          }
        } catch (parseError: any) {
          // If JSON parsing fails, keep the original string value
          logger.debug('Failed to parse value as JSON, keeping as string', {
            namespace,
            key,
            error: parseError.message,
          });
        }

        // If it's still a string, try to parse as other types
        if (typeof parsedValue === 'string') {
          const trimmedValue = parsedValue.trim();

          // Check for number
          if (/^-?\d+(\.\d+)?$/.test(trimmedValue)) {
            const numValue = Number(trimmedValue);
            if (!isNaN(numValue)) {
              parsedValue = numValue;
              logger.debug('Parsed value as number', { namespace, key });
            }
          }
          // Check for boolean
          else if (trimmedValue === 'true' || trimmedValue === 'false') {
            parsedValue = trimmedValue === 'true';
            logger.debug('Parsed value as boolean', { namespace, key });
          }
          // Check for null
          else if (trimmedValue === 'null') {
            parsedValue = null;
            logger.debug('Parsed value as null', { namespace, key });
          }
        }

        // Update the item
        await storeContextItem(db, namespace, key, parsedValue);
        return {
          content: [{ type: 'text', text: JSON.stringify({ result: true }) }],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error updating context item', normalizedError, { namespace, key });
        return {
          isError: true,
          content: [
            { type: 'text', text: `Error updating context item: ${normalizedError.message}` },
          ],
        };
      }
    }
  );

  server.tool(
    toolSchemas.delete_context_item.name,
    toolSchemas.delete_context_item.description,
    toolSchemas.delete_context_item.schema.shape,
    async ({ namespace, key }) => {
      try {
        logger.debug('Executing delete_context_item tool', { namespace, key });
        const deleted = await deleteContextItem(db, namespace, key);

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                result: deleted, // Will be true if item was deleted, false if it didn't exist
              }),
            },
          ],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error deleting context item', normalizedError, { namespace, key });
        return {
          isError: true,
          content: [
            { type: 'text', text: `Error deleting context item: ${normalizedError.message}` },
          ],
        };
      }
    }
  );

  // Namespace Management
  server.tool(
    toolSchemas.create_namespace.name,
    toolSchemas.create_namespace.description,
    toolSchemas.create_namespace.schema.shape,
    async ({ namespace }) => {
      try {
        logger.debug('Executing create_namespace tool', { namespace });
        const created = await createNamespace(db, namespace);

        if (!created) {
          // Namespace already exists, return as an error
          return {
            isError: true,
            content: [
              {
                type: 'text',
                text: `Error creating namespace: A namespace with the name "${namespace}" already exists.`,
              },
            ],
          };
        }

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                result: true,
              }),
            },
          ],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error creating namespace', normalizedError, { namespace });
        return {
          isError: true,
          content: [{ type: 'text', text: `Error creating namespace: ${normalizedError.message}` }],
        };
      }
    }
  );

  server.tool(
    toolSchemas.delete_namespace.name,
    toolSchemas.delete_namespace.description,
    toolSchemas.delete_namespace.schema.shape,
    async ({ namespace }) => {
      try {
        logger.debug('Executing delete_namespace tool', { namespace });
        const deleted = await deleteNamespace(db, namespace);

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                result: deleted, // Will be true if namespace was deleted, false if it didn't exist
              }),
            },
          ],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error deleting namespace', normalizedError, { namespace });
        return {
          isError: true,
          content: [{ type: 'text', text: `Error deleting namespace: ${normalizedError.message}` }],
        };
      }
    }
  );

  server.tool(
    toolSchemas.list_namespaces.name,
    toolSchemas.list_namespaces.description,
    toolSchemas.list_namespaces.schema.shape,
    async () => {
      try {
        logger.debug('Executing list_namespaces tool');
        const namespaces = await listNamespaces(db);

        return {
          content: [{ type: 'text', text: JSON.stringify({ result: namespaces }) }],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error listing namespaces', normalizedError);
        return {
          isError: true,
          content: [{ type: 'text', text: `Error listing namespaces: ${normalizedError.message}` }],
        };
      }
    }
  );

  // Keys Listing
  server.tool(
    toolSchemas.list_context_item_keys.name,
    toolSchemas.list_context_item_keys.description,
    toolSchemas.list_context_item_keys.schema.shape,
    async ({ namespace }) => {
      try {
        logger.debug('Executing list_context_item_keys tool', { namespace });
        const keys = await listContextItemKeys(db, namespace);

        return {
          content: [{ type: 'text', text: JSON.stringify({ result: keys }) }],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error listing context item keys', normalizedError, { namespace });
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: `Error listing context item keys: ${normalizedError.message}`,
            },
          ],
        };
      }
    }
  );

  // Semantic search
  server.tool(
    'retrieve_context_items_by_semantic_search',
    toolSchemas.retrieve_context_items_by_semantic_search.schema.shape,
    async ({ namespace, query, limit, similarity_threshold }) => {
      try {
        logger.debug('Executing retrieve_context_items_by_semantic_search tool', {
          namespace,
          query,
          limit,
          similarity_threshold,
        });

        const items = await retrieveBySemantic(db, namespace, query, {
          limit: limit,
          threshold: similarity_threshold,
        });

        logger.debug('Retrieved items by semantic search', {
          namespace,
          query,
          count: items.length,
        });

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                result: items.map((item) => ({
                  value: item.value,
                  similarity: item.similarity,
                  created_at: item.created_at,
                  updated_at: item.updated_at,
                })),
              }),
            },
          ],
        };
      } catch (error: any) {
        const normalizedError = normalizeError(error);
        logger.error('Error retrieving items by semantic search', normalizedError, {
          namespace,
          query,
        });
        return {
          isError: true,
          content: [
            {
              type: 'text',
              text: `Error retrieving items by semantic search: ${normalizedError.message}`,
            },
          ],
        };
      }
    }
  );
}
