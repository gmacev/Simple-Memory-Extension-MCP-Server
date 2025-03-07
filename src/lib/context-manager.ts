import { SQLiteDataStore } from './sqlite-store.js';
import { InvalidNamespaceError, InvalidKeyError } from './errors.js';
import { logger } from './logger.js';

/**
 * Stores a context item
 * @param db Database connection
 * @param namespace Namespace for the context item
 * @param key Key for the context item
 * @param value Value to store
 * @returns Promise that resolves when the operation is complete
 */
export async function storeContextItem(
  db: SQLiteDataStore,
  namespace: string,
  key: string,
  value: any
): Promise<void> {
  if (!namespace || namespace.trim() === '') {
    logger.warn('Attempted to store context item with empty namespace', { key });
    throw new InvalidNamespaceError('Namespace cannot be empty');
  }

  if (!key || key.trim() === '') {
    logger.warn('Attempted to store context item with empty key', { namespace });
    throw new InvalidKeyError('Key cannot be empty');
  }

  logger.debug('Storing context item', { namespace, key });
  // Store the item in the database
  await db.storeDataItem(namespace, key, value);
}

/**
 * Retrieves a context item by its key
 * @param db Database connection
 * @param namespace Namespace for the context item
 * @param key Key for the context item
 * @returns The context item with timestamps or null if not found
 */
export async function retrieveContextItemByKey(
  db: SQLiteDataStore,
  namespace: string,
  key: string
): Promise<{ value: any; created_at: string; updated_at: string } | null> {
  if (!namespace || namespace.trim() === '') {
    logger.warn('Attempted to retrieve context item with empty namespace', { key });
    throw new InvalidNamespaceError('Namespace cannot be empty');
  }

  if (!key || key.trim() === '') {
    logger.warn('Attempted to retrieve context item with empty key', { namespace });
    throw new InvalidKeyError('Key cannot be empty');
  }

  logger.debug('Retrieving context item by key', { namespace, key });
  const item = await db.retrieveDataItem(namespace, key);

  if (!item) {
    logger.debug('Context item not found', { namespace, key });
    return null;
  }

  logger.debug('Context item retrieved successfully', { namespace, key });
  return {
    value: item.value,
    created_at: item.created_at,
    updated_at: item.updated_at,
  };
}

/**
 * Delete a context item
 * @param db Database connection
 * @param namespace Namespace to delete from
 * @param key Key to delete
 * @returns True if item was deleted, false if it did not exist
 */
export async function deleteContextItem(
  db: SQLiteDataStore,
  namespace: string,
  key: string
): Promise<boolean> {
  if (!namespace || namespace.trim() === '') {
    logger.warn('Attempted to delete context item with empty namespace', { key });
    throw new InvalidNamespaceError('Namespace cannot be empty');
  }

  if (!key || key.trim() === '') {
    logger.warn('Attempted to delete context item with empty key', { namespace });
    throw new InvalidKeyError('Key cannot be empty');
  }

  logger.debug('Deleting context item', { namespace, key });
  const deleted = await db.deleteDataItem(namespace, key);
  logger.debug('Context item deletion result', { namespace, key, deleted });
  return deleted;
}

/**
 * List context item keys in a namespace
 * @param db Database instance
 * @param namespace Namespace to list items from
 * @returns Array of key objects for all items in the namespace
 */
export async function listContextItemKeys(
  db: SQLiteDataStore,
  namespace: string
): Promise<
  Array<{
    key: string;
    created_at: string;
    updated_at: string;
  }>
> {
  if (!namespace || namespace.trim() === '') {
    logger.warn('Attempted to list context item keys with empty namespace');
    throw new InvalidNamespaceError('Namespace cannot be empty');
  }

  logger.debug('Listing context item keys', { namespace });
  const keys = await db.listContextItemKeys(namespace);
  logger.debug('Listed context item keys', { namespace, count: keys.length });
  return keys;
}
