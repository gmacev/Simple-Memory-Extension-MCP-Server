import { SQLiteDataStore } from './sqlite-store.js';
import { InvalidNamespaceError } from './errors.js';
import { logger } from './logger.js';

/**
 * Creates a new namespace
 * @param db Database connection
 * @param namespace Name of the namespace to create
 * @returns Promise that resolves to true if a new namespace was created, false if it already existed
 */
export async function createNamespace(db: SQLiteDataStore, namespace: string): Promise<boolean> {
  if (!namespace) {
    logger.error('Cannot create namespace: namespace is required');
    throw new InvalidNamespaceError('Namespace is required');
  }

  logger.debug('Creating namespace', { namespace });
  const created = await db.createNamespace(namespace);

  if (created) {
    logger.info('Created new namespace', { namespace });
  } else {
    logger.debug('Namespace already exists', { namespace });
  }

  return created;
}

/**
 * Deletes a namespace and all items within it
 * @param db Database connection
 * @param namespace Name of the namespace to delete
 * @returns Promise that resolves to true if the namespace was deleted, false otherwise
 */
export async function deleteNamespace(db: SQLiteDataStore, namespace: string): Promise<boolean> {
  if (!namespace) {
    logger.error('Cannot delete namespace: namespace is required');
    throw new InvalidNamespaceError('Namespace is required');
  }

  logger.debug('Deleting namespace', { namespace });
  const deleted = await db.deleteNamespace(namespace);

  if (deleted) {
    logger.info('Deleted namespace', { namespace });
  } else {
    logger.debug('Namespace does not exist, nothing to delete', { namespace });
  }

  return deleted;
}

/**
 * Lists all namespaces
 * @param db Database connection
 * @returns Array of namespace names
 */
export async function listNamespaces(db: SQLiteDataStore): Promise<string[]> {
  logger.debug('Listing namespaces');
  return await db.listAllNamespaces();
}
