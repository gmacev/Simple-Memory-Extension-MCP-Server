import type { AppConfig } from '../config.js';
import { Indexer } from '../indexing/indexer.js';
import { Logger } from '../logger.js';
import { ModelClient } from '../models/model-client.js';
import { SearchEngine } from '../retrieval/search-engine.js';
import { MemoryStore } from '../storage/memory-store.js';
import { MemoryService } from './memory-service.js';

export function createMemoryService(config: AppConfig): MemoryService {
  const logger = new Logger(config.logLevel);
  const store = new MemoryStore(config, logger);
  const models = new ModelClient(config, logger);
  const indexer = new Indexer(config, store, models, logger);
  const search = new SearchEngine(config, store, models, logger);
  return new MemoryService(config, store, indexer, search, models, logger);
}
