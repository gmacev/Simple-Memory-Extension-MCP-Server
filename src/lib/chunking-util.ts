import { logger } from './logger.js';

/**
 * Configuration for text chunking
 */
export interface ChunkingConfig {
  // Maximum tokens per chunk (default matches E5 model constraints)
  maxTokens: number;
  // Tokens to overlap between chunks for context preservation
  overlapTokens: number;
  // Rough estimation of characters per token (varies by language)
  charsPerToken: number;
}

// Default configuration optimized for the E5 model
export const DEFAULT_CHUNKING_CONFIG: ChunkingConfig = {
  maxTokens: 400, // Target 400 tokens to stay safely below 512 limit
  overlapTokens: 50, // ~12.5% overlap to maintain context
  charsPerToken: 4, // Rough estimate - varies by language
};

/**
 * Estimates token count from character count
 * This is a rough approximation - actual tokenization depends on model and language
 */
export function estimateTokenCount(
  text: string,
  charsPerToken: number = DEFAULT_CHUNKING_CONFIG.charsPerToken
): number {
  return Math.ceil(text.length / charsPerToken);
}

/**
 * Chunk text by semantic boundaries like paragraphs and sentences
 * Tries to respect natural text boundaries while staying within token limits
 */
export function chunkTextBySemanticBoundaries(
  text: string,
  config: ChunkingConfig = DEFAULT_CHUNKING_CONFIG
): string[] {
  logger.debug(`Chunking text of length ${text.length} characters`);

  // If text is already small enough, return as-is
  if (estimateTokenCount(text, config.charsPerToken) <= config.maxTokens) {
    logger.debug('Text fits in a single chunk, no chunking needed');
    return [text];
  }

  const chunks: string[] = [];

  // First split by double newlines (paragraphs)
  const paragraphs = text.split(/\n\s*\n/);
  logger.debug(`Split into ${paragraphs.length} paragraphs`);

  let currentChunk = '';
  let currentTokenCount = 0;

  // Process paragraph by paragraph
  for (let i = 0; i < paragraphs.length; i++) {
    const para = paragraphs[i];
    const paraTokens = estimateTokenCount(para, config.charsPerToken);

    // If this paragraph alone exceeds max tokens, split it into sentences
    if (paraTokens > config.maxTokens) {
      logger.debug(`Large paragraph found (est. ${paraTokens} tokens), splitting into sentences`);

      // If we have accumulated content in current chunk, save it first
      if (currentChunk) {
        chunks.push(currentChunk);
        currentChunk = '';
        currentTokenCount = 0;
      }

      // Split large paragraph into sentences and process them
      const sentences = para.split(/(?<=[.!?])\s+/);
      let sentenceChunk = '';
      let sentenceTokenCount = 0;

      for (const sentence of sentences) {
        const sentenceTokens = estimateTokenCount(sentence, config.charsPerToken);

        // If single sentence exceeds limit, we have to split it by character count
        if (sentenceTokens > config.maxTokens) {
          logger.debug(
            `Very long sentence found (est. ${sentenceTokens} tokens), splitting by character count`
          );

          // Save any accumulated content first
          if (sentenceChunk) {
            chunks.push(sentenceChunk);
            sentenceChunk = '';
            sentenceTokenCount = 0;
          }

          // Force split the long sentence into multiple chunks
          const maxChars = config.maxTokens * config.charsPerToken;
          for (let j = 0; j < sentence.length; j += maxChars) {
            const subChunk = sentence.substring(j, j + maxChars);
            chunks.push(subChunk);
          }
        }
        // If adding this sentence exceeds limit, save current and start new
        else if (sentenceTokenCount + sentenceTokens > config.maxTokens) {
          chunks.push(sentenceChunk);

          // Start new chunk with overlap if possible
          if (sentenceChunk && config.overlapTokens > 0) {
            // Extract last N tokens worth of text as overlap
            const overlapChars = config.overlapTokens * config.charsPerToken;
            const overlapText = sentenceChunk.substring(
              Math.max(0, sentenceChunk.length - overlapChars)
            );
            sentenceChunk = overlapText + ' ' + sentence;
            sentenceTokenCount = estimateTokenCount(sentenceChunk, config.charsPerToken);
          } else {
            sentenceChunk = sentence;
            sentenceTokenCount = sentenceTokens;
          }
        }
        // Otherwise add to current sentence chunk
        else {
          sentenceChunk = sentenceChunk ? `${sentenceChunk} ${sentence}` : sentence;
          sentenceTokenCount += sentenceTokens;
        }
      }

      // Add the last sentence chunk if not empty
      if (sentenceChunk) {
        chunks.push(sentenceChunk);
      }
    }
    // If adding this paragraph would exceed the token limit
    else if (currentTokenCount + paraTokens > config.maxTokens) {
      // Save current chunk
      chunks.push(currentChunk);

      // Start new chunk with overlap if possible
      if (currentChunk && config.overlapTokens > 0) {
        // Extract last N tokens worth of text as overlap
        const overlapChars = config.overlapTokens * config.charsPerToken;
        const overlapText = currentChunk.substring(Math.max(0, currentChunk.length - overlapChars));
        currentChunk = overlapText + '\n\n' + para;
        currentTokenCount = estimateTokenCount(currentChunk, config.charsPerToken);
      } else {
        currentChunk = para;
        currentTokenCount = paraTokens;
      }
    }
    // Otherwise add to current chunk
    else {
      if (currentChunk) {
        currentChunk += '\n\n' + para;
      } else {
        currentChunk = para;
      }
      currentTokenCount += paraTokens;
    }
  }

  // Add the last chunk if not empty
  if (currentChunk) {
    chunks.push(currentChunk);
  }

  logger.debug(`Text chunked into ${chunks.length} semantic chunks`);
  return chunks;
}
