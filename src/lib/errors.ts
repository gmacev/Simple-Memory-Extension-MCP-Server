/**
 * Custom error classes for better error classification and handling
 */

/**
 * Base error class for all MCP server errors
 */
export class McpServerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'McpServerError';
    // Ensure proper prototype chain for instanceof checks
    Object.setPrototypeOf(this, McpServerError.prototype);
  }
}

/**
 * Database-related errors
 */
export class DatabaseError extends McpServerError {
  constructor(
    message: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'DatabaseError';
    Object.setPrototypeOf(this, DatabaseError.prototype);
  }
}

/**
 * Validation-related errors
 */
export class ValidationError extends McpServerError {
  constructor(message: string) {
    super(message);
    this.name = 'ValidationError';
    Object.setPrototypeOf(this, ValidationError.prototype);
  }
}

/**
 * Error for invalid namespace names
 */
export class InvalidNamespaceError extends ValidationError {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidNamespaceError';
    Object.setPrototypeOf(this, InvalidNamespaceError.prototype);
  }
}

/**
 * Error for invalid key names
 */
export class InvalidKeyError extends ValidationError {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidKeyError';
    Object.setPrototypeOf(this, InvalidKeyError.prototype);
  }
}

/**
 * Helper function to convert unknown errors to typed errors
 * @param error The original error
 * @param defaultMessage Default message if error is not an Error object
 * @returns A properly typed error
 */
export function normalizeError(error: unknown, defaultMessage = 'Unknown error'): Error {
  if (error instanceof Error) {
    return error;
  }

  if (typeof error === 'string') {
    return new Error(error);
  }

  return new Error(defaultMessage);
}
