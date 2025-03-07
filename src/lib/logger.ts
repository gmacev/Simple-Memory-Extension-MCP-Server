/**
 * Structured logging utility for the MCP server
 * Provides consistent logging format across the application
 */

// Log levels
export enum LogLevel {
  DEBUG = 'debug',
  INFO = 'info',
  WARN = 'warn',
  ERROR = 'error',
}

// Log entry structure
export interface LogEntry {
  timestamp: string;
  level: LogLevel;
  message: string;
  context?: Record<string, any>;
  error?: Error | unknown;
}

/**
 * Logger class for structured logging
 */
export class Logger {
  private static instance: Logger;
  private logLevel: LogLevel = LogLevel.INFO;

  private constructor() {}

  /**
   * Get the singleton logger instance
   */
  public static getInstance(): Logger {
    if (!Logger.instance) {
      Logger.instance = new Logger();
    }
    return Logger.instance;
  }

  /**
   * Set the minimum log level
   * @param level Minimum log level to display
   */
  public setLogLevel(level: LogLevel): void {
    this.logLevel = level;
  }

  /**
   * Log a debug message
   * @param message Log message
   * @param context Optional context object
   */
  public debug(message: string, context?: Record<string, any>): void {
    this.log(LogLevel.DEBUG, message, context);
  }

  /**
   * Log an info message
   * @param message Log message
   * @param context Optional context object
   */
  public info(message: string, context?: Record<string, any>): void {
    this.log(LogLevel.INFO, message, context);
  }

  /**
   * Log a warning message
   * @param message Log message
   * @param context Optional context object
   */
  public warn(message: string, context?: Record<string, any>): void {
    this.log(LogLevel.WARN, message, context);
  }

  /**
   * Log an error message
   * @param message Log message
   * @param error Error object
   * @param context Optional context object
   */
  public error(message: string, error?: Error | unknown, context?: Record<string, any>): void {
    this.log(LogLevel.ERROR, message, context, error);
  }

  /**
   * Internal logging method
   * @param level Log level
   * @param message Log message
   * @param context Optional context object
   * @param error Optional error object
   */
  private log(
    level: LogLevel,
    message: string,
    context?: Record<string, any>,
    error?: Error | unknown
  ): void {
    // Skip logging if level is below configured level
    if (!this.shouldLog(level)) {
      return;
    }

    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      context,
      error,
    };

    // Format and output the log entry
    this.output(entry);
  }

  /**
   * Check if a log level should be displayed
   * @param level Log level to check
   * @returns True if the log should be displayed
   */
  private shouldLog(level: LogLevel): boolean {
    const levels = [LogLevel.DEBUG, LogLevel.INFO, LogLevel.WARN, LogLevel.ERROR];
    const configuredIndex = levels.indexOf(this.logLevel);
    const messageIndex = levels.indexOf(level);

    return messageIndex >= configuredIndex;
  }

  /**
   * Output a log entry
   * @param entry Log entry to output
   */
  private output(entry: LogEntry): void {
    // Format the log entry
    let output = `[${entry.timestamp}] [${entry.level.toUpperCase()}] ${entry.message}`;

    // Add context if available
    if (entry.context && Object.keys(entry.context).length > 0) {
      output += ` | Context: ${JSON.stringify(entry.context)}`;
    }

    // Add error details if available
    if (entry.error) {
      if (entry.error instanceof Error) {
        output += ` | Error: ${entry.error.message}`;
        if (entry.error.stack) {
          output += `\n${entry.error.stack}`;
        }
      } else {
        output += ` | Error: ${String(entry.error)}`;
      }
    }

    // Output to console based on log level
    switch (entry.level) {
      case LogLevel.DEBUG:
      case LogLevel.INFO:
        console.log(output);
        break;
      case LogLevel.WARN:
        console.warn(output);
        break;
      case LogLevel.ERROR:
        console.error(output);
        break;
    }
  }
}

// Export a singleton instance
export const logger = Logger.getInstance();
