export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const priorities: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export class Logger {
  public constructor(private readonly level: LogLevel) {}

  private write(level: LogLevel, message: string, details?: unknown): void {
    if (priorities[level] < priorities[this.level]) return;
    const suffix = details === undefined ? '' : ` ${JSON.stringify(details)}`;
    process.stderr.write(
      `${new Date().toISOString()} ${level.toUpperCase()} ${message}${suffix}\n`,
    );
  }

  public debug(message: string, details?: unknown): void {
    this.write('debug', message, details);
  }

  public info(message: string, details?: unknown): void {
    this.write('info', message, details);
  }

  public warn(message: string, details?: unknown): void {
    this.write('warn', message, details);
  }

  public error(message: string, details?: unknown): void {
    this.write('error', message, details);
  }
}
