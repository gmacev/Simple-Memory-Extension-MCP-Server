import * as z from 'zod/v4';

const compilationAvailable = (() => {
  try {
    return new Function('return true')() === true;
  } catch {
    return false;
  }
})();

export function compileSchema<T extends z.ZodType>(schema: T): T {
  return compilationAvailable ? z.compile(schema, { strict: true }) : schema;
}
