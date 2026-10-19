import data from './locale-date-data.json' with { type: 'json' };

export interface LocalizedDate {
  year: number;
  month: number;
  day: number;
  offset: number;
  endOffset: number;
}

const patterns = data.patterns.map((pattern) => ({
  ...pattern,
  expression: new RegExp(pattern.source, 'giu'),
  monthNumbers: new Map(pattern.months.map((month, index) => [month.toLowerCase(), index + 1])),
}));
const patternsByHint = new Map<string, Set<(typeof patterns)[number]>>();
for (const pattern of patterns) {
  for (const hint of pattern.hints) {
    const key = hint.toLowerCase();
    const group = patternsByHint.get(key) ?? new Set();
    group.add(pattern);
    patternsByHint.set(key, group);
  }
}
const hintExpression = new RegExp(
  `(?=(${[...patternsByHint.keys()]
    .sort((a, b) => b.length - a.length)
    .map((hint) => hint.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&'))
    .join('|')}))`,
  'giu',
);
const digits = new Map(
  data.digitZeros.flatMap((zero) =>
    Array.from({ length: 10 }, (_, digit) => [String.fromCodePoint(zero + digit), String(digit)]),
  ),
);

/** Normalize digits without losing offsets into the original evidence. */
function normalizeDigits(text: string): { text: string; offsets?: number[] } {
  if (!/[\p{Decimal_Number}--[0-9]]/v.test(text)) return { text };
  let normalized = '';
  const offsets: number[] = [];
  let index = 0;
  for (const character of text) {
    const replacement = digits.get(character) ?? character;
    for (let i = 0; i < replacement.length; i++) offsets.push(index);
    normalized += replacement;
    index += character.length;
  }
  offsets.push(text.length);
  return { text: normalized, offsets };
}

/** Full Gregorian dates in ICU's localized formats, without language detection or date-order guesses. */
export function localizedDates(text: string): LocalizedDate[] {
  if (!/\p{Decimal_Number}{4}/u.test(text)) return [];
  const normalized = normalizeDigits(text);
  const eligible = new Set<(typeof patterns)[number]>();
  for (const hint of normalized.text.matchAll(hintExpression)) {
    for (const pattern of patternsByHint.get(hint[1]?.toLowerCase() ?? '') ?? [])
      eligible.add(pattern);
  }
  const found = new Map<string, LocalizedDate[]>();
  for (const pattern of eligible) {
    for (const match of normalized.text.matchAll(pattern.expression)) {
      const before = normalized.text[match.index - 1];
      const after = normalized.text[match.index + match[0].length];
      // Do not extract dates embedded in identifiers or larger numbers.
      if ((before && /[\p{N}_]/u.test(before)) || (after && /[\p{N}_]/u.test(after))) continue;
      const year = Number(match[pattern.fields.indexOf('year') + 1]);
      const day = Number(match[pattern.fields.indexOf('day') + 1]);
      const monthText = match[pattern.fields.indexOf('month') + 1];
      const month = monthText ? pattern.monthNumbers.get(monthText.toLowerCase()) : undefined;
      const offset = normalized.offsets?.[match.index] ?? match.index;
      const endOffset =
        normalized.offsets?.[match.index + match[0].length] ?? match.index + match[0].length;
      if (!month || offset === undefined || endOffset === undefined) continue;
      const candidate = { year, month, day, offset, endOffset };
      const key = `${offset}:${endOffset}`;
      const alternatives = found.get(key) ?? [];
      alternatives.push(candidate);
      found.set(key, alternatives);
    }
  }
  const output: LocalizedDate[] = [];
  for (const alternatives of found.values()) {
    const first = alternatives[0];
    if (
      first &&
      alternatives.every(
        (date) => date.year === first.year && date.month === first.month && date.day === first.day,
      )
    )
      output.push(first);
  }
  return output.sort((a, b) => a.offset - b.offset || b.endOffset - a.endOffset);
}
