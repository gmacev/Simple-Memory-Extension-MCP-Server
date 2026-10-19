// Refresh with the project's supported Node/ICU runtime. No network or model needed.
import { writeFileSync } from 'node:fs';

const codes = [];
for (let a = 97; a <= 122; a++) {
  for (let b = 97; b <= 122; b++) {
    codes.push(String.fromCharCode(a, b));
    for (let c = 97; c <= 122; c++) codes.push(String.fromCharCode(a, b, c));
  }
}
const locales = [...new Set(Intl.DateTimeFormat.supportedLocalesOf(codes))].sort();
const patterns = new Map();
const escapeRegex = (text) => text.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
const literal = (text) =>
  escapeRegex(text.replace(/[\u200e\u200f\u061c]/gu, '')).replace(/\s+/gu, '\\s*');
for (const locale of locales) {
  for (const monthStyle of ['long', 'short', 'numeric']) {
    const format = new Intl.DateTimeFormat(locale, {
      calendar: 'gregory',
      numberingSystem: 'latn',
      year: 'numeric',
      month: monthStyle,
      day: 'numeric',
      timeZone: 'UTC',
    });
    const months = Array.from(
      { length: 12 },
      (_, month) =>
        format.formatToParts(new Date(Date.UTC(2026, month, 23))).find((p) => p.type === 'month')
          ?.value,
    );
    if (months.some((month) => month === undefined)) throw new Error(`Missing months: ${locale}`);
    const parts = format.formatToParts(new Date('2026-02-23T00:00:00Z'));
    // Trailing day/year labels vary in prose; the complete numeric fields are sufficient.
    while (parts.at(-1)?.type === 'literal') parts.pop();
    // Numeric-only regional dates have no universally safe month/day ordering.
    if (
      !/\p{L}/u.test(months.join('')) &&
      !parts.some((part) => part.type === 'literal' && /\p{L}/u.test(part.value))
    ) {
      continue;
    }
    const fields = [];
    const hints = months.some((month) => /\p{L}/u.test(month))
      ? months.filter((month) => /\p{L}/u.test(month))
      : parts
          .filter((part) => part.type === 'literal' && /\p{L}/u.test(part.value))
          .map((part) => part.value.trim());
    let source = '';
    for (const part of parts) {
      if (part.type === 'literal') source += literal(part.value);
      else if (['year', 'day', 'month'].includes(part.type)) {
        fields.push(part.type);
        source +=
          part.type === 'year'
            ? '([0-9]{4})'
            : part.type === 'day'
              ? '([0-9]{1,2})'
              : `(${[...new Set(months)]
                  .map(escapeRegex)
                  .sort((a, b) => b.length - a.length)
                  .join('|')})`;
      } else throw new Error(`Unexpected date field ${part.type}`);
    }
    const key = JSON.stringify({ source, fields, months });
    const existing = patterns.get(key);
    if (existing) existing.locales.push(locale);
    else patterns.set(key, { source, fields, months, hints, locales: [locale] });
  }
}
const digitZeros = [];
let inDigits = 0;
for (let cp = 0; cp <= 0x10ffff; cp++) {
  if (/\p{Decimal_Number}/u.test(String.fromCodePoint(cp))) {
    if (inDigits % 10 === 0) digitZeros.push(cp);
    inDigits++;
  } else inDigits = 0;
}
writeFileSync(
  'src/retrieval/locale-date-data.json',
  `${JSON.stringify(
    {
      generatedBy: 'scripts/generate-locale-date-data.mjs',
      license: 'Unicode License v3; see UNICODE-LICENSE.txt',
      icuVersion: process.versions.icu,
      cldrVersion: process.versions.cldr,
      digitZeros,
      patterns: [...patterns.values()],
    },
    null,
    2,
  )}\n`,
);
console.log(JSON.stringify({ locales: locales.length, patterns: patterns.size }));
