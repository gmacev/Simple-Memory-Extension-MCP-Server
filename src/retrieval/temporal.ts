import type { TimeWindow } from '../domain/types.js';
import { localizedDates } from './localized-dates.js';

const DAY = 86_400_000;
const MONTHS = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];
const MONTH_PATTERN = MONTHS.join('|');

export interface DatedWindow extends TimeWindow {
  offset: number;
  text: string;
}

function dayWindow(year: number, month: number, day: number): TimeWindow | undefined {
  if (year < 1000 || year > 9998) return undefined;
  const start = new Date(Date.UTC(year, month, day));
  if (
    start.getUTCFullYear() !== year ||
    start.getUTCMonth() !== month ||
    start.getUTCDate() !== day
  ) {
    return undefined;
  }
  return { start: start.toISOString(), end: new Date(start.getTime() + DAY).toISOString() };
}

/** Only unambiguous full calendar dates, never locale-dependent numeric dates. */
export function calendarDates(text: string): DatedWindow[] {
  const clean = text.replace(/https?:\/\/\S+/giu, (url) => ' '.repeat(url.length));
  const found: DatedWindow[] = [];
  for (const match of clean.matchAll(/\b(\d{4})-(\d{2})-(\d{2})(?!\d)/gu)) {
    const window = dayWindow(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
    if (window) found.push({ ...window, offset: match.index, text: match[0] });
  }
  const english = new RegExp(
    `\\b(\\d{1,2})(?:st|nd|rd|th)?\\s+(${MONTH_PATTERN})\\s+(\\d{4})\\b|\\b(${MONTH_PATTERN})\\s+(\\d{1,2})(?:st|nd|rd|th)?,?\\s+(\\d{4})\\b`,
    'giu',
  );
  for (const match of clean.matchAll(english)) {
    const month = MONTHS.indexOf((match[2] ?? match[4] ?? '').toLowerCase());
    const window = dayWindow(Number(match[3] ?? match[6]), month, Number(match[1] ?? match[5]));
    if (window) found.push({ ...window, offset: match.index, text: match[0] });
  }
  for (const date of localizedDates(clean)) {
    const window = dayWindow(date.year, date.month - 1, date.day);
    if (window)
      found.push({ ...window, offset: date.offset, text: text.slice(date.offset, date.endOffset) });
  }
  return found
    .sort((a, b) => a.offset - b.offset || b.text.length - a.text.length)
    .filter(
      (date, index, all) =>
        !all
          .slice(0, index)
          .some(
            (previous) =>
              previous.start === date.start &&
              previous.end === date.end &&
              previous.offset <= date.offset &&
              previous.offset + previous.text.length >= date.offset + date.text.length,
          ),
    );
}

export function queryTimeWindows(query: string): TimeWindow[] {
  // Do not guess the date of a personal event, an unspecified year, or an open-ended
  // before/after relation. Existing history tools remain available for those cases.
  const dates = calendarDates(query);
  if (dates.length === 0 || dates.length > 4) return [];
  const first = dates[0];
  if (!first) return [];
  const prefix = query.slice(0, first.offset);
  if (/\bbetween\s*$/iu.test(prefix) && dates.length === 2) {
    const last = dates[1];
    if (!last || last.start < first.start) return [];
    return [{ start: first.start, end: last.end }];
  }
  const windows: TimeWindow[] = [];
  for (const [index, date] of dates.entries()) {
    const previous = dates[index - 1];
    const context = query.slice(previous ? previous.offset + previous.text.length : 0, date.offset);
    const dayAfter = /\b(?:the\s+)?day\s+after\s*$/iu.test(context);
    const dayBefore = /\b(?:the\s+)?day\s+before\s*$/iu.test(context);
    if (dayAfter || dayBefore) {
      const offset = dayAfter ? DAY : -DAY;
      windows.push({
        start: new Date(Date.parse(date.start) + offset).toISOString(),
        end: new Date(Date.parse(date.end) + offset).toISOString(),
      });
    } else {
      if (/\b(?:before|after|since|until)\b/iu.test(context)) return [];
      if (!/\b(?:on|during|in|and|or)\s*$/iu.test(context) && !/,\s*$/u.test(context)) return [];
      windows.push({ start: date.start, end: date.end });
    }
  }
  return windows;
}

/** Recorded-state routing requires an explicit knowledge/recording cue. */
export function recordedSnapshotTime(query: string): string | undefined {
  if (!/\b(?:recorded|known)\b/iu.test(query)) return undefined;
  const match = /\bas\s+of\s+/iu.exec(query);
  if (!match) return undefined;
  const suffix = query.slice(match.index + match[0].length);
  const date = calendarDates(suffix)[0];
  if (date?.offset !== 0) return undefined;
  const timestamp = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})/u.exec(
    suffix,
  )?.[0];
  if (timestamp)
    return Number.isFinite(Date.parse(timestamp)) ? new Date(timestamp).toISOString() : undefined;
  if (/^\d{4}-\d{2}-\d{2}T/u.test(suffix)) return undefined;
  return new Date(Date.parse(date.end) - 1).toISOString();
}

export interface TemporalSegment {
  id: string;
  path: string;
  text: string;
}

export interface SegmentTime extends TimeWindow {
  segmentId: string;
  basis: 'content' | 'observed';
  passageStart: number;
  passageEnd: number;
}

const sentences = new Intl.Segmenter('und', { granularity: 'sentence' });

/** Offsets refer to the stored segment, never to rewritten or translated text. */
export function datePassages(
  text: string,
  dates: DatedWindow[],
): Array<{
  start: number;
  end: number;
  dates: DatedWindow[];
}> {
  if (new Set(dates.map((date) => date.start)).size < 2)
    return [{ start: 0, end: text.length, dates }];
  const boundaries = new Set([0, text.length]);
  for (const sentence of sentences.segment(text)) boundaries.add(sentence.index);
  // ICU can keep a sentence-ending period and a following numeric date together.
  for (const date of dates) {
    if (/[.!?。！？]\s+$/u.test(text.slice(0, date.offset))) boundaries.add(date.offset);
  }
  for (const match of text.matchAll(/[;；\n]+/gu)) boundaries.add(match.index + match[0].length);
  const points = [...boundaries]
    .filter(
      (point) =>
        !dates.some((date) => point > date.offset && point < date.offset + date.text.length),
    )
    .sort((a, b) => a - b);
  const passages: Array<{ start: number; end: number; dates: DatedWindow[] }> = [];
  let current: (typeof passages)[number] | undefined;
  let heading = false;
  for (let index = 0; index < points.length - 1; index += 1) {
    const start = points[index];
    const end = points[index + 1];
    if (start === undefined || end === undefined) continue;
    const local = dates.filter((date) => date.offset >= start && date.offset < end);
    if (local.length > 0) {
      current = { start, end, dates: local };
      passages.push(current);
      const date = local[0];
      heading =
        local.length === 1 &&
        date !== undefined &&
        /^[\s#]*$/u.test(text.slice(start, date.offset)) &&
        /^[\s:：]*$/u.test(text.slice(date.offset + date.text.length, end));
    } else if (current) {
      // A dated heading scopes its section. Ordinary prose only scopes its paragraph.
      if (!heading && /\n\s*\n/u.test(text.slice(Math.max(current.start, current.end - 2), end)))
        current = undefined;
      else if (/^\s*#{1,6}\s/u.test(text.slice(start, end))) current = undefined;
      else current.end = end;
    }
  }
  const first = passages[0];
  // Preserve shared introductions (environment, scope, caveats) for every date.
  // They contain no date themselves and must not disappear when focusing later events.
  const headings = [...text.matchAll(/^#{1,6}[ \t]+[^\n]+/gmu)].filter(
    (heading) =>
      !dates.some(
        (date) => date.offset >= heading.index && date.offset < heading.index + heading[0].length,
      ),
  );
  const context: typeof passages = [];
  if (first && first.start > 0) {
    const nextHeading = headings.find((heading) => heading.index >= first.start);
    context.push({
      start: 0,
      end: first.start,
      dates: dates.filter((date) => date.offset < (nextHeading?.index ?? text.length)),
    });
  }
  for (const [index, heading] of headings.entries()) {
    if (first && heading.index < first.start) continue;
    const end = headings[index + 1]?.index ?? text.length;
    context.push({
      start: heading.index,
      end: heading.index + heading[0].length,
      dates: dates.filter((date) => date.offset >= heading.index && date.offset < end),
    });
  }
  passages.push(...context);
  return passages;
}

export function passageText(
  segment: TemporalSegment,
  spans: Array<{ start: number; end: number }>,
): string {
  const prefix = `${segment.path}:`;
  const prefixIndex = segment.text.indexOf(prefix);
  const prefixEnd = prefixIndex < 0 ? 0 : prefixIndex + prefix.length;
  const bodyStart = prefixEnd + (/^[ \t]*/u.exec(segment.text.slice(prefixEnd))?.[0].length ?? 0);
  const merged: Array<{ start: number; end: number }> = [];
  for (const span of spans.sort((a, b) => a.start - b.start)) {
    const previous = merged.at(-1);
    if (previous && span.start <= previous.end) previous.end = Math.max(previous.end, span.end);
    else merged.push({ ...span });
  }
  if (merged.some((span) => span.start === 0 && span.end === segment.text.length))
    return segment.text;
  return (
    segment.text.slice(0, bodyStart) +
    merged
      .map((span) => segment.text.slice(Math.max(bodyStart, span.start), span.end))
      .join('\n…\n')
  );
}

function evidenceText(segment: TemporalSegment): string {
  const prefix = `${segment.path}:`;
  const index = segment.text.indexOf(prefix);
  return index < 0 ? segment.text : segment.text.slice(index + prefix.length);
}

function relativeDates(text: string, observedAt: string | null): TimeWindow[] {
  if (!observedAt) return [];
  const reference = calendarDates(new Date(observedAt).toISOString())[0];
  if (!reference) return [];
  return [...text.matchAll(/\b(yesterday|today|tomorrow)\b/giu)].map((match) => {
    const offset =
      match[0].toLowerCase() === 'yesterday'
        ? -DAY
        : match[0].toLowerCase() === 'tomorrow'
          ? DAY
          : 0;
    return {
      start: new Date(Date.parse(reference.start) + offset).toISOString(),
      end: new Date(Date.parse(reference.end) + offset).toISOString(),
    };
  });
}

export function datedExcerpt(text: string, window: TimeWindow, limit: number): string {
  const date = calendarDates(text).find(
    (candidate) => candidate.start < window.end && candidate.end > window.start,
  );
  const start = date ? Math.max(0, date.offset - 200) : 0;
  return text.slice(start, start + limit);
}

export function segmentTimes(
  segments: TemporalSegment[],
  observedAt: string | null,
): SegmentTime[] {
  const inherited = new Map<string, TimeWindow[]>();
  for (const segment of segments) {
    if (!/^\$\//u.test(segment.path) || !/\/(?:date|eventDate|occurredAt)$/u.test(segment.path))
      continue;
    const dates = calendarDates(evidenceText(segment));
    if (dates.length === 1)
      inherited.set(segment.path.slice(0, segment.path.lastIndexOf('/')), dates);
  }
  const output: SegmentTime[] = [];
  for (const segment of segments) {
    if (segment.path !== '$' && !segment.path.startsWith('$/')) continue;
    const body = evidenceText(segment);
    const explicitDates = calendarDates(body);
    if (new Set(explicitDates.map((date) => date.start)).size > 1) {
      const offset = segment.text.length - body.length;
      const indexedDates = new Set(explicitDates.slice(0, 32));
      for (const passage of datePassages(body, explicitDates)) {
        for (const date of passage.dates) {
          if (!indexedDates.has(date)) continue;
          output.push({
            segmentId: segment.id,
            start: date.start,
            end: date.end,
            basis: 'content',
            passageStart: offset + passage.start,
            passageEnd: offset + passage.end,
          });
        }
      }
      continue;
    }
    let dates: TimeWindow[] = explicitDates;
    if (dates.length === 0) dates = relativeDates(evidenceText(segment), observedAt);
    let parent = segment.path.slice(0, segment.path.lastIndexOf('/'));
    while (dates.length === 0 && parent) {
      dates = inherited.get(parent) ?? [];
      parent = parent.slice(0, parent.lastIndexOf('/'));
    }
    if (dates.length > 0) {
      for (const { start, end } of dates.slice(0, 32))
        output.push({
          segmentId: segment.id,
          start,
          end,
          basis: 'content',
          passageStart: 0,
          passageEnd: segment.text.length,
        });
    } else if (observedAt) {
      const date = calendarDates(new Date(observedAt).toISOString())[0];
      if (date)
        output.push({
          segmentId: segment.id,
          start: date.start,
          end: date.end,
          basis: 'observed',
          passageStart: 0,
          passageEnd: segment.text.length,
        });
    }
  }
  return output;
}
