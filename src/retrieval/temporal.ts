import type { TimeWindow } from '../domain/types.js';

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
  return found.sort((a, b) => a.offset - b.offset);
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
    let dates: TimeWindow[] = calendarDates(evidenceText(segment));
    if (dates.length === 0) dates = relativeDates(evidenceText(segment), observedAt);
    let parent = segment.path.slice(0, segment.path.lastIndexOf('/'));
    while (dates.length === 0 && parent) {
      dates = inherited.get(parent) ?? [];
      parent = parent.slice(0, parent.lastIndexOf('/'));
    }
    if (dates.length > 0) {
      for (const { start, end } of dates.slice(0, 32))
        output.push({ segmentId: segment.id, start, end, basis: 'content' });
    } else if (observedAt) {
      const date = calendarDates(new Date(observedAt).toISOString())[0];
      if (date)
        output.push({ segmentId: segment.id, start: date.start, end: date.end, basis: 'observed' });
    }
  }
  return output;
}
