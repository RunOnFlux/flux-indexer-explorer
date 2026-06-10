import type { Response } from 'express';

const ZATOSHIS_PER_FLUX = 100000000n;
const DEFAULT_RANGE_LIMIT = 10;
const MAX_RANGE_LIMIT = 50;
const MAX_RANGE_FROM = Number.MAX_SAFE_INTEGER - MAX_RANGE_LIMIT;
const HASH_REGEX = /^[0-9a-fA-F]{1,64}$/;
const DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;

export interface InsightRange {
  from: number;
  to: number;
  limit: number;
}

export interface InsightBlockDateRange {
  start: number;
  end: number;
  current: string;
  next: string;
  prev: string;
}

export interface InsightErrorResponse {
  status: number;
  url?: string;
  error: string;
}

export type LegacySatoshiValue = number | string;

export function zatoshisToFlux(value: bigint | string | number): number {
  return Number(zatoshisToFluxString(value));
}

export function zatoshisToFluxString(value: bigint | string | number): string {
  const zatoshis = toZatoshis(value);
  const isNegative = zatoshis < 0n;
  const absoluteValue = isNegative ? -zatoshis : zatoshis;
  const whole = absoluteValue / ZATOSHIS_PER_FLUX;
  const fractional = absoluteValue % ZATOSHIS_PER_FLUX;
  const prefix = isNegative ? '-' : '';

  return `${prefix}${whole.toString()}.${fractional.toString().padStart(8, '0')}`;
}

export function parseAddressList(pathAddresses?: unknown, bodyAddresses?: unknown): string[] {
  const rawAddresses = coerceStringParam(pathAddresses) ?? coerceAddressBody(bodyAddresses);

  if (!rawAddresses) {
    return [];
  }

  return rawAddresses
    .split(',')
    .map((address) => address.trim())
    .filter((address) => address.length > 0);
}

export function parseRange(query: Record<string, unknown>): InsightRange {
  const from = Math.min(parseNonNegativeInt(query.from, 0), MAX_RANGE_FROM);
  const defaultTo = from + DEFAULT_RANGE_LIMIT;
  const requestedTo = parseNonNegativeInt(query.to, defaultTo);
  const validTo = requestedTo <= from ? defaultTo : requestedTo;
  const to = Math.min(validTo, from + MAX_RANGE_LIMIT);

  return {
    from,
    to,
    limit: to - from,
  };
}

export function parseLimit(raw: unknown, defaultValue = DEFAULT_RANGE_LIMIT, maxValue = MAX_RANGE_LIMIT): number {
  const boundedDefault = Math.min(defaultValue, maxValue);
  const value = isRecord(raw) && 'limit' in raw
    ? parseNonNegativeInt(raw.limit, boundedDefault)
    : parseNonNegativeInt(raw, boundedDefault);

  return value <= 0 ? boundedDefault : Math.min(value, maxValue);
}

export function normalizeHash(hash: string): string {
  const normalized = hash.trim();

  if (normalized.length === 0) {
    throw new Error('Invalid hash (must not be empty)');
  }

  if (normalized.length > 64) {
    throw new Error('Invalid hash (must be 64 hex characters or fewer)');
  }

  if (!/^[0-9a-fA-F]+$/.test(normalized)) {
    throw new Error('Invalid hash (must be hex)');
  }

  return normalized.toLowerCase().padStart(64, '0');
}

export function isValidHash(hash: unknown): hash is string {
  const value = coerceStringParam(hash);
  return value !== undefined && HASH_REGEX.test(value.trim());
}

export function parseBlockDate(blockDate?: unknown, now = new Date()): InsightBlockDateRange {
  const current = coerceStringParam(blockDate)?.trim() || formatUtcDate(now);

  if (!DATE_REGEX.test(current)) {
    throw new Error('Invalid blockDate (expected YYYY-MM-DD)');
  }

  const startDate = new Date(`${current}T00:00:00.000Z`);
  if (Number.isNaN(startDate.getTime()) || formatUtcDate(startDate) !== current) {
    throw new Error('Invalid blockDate (expected a real UTC date)');
  }

  const nextDate = addUtcDays(startDate, 1);
  const prevDate = addUtcDays(startDate, -1);
  const start = Math.floor(startDate.getTime() / 1000);

  return {
    start,
    end: start + 86400 - 1,
    current,
    next: formatUtcDate(nextDate),
    prev: formatUtcDate(prevDate),
  };
}

export function createNotFound(url: string): InsightErrorResponse {
  return {
    status: 404,
    url,
    error: 'Not found',
  };
}

export function sendNotFound(res: Response, url: string): Response {
  return res.status(404).json(createNotFound(url));
}

export function sendBadRequest(res: Response, error: string): Response {
  return res.status(400).json({
    message: error,
    code: 1,
  });
}

export function toZatoshis(value: bigint | string | number): bigint {
  if (typeof value === 'bigint') {
    return value;
  }

  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value)) {
      throw new Error('Invalid zatoshi amount: number inputs must be safe integers');
    }

    return BigInt(value);
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return 0n;
  }

  if (!/^-?\d+$/.test(trimmed)) {
    throw new Error('Invalid zatoshi amount: expected an integer');
  }

  return BigInt(trimmed);
}

export function zatoshisToSafeNumber(value: bigint | string | number): LegacySatoshiValue {
  const zatoshis = toZatoshis(value);

  if (
    zatoshis <= BigInt(Number.MAX_SAFE_INTEGER)
    && zatoshis >= BigInt(Number.MIN_SAFE_INTEGER)
  ) {
    return Number(zatoshis);
  }

  return zatoshis.toString();
}

export function toSafeInteger(value: string | number, name: string): number {
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value)) {
      throw new Error(`Invalid ${name}: must be a safe integer`);
    }

    return value;
  }

  const trimmed = value.trim();
  if (!/^-?\d+$/.test(trimmed)) {
    throw new Error(`Invalid ${name}: expected an integer`);
  }

  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid ${name}: must be a safe integer`);
  }

  return parsed;
}

function coerceAddressBody(bodyAddresses: unknown): string | undefined {
  if (isRecord(bodyAddresses)) {
    return coerceStringParam(bodyAddresses.addrs)
      ?? coerceStringParam(bodyAddresses.addresses)
      ?? coerceStringParam(bodyAddresses.address);
  }

  return coerceStringParam(bodyAddresses);
}

function coerceStringParam(raw: unknown): string | undefined {
  if (raw === undefined || raw === null) {
    return undefined;
  }

  if (Array.isArray(raw)) {
    if (raw.length === 0) {
      return undefined;
    }

    return coerceStringParam(raw[0]);
  }

  return typeof raw === 'string' ? raw : String(raw);
}

function parseNonNegativeInt(raw: unknown, defaultValue: number): number {
  const value = coerceStringParam(raw)?.trim();

  if (!value) {
    return defaultValue;
  }

  if (!/^\d+$/.test(value)) {
    return defaultValue;
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    return defaultValue;
  }

  return parsed;
}

function formatUtcDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addUtcDays(date: Date, days: number): Date {
  const result = new Date(date.getTime());
  result.setUTCDate(result.getUTCDate() + days);
  return result;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
