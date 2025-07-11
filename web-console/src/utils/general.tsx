/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Classes, Icon, Intent } from '@blueprintjs/core';
import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';
import copy from 'copy-to-clipboard';
import * as JSONBig from 'json-bigint-native';
import numeral from 'numeral';
import type { JSX } from 'react';

import { AppToaster } from '../singletons';

// These constants are used to make sure that they are not constantly recreated thrashing the pure components
export const EMPTY_OBJECT: any = {};
export const EMPTY_ARRAY: any[] = [];

export type NumberLike = number | bigint;

export function isNumberLike(x: unknown): x is NumberLike {
  const t = typeof x;
  return t === 'number' || t === 'bigint';
}

export function isNumberLikeNaN(x: NumberLike): boolean {
  return isNaN(Number(x));
}

export function nonEmptyString(s: unknown): s is string {
  return typeof s === 'string' && s !== '';
}

export function nonEmptyArray(a: unknown): a is unknown[] {
  return Array.isArray(a) && Boolean(a.length);
}

export function isSimpleArray(a: any): a is (string | number | boolean)[] {
  return (
    Array.isArray(a) &&
    a.every(x => {
      const t = typeof x;
      return t === 'string' || t === 'number' || t === 'boolean';
    })
  );
}

export function arraysEqualByElement<T>(xs: T[], ys: T[]): boolean {
  return xs.length === ys.length && xs.every((x, i) => x === ys[i]);
}

export function wait(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

export function clamp(n: number, min = -Infinity, max = Infinity): number {
  return Math.min(Math.max(n, min), max);
}

export function addOrUpdate<T>(xs: readonly T[], x: T, keyFn: (x: T) => string | number): T[] {
  const keyX = keyFn(x);
  let added = false;
  const newXs = xs.map(currentX => {
    if (keyFn(currentX) === keyX) {
      added = true;
      return x;
    } else {
      return currentX;
    }
  });
  if (!added) {
    newXs.push(x);
  }
  return newXs;
}

// ----------------------------

export function caseInsensitiveEquals(str1: string | undefined, str2: string | undefined): boolean {
  return str1?.toLowerCase() === str2?.toLowerCase();
}

export function caseInsensitiveContains(testString: string, searchString: string): boolean {
  if (!searchString) return true;
  return testString.toLowerCase().includes(searchString.toLowerCase());
}

function validateKnown<T>(allKnownValues: T[], options: T[]): void {
  options.forEach(o => {
    if (!allKnownValues.includes(o)) {
      throw new Error(`allKnownValues (${allKnownValues.join(', ')}) must include '${o}'`);
    }
  });
}

export function oneOf<T>(value: T, ...options: T[]): boolean {
  return options.includes(value);
}

export function oneOfKnown<T>(value: T, allKnownValues: T[], ...options: T[]): boolean | undefined {
  validateKnown(allKnownValues, options);
  if (options.includes(value)) return true;
  return allKnownValues.includes(value) ? false : undefined;
}

export function typeIs<T extends { type?: S }, S = string>(...options: S[]): (x: T) => boolean {
  return x => {
    if (x.type == null) return false;
    return options.includes(x.type);
  };
}

export function typeIsKnown<T extends { type?: S }, S = string>(
  allKnownValues: S[],
  ...options: S[]
): (x: T) => boolean | undefined {
  validateKnown(allKnownValues, options);
  return x => {
    const value = x.type;
    if (value == null) return;
    if (options.includes(value)) return true;
    return allKnownValues.includes(value) ? false : undefined;
  };
}

export function without<T>(xs: readonly T[], x: T | undefined): T[] {
  return xs.filter(i => i !== x);
}

export function change<T>(xs: readonly T[], from: T, to: T): T[] {
  return xs.map(x => (x === from ? to : x));
}

// ----------------------------

export function countBy<T>(
  array: readonly T[],
  fn: (x: T, index: number) => string | number = String,
): Record<string, number> {
  const counts: Record<string, number> = {};
  for (let i = 0; i < array.length; i++) {
    const key = fn(array[i], i);
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function identity<T>(x: T): T {
  return x;
}

export function lookupBy<T, Q = T>(
  array: readonly T[],
  keyFn: (x: T, index: number) => string | number = String,
  valueFn?: (x: T, index: number) => Q,
): Record<string, Q> {
  if (!valueFn) valueFn = identity as any;
  const lookup: Record<string, Q> = {};
  const n = array.length;
  for (let i = 0; i < n; i++) {
    const a = array[i];
    lookup[keyFn(a, i)] = valueFn!(a, i);
  }
  return lookup;
}

export function mapRecord<T, Q>(
  record: Record<string, T>,
  fn: (value: T, key: string) => Q,
): Record<string, Q> {
  const newRecord: Record<string, Q> = {};
  const keys = Object.keys(record);
  for (const key of keys) {
    const mapped = fn(record[key], key);
    if (typeof mapped === 'undefined') continue;
    newRecord[key] = mapped;
  }
  return newRecord;
}

export function mapRecordOrReturn<T>(
  record: Record<string, T>,
  fn: (value: T, key: string) => T | undefined,
): Record<string, T> {
  const newRecord: Record<string, T> = {};
  let changed = false;
  const keys = Object.keys(record);
  for (const key of keys) {
    const v = record[key];
    const mapped = fn(v, key);
    if (v !== mapped) changed = true;
    if (typeof mapped === 'undefined') continue;
    newRecord[key] = mapped;
  }
  return changed ? newRecord : record;
}

export function groupBy<T, Q>(
  array: readonly T[],
  keyFn: (x: T, index: number) => string,
  aggregateFn: (xs: readonly T[], key: string) => Q,
): Q[] {
  const buckets: Record<string, T[]> = {};
  const n = array.length;
  for (let i = 0; i < n; i++) {
    const value = array[i];
    const key = keyFn(value, i);
    buckets[key] = buckets[key] || [];
    buckets[key].push(value);
  }
  return Object.entries(buckets).map(([key, xs]) => aggregateFn(xs, key));
}

export function groupByAsMap<T, Q>(
  array: readonly T[],
  keyFn: (x: T, index: number) => string | number,
  aggregateFn: (xs: readonly T[], key: string) => Q,
): Record<string, Q> {
  const buckets: Record<string, T[]> = {};
  const n = array.length;
  for (let i = 0; i < n; i++) {
    const value = array[i];
    const key = keyFn(value, i);
    buckets[key] = buckets[key] || [];
    buckets[key].push(value);
  }
  return mapRecord(buckets, aggregateFn);
}

export function uniq<T>(array: readonly T[], by: (t: T) => string = String): T[] {
  const seen: Record<string, boolean> = {};
  return array.filter(s => {
    const key = by(s);
    if (Object.hasOwn(seen, key)) {
      return false;
    } else {
      seen[key] = true;
      return true;
    }
  });
}

export function allSameValue<T>(xs: readonly T[]): T | undefined {
  const sameValue: T | undefined = xs[0];
  for (let i = 1; i < xs.length; i++) {
    if (sameValue !== xs[i]) return;
  }
  return sameValue;
}

// ----------------------------

export function formatEmpty(str: string): string {
  return str === '' ? 'empty' : str;
}

// ----------------------------

export function formatInteger(n: NumberLike): string {
  return numeral(n).format('0,0');
}

export function formatNumber(n: NumberLike): string {
  return (n || 0).toLocaleString('en-US', { maximumFractionDigits: 20 });
}

export function formatNumberAbbreviated(n: NumberLike): string {
  return (n || 0).toLocaleString('en-US', {
    notation: 'compact',
    compactDisplay: 'short',
    maximumFractionDigits: 2,
  });
}

export function formatRate(n: NumberLike) {
  return numeral(n).format('0,0.0') + '/s';
}

export function formatBytes(n: NumberLike): string {
  return numeral(n).format('0.00 b');
}

export function formatByteRate(n: NumberLike): string {
  return numeral(n).format('0.00 b') + '/s';
}

export function formatBytesCompact(n: NumberLike): string {
  return numeral(n).format('0.00b');
}

export function formatByteRateCompact(n: NumberLike): string {
  return numeral(n).format('0.00b') + '/s';
}

export function formatMegabytes(n: NumberLike): string {
  return numeral(Number(n) / 1048576).format('0,0.0');
}

export function formatPercent(n: NumberLike): string {
  return (Number(n) * 100).toFixed(2) + '%';
}

export function formatPercentClapped(n: NumberLike): string {
  return formatPercent(Math.min(Math.max(Number(n), 0), 1));
}

export function formatMillions(n: NumberLike): string {
  const s = (Number(n) / 1e6).toFixed(3);
  if (s === '0.000') return String(Math.round(Number(n)));
  return s + ' M';
}

export function forceSignInNumberFormatter(
  formatter: (n: NumberLike) => string,
): (n: NumberLike) => string {
  return (n: NumberLike) => {
    if (n > 0) {
      return '+' + formatter(n);
    } else {
      return formatter(n);
    }
  };
}

function sign(n: NumberLike): string {
  return n < 0 ? '-' : '';
}

function pad2(str: string | number): string {
  return ('00' + str).slice(-2);
}

function pad3(str: string | number): string {
  return ('000' + str).slice(-3);
}

export function formatDuration(ms: NumberLike): string {
  const n = Math.abs(Number(ms));
  const timeInHours = Math.floor(n / 3600000);
  const timeInMin = Math.floor(n / 60000) % 60;
  const timeInSec = Math.floor(n / 1000) % 60;
  return sign(ms) + timeInHours + ':' + pad2(timeInMin) + ':' + pad2(timeInSec);
}

export function formatDurationWithMs(ms: NumberLike): string {
  const n = Math.abs(Number(ms));
  const timeInHours = Math.floor(n / 3600000);
  const timeInMin = Math.floor(n / 60000) % 60;
  const timeInSec = Math.floor(n / 1000) % 60;
  return (
    sign(ms) +
    timeInHours +
    ':' +
    pad2(timeInMin) +
    ':' +
    pad2(timeInSec) +
    '.' +
    pad3(Math.floor(n) % 1000)
  );
}

export function formatDurationWithMsIfNeeded(ms: NumberLike): string {
  return Number(ms) < 1000 ? formatDurationWithMs(ms) : formatDuration(ms);
}

export function formatDurationHybrid(ms: NumberLike): string {
  const n = Math.abs(Number(ms));
  if (n < 600000) {
    // anything that looks like 1:23.45 (max 9:59.99)
    const timeInMin = Math.floor(n / 60000);
    const timeInSec = Math.floor(n / 1000) % 60;
    const timeInMs = Math.floor(n) % 1000;
    return `${sign(ms)}${timeInMin ? `${timeInMin}:` : ''}${
      timeInMin ? pad2(timeInSec) : timeInSec
    }.${pad3(timeInMs).slice(0, 2)}s`;
  } else {
    return formatDuration(ms);
  }
}

export function timezoneOffsetInMinutesToString(offsetInMinutes: number, padHour: boolean): string {
  const sign = offsetInMinutes < 0 ? '-' : '+';
  const absOffset = Math.abs(offsetInMinutes);
  const h = Math.floor(absOffset / 60);
  const m = absOffset % 60;
  return `${sign}${padHour ? pad2(h) : h}:${pad2(m)}`;
}

function pluralize(word: string): string {
  // Ignoring irregular plurals.
  if (/(s|x|z|ch|sh)$/.test(word)) {
    return word + 'es';
  } else if (/([^aeiou])y$/.test(word)) {
    return word.slice(0, -1) + 'ies';
  } else if (/(f|fe)$/.test(word)) {
    return word.replace(/fe?$/, 'ves');
  } else {
    return word + 's';
  }
}

export function pluralIfNeeded(n: NumberLike, singular: string, plural?: string): string {
  if (!plural) plural = pluralize(singular);
  return `${formatInteger(n)} ${n === 1 ? singular : plural}`;
}

// ----------------------------

export function partition<T>(xs: T[], predicate: (x: T, i: number) => boolean): [T[], T[]] {
  const match: T[] = [];
  const nonMatch: T[] = [];

  for (let i = 0; i < xs.length; i++) {
    const x = xs[i];
    if (predicate(x, i)) {
      match.push(x);
    } else {
      nonMatch.push(x);
    }
  }

  return [match, nonMatch];
}

export function filterMap<T, Q>(xs: readonly T[], f: (x: T, i: number) => Q | undefined): Q[] {
  return xs.map(f).filter((x: Q | undefined) => typeof x !== 'undefined') as Q[];
}

export function filterMapOrReturn<T>(
  xs: readonly T[],
  f: (x: T, i: number) => T | undefined,
): readonly T[] {
  let changed = false;
  const newXs = filterMap(xs, (x, i) => {
    const newX = f(x, i);
    if (typeof newX === 'undefined' || x !== newX) changed = true;
    return newX;
  });
  return changed ? newXs : xs;
}

export function filterOrReturn<T>(xs: readonly T[], f: (x: T, i: number) => unknown): readonly T[] {
  const newXs = xs.filter(f);
  return newXs.length === xs.length ? xs : newXs;
}

export function findMap<T, Q>(
  xs: readonly T[],
  f: (x: T, i: number) => Q | undefined,
): Q | undefined {
  return filterMap(xs, f)[0];
}

export function minBy<T>(xs: T[], f: (item: T, index: number) => number): T | undefined {
  if (!xs.length) return undefined;

  let minItem = xs[0];
  let minValue = f(xs[0], 0);

  for (let i = 1; i < xs.length; i++) {
    const currentValue = f(xs[i], i);
    if (currentValue < minValue) {
      minValue = currentValue;
      minItem = xs[i];
    }
  }

  return minItem;
}

export function changeByIndex<T>(
  xs: readonly T[],
  i: number,
  f: (x: T, i: number) => T | undefined,
): T[] {
  return filterMap(xs, (x, j) => (j === i ? f(x, i) : x));
}

export function compact<T>(xs: (T | undefined | false | null | '')[]): T[] {
  return xs.filter(Boolean) as T[];
}

export function assemble<T>(...xs: (T | undefined | false | null | '')[]): T[] {
  return compact(xs);
}

export function removeUndefinedValues<T extends Record<string, any>>(obj: T): Partial<T> {
  return Object.fromEntries(
    Object.entries(obj).filter(([_, value]) => value !== undefined),
  ) as Partial<T>;
}

export function moveToEnd<T>(
  xs: T[],
  predicate: (value: T, index: number, array: T[]) => unknown,
): T[] {
  return xs.filter((x, i, a) => !predicate(x, i, a)).concat(xs.filter(predicate));
}

export function alphanumericCompare(a: string, b: string): number {
  return String(a).localeCompare(b, undefined, { numeric: true });
}

export function zeroDivide(a: number, b: number): number {
  if (b === 0) return 0;
  return a / b;
}

export function capitalizeFirst(str: string): string {
  return str.slice(0, 1).toUpperCase() + str.slice(1).toLowerCase();
}

export function arrangeWithPrefixSuffix(
  things: readonly string[],
  prefix: readonly string[],
  suffix: readonly string[],
): string[] {
  const pre = uniq(prefix.filter(x => things.includes(x)));
  const mid = things.filter(x => !prefix.includes(x) && !suffix.includes(x));
  const post = uniq(suffix.filter(x => things.includes(x)));
  return pre.concat(mid, post);
}

// ----------------------------

export function copyAndAlert(copyString: string, alertMessage: string): void {
  copy(copyString, { format: 'text/plain' });
  AppToaster.show({
    message: alertMessage,
    intent: Intent.SUCCESS,
  });
}

export function delay(ms: number) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

export function swapElements<T>(items: readonly T[], indexA: number, indexB: number): T[] {
  const newItems = items.concat();
  const t = newItems[indexA];
  newItems[indexA] = newItems[indexB];
  newItems[indexB] = t;
  return newItems;
}

export function moveElement<T>(items: readonly T[], fromIndex: number, toIndex: number): T[] {
  const indexDiff = fromIndex - toIndex;
  if (indexDiff > 0) {
    // move left
    return [
      ...items.slice(0, toIndex),
      items[fromIndex],
      ...items.slice(toIndex, fromIndex),
      ...items.slice(fromIndex + 1, items.length),
    ];
  } else if (indexDiff < 0) {
    // move right
    const targetIndex = toIndex + 1;
    return [
      ...items.slice(0, fromIndex),
      ...items.slice(fromIndex + 1, targetIndex),
      items[fromIndex],
      ...items.slice(targetIndex, items.length),
    ];
  } else {
    // do nothing
    return items.slice();
  }
}

export function moveToIndex<T>(
  items: readonly T[],
  itemToIndex: (item: T, i: number) => number,
): T[] {
  const frontItems: { item: T; index: number }[] = [];
  const otherItems: T[] = [];
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const index = itemToIndex(item, i);
    if (index >= 0) {
      frontItems.push({ item, index });
    } else {
      otherItems.push(item);
    }
  }

  return frontItems
    .sort((a, b) => a.index - b.index)
    .map(d => d.item)
    .concat(otherItems);
}

export function stringifyValue(value: unknown): string {
  switch (typeof value) {
    case 'object':
      if (!value) return String(value);
      if (typeof (value as any).toISOString === 'function') return (value as any).toISOString();
      return JSONBig.stringify(value);

    default:
      return String(value);
  }
}

export function isInBackground(): boolean {
  return document.visibilityState === 'hidden';
}

export function twoLines(line1: string | JSX.Element, line2: string | JSX.Element) {
  return (
    <>
      {line1}
      <br />
      {line2}
    </>
  );
}

export function parseCsvLine(line: string): string[] {
  line = ',' + line.replace(/\r?\n?$/, ''); // remove trailing new lines
  const parts: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = /^,(?:"([^"]*(?:""[^"]*)*)"|([^,\r\n]*))/m.exec(line))) {
    parts.push(typeof m[1] === 'string' ? m[1].replace(/""/g, '"') : m[2]);
    line = line.slice(m[0].length);
  }
  return parts;
}

// From: https://en.wikipedia.org/wiki/Jenkins_hash_function
export function hashJoaat(str: string): number {
  let hash = 0;
  const n = str.length;
  for (let i = 0; i < n; i++) {
    hash += str.charCodeAt(i);
    // eslint-disable-next-line no-bitwise
    hash += hash << 10;
    // eslint-disable-next-line no-bitwise
    hash ^= hash >> 6;
  }
  // eslint-disable-next-line no-bitwise
  hash += hash << 3;
  // eslint-disable-next-line no-bitwise
  hash ^= hash >> 11;
  // eslint-disable-next-line no-bitwise
  hash += hash << 15;
  // eslint-disable-next-line no-bitwise
  return (hash & 4294967295) >>> 0;
}

export const OVERLAY_OPEN_SELECTOR = `.${Classes.PORTAL} .${Classes.OVERLAY_OPEN}`;

export function hasOverlayOpen(): boolean {
  return Boolean(document.querySelector(OVERLAY_OPEN_SELECTOR));
}

export function checkedCircleIcon(checked: boolean, exclude?: boolean): IconName {
  return checked ? (exclude ? IconNames.CROSS_CIRCLE : IconNames.TICK_CIRCLE) : IconNames.CIRCLE;
}

export function tickIcon(checked: boolean): IconName {
  return checked ? IconNames.TICK : IconNames.BLANK;
}

export function generate8HexId(): string {
  return (Math.random() * 1e10).toString(16).replace('.', '').slice(0, 8);
}

export interface RowColumn {
  row: number;
  column: number;
}

export function offsetToRowColumn(str: string, offset: number): RowColumn | undefined {
  // Ensure offset is within the string length
  if (offset < 0 || offset > str.length) return;

  const lines = str.split('\n');
  for (let row = 0; row < lines.length; row++) {
    const line = lines[row];
    if (offset <= line.length) {
      return {
        row,
        column: offset,
      };
    }

    offset -= line.length + 1;
  }

  return;
}

export function xor(a: unknown, b: unknown): boolean {
  return Boolean(a ? !b : b);
}

export function toggle<T>(xs: readonly T[], x: T, eq?: (a: T, b: T) => boolean): T[] {
  const e = eq || ((a, b) => a === b);
  return xs.find(_ => e(_, x)) ? xs.filter(d => !e(d, x)) : xs.concat([x]);
}

export const EXPERIMENTAL_ICON = (
  <Icon icon={IconNames.LAB_TEST} intent={Intent.WARNING} data-tooltip="Experimental" />
);
