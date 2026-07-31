'use strict';

/**
 * Built-in stage factories for the Pipeline.
 *
 * Each factory returns a function `(item) => any|Promise<any>` so the core
 * Pipeline engine can treat them uniformly. They are intentionally tiny —
 * the Pipeline is the API surface; these are conveniences.
 *
 * @module stages
 */

/**
 * Map stage. Returns `fn(item)`.
 *
 * @template T,U
 * @param {(item: T) => U|Promise<U>} fn
 * @returns {(item: T) => U|Promise<U>}
 */
export function mapStage(fn) {
  return async (item) => await fn(item);
}

/**
 * Filter predicate. Returning truthy keeps the item, falsy drops it.
 *
 * The wrapper pushes the kept item through unchanged so the next stage
 * receives a clean value.
 *
 * @param {(item: any) => boolean|Promise<boolean>} fn
 * @returns {(item: any) => any|undefined}
 */
export function filterStage(fn) {
  return async (item) => {
    const keep = await fn(item);
    return keep ? item : undefined;
  };
}

/**
 * Side-effect tap. Runs `fn(item)` and returns the original item.
 *
 * @param {(item: any) => void|Promise<void>} fn
 * @returns {(item: any) => any}
 */
export function tapStage(fn) {
  return async (item) => {
    await fn(item);
    return item;
  };
}

/**
 * Flatten stage. Accepts an array and returns a flat array up to `depth`.
 *
 * @param {number} [depth=1]
 * @returns {(item: any) => any}
 */
export function flattenStage(depth = 1) {
  if (!Number.isFinite(depth) || depth < 0) {
    throw new TypeError(`flatten depth must be a non-negative number, got ${depth}`);
  }
  const d = Math.floor(depth);
  return (item) => {
    if (!Array.isArray(item)) return item;
    return item.flat(d);
  };
}

/**
 * Pluck stage. Extracts a single field by key.
 *
 * Missing keys resolve to `undefined` and are dropped by the Pipeline
 * (since dropping is the default for null/undefined outputs).
 *
 * @param {string|number|symbol} key
 * @returns {(item: any) => any}
 */
export function pluckStage(key) {
  if (key === undefined || key === null) {
    throw new TypeError('pluck requires a non-null key');
  }
  return (item) => (item == null ? undefined : item[key]);
}

/**
 * Debounce stage (per-item). Returns a function that delays each item by
 * `wait` ms, then emits it. Useful when stages upstream produce a stream
 * that needs rate limiting.
 *
 * @param {number} wait - Milliseconds.
 * @returns {(item: any) => Promise<any>}
 */
export function debounceStage(wait) {
  if (!Number.isFinite(wait) || wait < 0) {
    throw new TypeError(`debounce wait must be a non-negative number, got ${wait}`);
  }
  let last = 0;
  return async (item) => {
    const now = Date.now();
    const elapsed = now - last;
    if (elapsed < wait && last !== 0) {
      await new Promise((r) => setTimeout(r, wait - elapsed));
    }
    last = Date.now();
    return item;
  };
}

/**
 * Identity stage. Useful as a no-op for conditional pipelines.
 *
 * @returns {(item: any) => any}
 */
export function identityStage() {
  return async (item) => item;
}

export default {
  mapStage,
  filterStage,
  tapStage,
  flattenStage,
  pluckStage,
  debounceStage,
  identityStage,
};