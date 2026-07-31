'use strict';

/**
 * Tiny Pipeline - Async stream pipeline builder for AI agent data workflows.
 *
 * @module pipeline
 */

import {
  mapStage,
  filterStage,
  tapStage,
  flattenStage,
  pluckStage,
  debounceStage,
} from './stages.js';

/**
 * Error strategy constants. Controls how stage failures are handled at runtime.
 *
 * - `ABORT` (default): stop the pipeline on first error, throw it.
 * - `SKIP`: silently drop the failing item, continue with the next one.
 * - `CONTINUE`: keep the failing item as-is, log the error, continue.
 *
 * @enum {string}
 */
export const ErrorStrategy = Object.freeze({
  ABORT: 'abort',
  SKIP: 'skip',
  CONTINUE: 'continue',
});

/**
 * Stage kinds. `PIPE` is a generic user-supplied transform; the rest map to
 * the built-in stage factories or list-level operators.
 *
 * @private
 * @enum {string}
 */
const Kind = Object.freeze({
  SOURCE: 'source',
  ON_ERROR: 'onError',
  PARALLEL: 'parallel',
  BATCH: 'batch',
  PIPE: 'pipe',
  MAP: 'map',
  FILTER: 'filter',
  TAP: 'tap',
  FLATTEN: 'flatten',
  PLUCK: 'pluck',
  DEBOUNCE: 'debounce',
});

/**
 * Wrap a stage function so undefined/null returns drop the item from the
 * output. The wrapper preserves whatever the underlying function returns.
 *
 * @private
 * @param {(item: any) => any|Promise<any>} fn
 */
function dropUndefined(fn) {
  return async (item) => {
    const v = await fn(item);
    return v; // null/undefined filtering happens in the loop
  };
}

/**
 * Normalize an inline stage call into a descriptor.
 *
 * @private
 * @param {string} kind
 * @param {*} arg
 */
function inline(kind, arg) {
  switch (kind) {
    case Kind.MAP: return { kind, fn: dropUndefined(mapStage(arg)), __arg: arg };
    case Kind.FILTER: return { kind, fn: dropUndefined(filterStage(arg)), __arg: arg };
    case Kind.TAP: return { kind, fn: tapStage(arg), __arg: arg };
    case Kind.FLATTEN: return { kind, fn: flattenStage(arg), __arg: arg };
    case Kind.PLUCK: return { kind, fn: dropUndefined(pluckStage(arg)), __arg: arg };
    case Kind.DEBOUNCE: return { kind, fn: debounceStage(arg), __arg: arg };
    case Kind.PIPE:
    default:
      return { kind: Kind.PIPE, fn: arg, __arg: arg };
  }
}

/**
 * Pipeline is a fluent builder for async data transformation chains.
 *
 * Stages are evaluated lazily when `.run()` is invoked. The pipeline
 * preserves ordering, supports sync and async stage functions, and offers
 * three error strategies: `abort` (default), `skip`, and `continue`.
 *
 * @example
 * const result = await Pipeline.of(data)
 *   .pipe(parseJson)
 *   .filter(valid => valid.status === 'active')
 *   .map(transform)
 *   .run();
 */
export class Pipeline {
  /**
   * Internal stage list. Each entry is a descriptor; index 0 is the
   * synthetic source.
   * @private
   */
  #stages;

  /**
   * Default error strategy.
   * @private
   */
  #errorStrategy;

  /**
   * Optional onError callback.
   * @private
   */
  #onError;

  /**
   * @private
   * @param {Iterable<any>|any} data
   * @param {string} [errorStrategy]
   * @param {Function|null} [onError]
   */
  constructor(data, errorStrategy = ErrorStrategy.ABORT, onError = null) {
    if (!Object.values(ErrorStrategy).includes(errorStrategy)) {
      throw new TypeError(
        `Invalid errorStrategy "${errorStrategy}". Expected one of: ${Object.values(ErrorStrategy).join(', ')}`
      );
    }
    this.#stages = [];
    this.#errorStrategy = errorStrategy;
    this.#onError = onError;

    this.#stages.push({ kind: Kind.SOURCE, data: Pipeline.#coerce(data) });
  }

  /**
   * Wrap any value (or iterable) into an array. Non-iterables are wrapped
   * as a single-item stream.
   *
   * @private
   */
  static #coerce(data) {
    if (data == null) return [];
    if (Array.isArray(data)) return data.slice();
    if (typeof data[Symbol.iterator] === 'function' || typeof data[Symbol.asyncIterator] === 'function') {
      return Array.from(data);
    }
    return [data];
  }

  /**
   * Create a new pipeline seeded with `data`.
   *
   * @template T
   * @param {Iterable<T>|T} data - Input items. Scalars are wrapped.
   * @returns {Pipeline}
   */
  static of(data) {
    return new Pipeline(data);
  }

  /**
   * Create a new pipeline with a default error strategy and optional
   * `onError` callback.
   *
   * @template T
   * @param {Iterable<T>|T} data
   * @param {string} errorStrategy - `abort` | `skip` | `continue`.
   * @param {Function} [onError] - `(error, stage, item) => void`.
   * @returns {Pipeline}
   */
  static with(data, errorStrategy, onError) {
    return new Pipeline(data, errorStrategy, onError);
  }

  /**
   * Append a generic transform stage.
   *
   * The stage function receives a single item and returns the transformed
   * value (or a Promise of one). Returning `undefined`/`null` drops the item.
   *
   * @param {(item: any) => any|Promise<any>} fn
   * @returns {Pipeline}
   */
  pipe(fn) {
    if (typeof fn !== 'function') {
      throw new TypeError('pipe requires a function');
    }
    this.#stages.push(inline(Kind.PIPE, fn));
    return this;
  }

  /**
   * 1-to-1 transform stage. Equivalent to `.pipe(fn)` but signals intent.
   *
   * @param {(item: any) => any|Promise<any>} fn
   * @returns {Pipeline}
   */
  map(fn) {
    if (typeof fn !== 'function') {
      throw new TypeError('map requires a function');
    }
    this.#stages.push(inline(Kind.MAP, fn));
    return this;
  }

  /**
   * Predicate stage. Items where `fn(item)` returns falsy are dropped.
   *
   * @param {(item: any) => boolean|Promise<boolean>} fn
   * @returns {Pipeline}
   */
  filter(fn) {
    if (typeof fn !== 'function') {
      throw new TypeError('filter requires a function');
    }
    this.#stages.push(inline(Kind.FILTER, fn));
    return this;
  }

  /**
   * Side-effect stage. Runs `fn(item)` for each item and passes the item
   * through unchanged.
   *
   * @param {(item: any) => void|Promise<void>} fn
   * @returns {Pipeline}
   */
  tap(fn) {
    if (typeof fn !== 'function') {
      throw new TypeError('tap requires a function');
    }
    this.#stages.push(inline(Kind.TAP, fn));
    return this;
  }

  /**
   * Group items into arrays of `size`. The final batch may be shorter.
   *
   * Implemented as a list-level stage: it consumes the current item list
   * and emits an array of arrays.
   *
   * @param {number} size - Items per batch.
   * @returns {Pipeline}
   */
  batch(size) {
    if (!Number.isInteger(size) || size <= 0) {
      throw new TypeError(`batch size must be a positive integer, got ${size}`);
    }
    this.#stages.push({ kind: Kind.BATCH, size });
    return this;
  }

  /**
   * Flatten one level of arrays across the current item list.
   *
   * Implemented as a list-level stage: it pulls arrays out of the item
   * list and concatenates their elements into the next stage. Items that
   * aren't arrays pass through unchanged.
   *
   * @param {number} [depth=1]
   * @returns {Pipeline}
   */
  flatten(depth = 1) {
    const d = Math.max(0, Math.floor(Number(depth) || 1));
    this.#stages.push({ kind: Kind.FLATTEN, __arg: d, fn: null });
    return this;
  }

  /**
   * Select a single field from each item. Equivalent to `map(item => item[key])`.
   *
   * @param {string|number|symbol} key
   * @returns {Pipeline}
   */
  pluck(key) {
    if (key === undefined || key === null) {
      throw new TypeError('pluck requires a non-null key');
    }
    this.#stages.push(inline(Kind.PLUCK, key));
    return this;
  }

  /**
   * Drop items that arrive within `wait` ms of the previous one.
   *
   * @param {number} wait - Milliseconds.
   * @returns {Pipeline}
   */
  debounce(wait) {
    if (!Number.isFinite(wait) || wait < 0) {
      throw new TypeError(`debounce wait must be a non-negative number, got ${wait}`);
    }
    this.#stages.push(inline(Kind.DEBOUNCE, wait));
    return this;
  }

  /**
   * Run the next `size` concurrent stage invocations in parallel.
   * Concurrency is scoped to the immediately following stage only.
   *
   * @param {number} size - Worker count.
   * @returns {Pipeline}
   */
  parallel(size = 4) {
    if (!Number.isInteger(size) || size <= 0) {
      throw new TypeError(`parallel size must be a positive integer, got ${size}`);
    }
    this.#stages.push({ kind: Kind.PARALLEL, size });
    return this;
  }

  /**
   * Override the error strategy for subsequent stages.
   *
   * @param {string} strategy - `abort` | `skip` | `continue`.
   * @param {Function} [onError]
   * @returns {Pipeline}
   */
  onError(strategy, onError) {
    if (!Object.values(ErrorStrategy).includes(strategy)) {
      throw new TypeError(
        `Invalid onError strategy "${strategy}". Expected one of: ${Object.values(ErrorStrategy).join(', ')}`
      );
    }
    this.#stages.push({ kind: Kind.ON_ERROR, strategy, onError: onError || null });
    return this;
  }

  /**
   * Execute the pipeline. Returns the final collected array of items.
   *
   * @returns {Promise<Array<any>>}
   */
  async run() {
    let items = this.#stages[0].data.slice();
    let strategy = this.#errorStrategy;
    let onErr = this.#onError;

    for (let i = 1; i < this.#stages.length; i++) {
      const stage = this.#stages[i];

      if (stage.kind === Kind.ON_ERROR) {
        strategy = stage.strategy;
        onErr = stage.onError;
        continue;
      }

      if (stage.kind === Kind.PARALLEL) {
        // Mark the next non-marker stage as parallel. We attach metadata
        // onto it so the run loop can pick the worker pool size up.
        const nextIdx = i + 1;
        if (nextIdx < this.#stages.length) {
          const next = this.#stages[nextIdx];
          if (next.kind !== Kind.ON_ERROR && next.kind !== Kind.PARALLEL && next.kind !== Kind.BATCH) {
            this.#stages[nextIdx] = { ...next, __parallel: stage.size };
          }
        }
        continue;
      }

      if (stage.kind === Kind.BATCH) {
        const size = stage.size;
        const batched = [];
        for (let j = 0; j < items.length; j += size) {
          batched.push(items.slice(j, j + size));
        }
        items = batched;
        continue;
      }

      if (stage.kind === Kind.FLATTEN) {
        // List-level flatten: pull one level of arrays out of the item
        // list. Items that aren't arrays are kept as-is.
        const depth = stage.__arg ?? 1;
        const flat = [];
        for (const item of items) {
          if (Array.isArray(item)) {
            // Use Array#flat with the requested depth.
            flat.push(...item.flat(depth));
          } else {
            flat.push(item);
          }
        }
        items = flat;
        continue;
      }

      // Per-item stage.
      const parallelSize = stage.__parallel;
      try {
        if (parallelSize && parallelSize > 1) {
          items = await this.#runParallel(items, stage, parallelSize, strategy, onErr);
        } else {
          items = await this.#runSerial(items, stage, strategy, onErr);
        }
      } catch (err) {
        if (strategy === ErrorStrategy.ABORT) throw err;
        // skip/continue already mutated items; nothing else to do.
      }
    }

    return items;
  }

  /**
   * Run a stage against items serially, applying the error strategy.
   *
   * @private
   */
  async #runSerial(items, stage, strategy, onErr) {
    const fn = stage.fn;
    const out = [];
    for (let idx = 0; idx < items.length; idx++) {
      const item = items[idx];
      try {
        const value = await fn(item);
        if (value === undefined || value === null) continue;
        out.push(value);
      } catch (err) {
        if (strategy === ErrorStrategy.SKIP) {
          this.#report(onErr, err, stage, item);
          continue;
        }
        if (strategy === ErrorStrategy.CONTINUE) {
          this.#report(onErr, err, stage, item);
          out.push(item);
          continue;
        }
        throw err;
      }
    }
    return out;
  }

  /**
   * Run a stage with bounded concurrency, preserving input order.
   *
   * @private
   */
  async #runParallel(items, stage, size, strategy, onErr) {
    const fn = stage.fn;
    const total = items.length;
    const results = new Array(total);
    const errors = new Array(total).fill(null);
    let cursor = 0;

    const workers = Array.from({ length: Math.min(size, total) }, async () => {
      while (cursor < total) {
        const idx = cursor++;
        try {
          const value = await fn(items[idx]);
          if (value !== undefined && value !== null) results[idx] = value;
        } catch (err) {
          errors[idx] = err;
        }
      }
    });

    await Promise.all(workers);

    const out = [];
    for (let i = 0; i < total; i++) {
      const err = errors[i];
      if (err) {
        if (strategy === ErrorStrategy.SKIP) {
          this.#report(onErr, err, stage, items[i]);
          continue;
        }
        if (strategy === ErrorStrategy.CONTINUE) {
          this.#report(onErr, err, stage, items[i]);
          out.push(items[i]);
          continue;
        }
        throw err;
      }
      if (results[i] !== undefined) out.push(results[i]);
    }
    return out;
  }

  /**
   * Safely invoke the user-supplied onError callback.
   *
   * @private
   */
  #report(onErr, err, stage, item) {
    if (typeof onErr !== 'function') return;
    try { onErr(err, stage, item); } catch (_) { /* swallow cb errors */ }
  }

  /**
   * Number of registered stages (excluding the synthetic source).
   *
   * @returns {number}
   */
  get size() {
    return Math.max(0, this.#stages.length - 1);
  }

  /**
   * Return a shallow copy of the stage descriptors. Useful for debugging
   * and tests.
   *
   * @returns {Array<object>}
   */
  describe() {
    return this.#stages.map((s) => ({ ...s }));
  }
}

export default Pipeline;