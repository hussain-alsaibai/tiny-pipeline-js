'use strict';

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';

import { Pipeline, ErrorStrategy } from './pipeline.js';
import {
  mapStage,
  filterStage,
  tapStage,
  flattenStage,
  pluckStage,
  debounceStage,
  identityStage,
} from './stages.js';

// ---------------------------------------------------------------------------
// Basic chain & fluent API
// ---------------------------------------------------------------------------

describe('Pipeline basics', () => {
  test('Pipeline.of creates a pipeline from an array', () => {
    const p = Pipeline.of([1, 2, 3]);
    assert.ok(p instanceof Pipeline);
    assert.equal(p.size, 0);
  });

  test('Pipeline.of wraps a scalar into a single-item stream', async () => {
    const out = await Pipeline.of(42).map((n) => n * 2).run();
    assert.deepEqual(out, [84]);
  });

  test('Pipeline.of accepts an iterable (Set)', async () => {
    const out = await Pipeline.of(new Set([3, 1, 2])).map((n) => n).run();
    assert.deepEqual(out, [3, 1, 2]);
  });

  test('Pipeline.of with null/undefined yields empty', async () => {
    const a = await Pipeline.of(null).map((x) => x).run();
    const b = await Pipeline.of(undefined).map((x) => x).run();
    assert.deepEqual(a, []);
    assert.deepEqual(b, []);
  });

  test('returns this from every chainable method', () => {
    const p = Pipeline.of([1]);
    assert.equal(p.pipe((x) => x), p);
    assert.equal(p.map((x) => x), p);
    assert.equal(p.filter(() => true), p);
    assert.equal(p.tap(() => {}), p);
    assert.equal(p.batch(1), p);
    assert.equal(p.flatten(), p);
    assert.equal(p.pluck('a'), p);
    assert.equal(p.parallel(2), p);
    assert.equal(p.onError(ErrorStrategy.SKIP), p);
  });

  test('describe returns stage descriptors', () => {
    const desc = Pipeline.of([1, 2])
      .map((x) => x * 2)
      .filter((x) => x > 0)
      .describe();
    assert.equal(desc.length, 3);
    assert.equal(desc[0].kind, 'source');
    assert.equal(desc[1].kind, 'map');
    assert.equal(desc[2].kind, 'filter');
  });
});

// ---------------------------------------------------------------------------
// Sync / async stages
// ---------------------------------------------------------------------------

describe('Stage execution', () => {
  test('runs a simple synchronous map', async () => {
    const out = await Pipeline.of([1, 2, 3]).map((n) => n + 1).run();
    assert.deepEqual(out, [2, 3, 4]);
  });

  test('runs an async stage', async () => {
    const out = await Pipeline.of([1, 2, 3])
      .map(async (n) => {
        await new Promise((r) => setTimeout(r, 1));
        return n * 10;
      })
      .run();
    assert.deepEqual(out, [10, 20, 30]);
  });

  test('mixes sync and async stages in order', async () => {
    const calls = [];
    const out = await Pipeline.of([1, 2, 3])
      .tap((n) => calls.push(`t1:${n}`))
      .map(async (n) => {
        await new Promise((r) => setTimeout(r, 1));
        return n * 2;
      })
      .tap((n) => calls.push(`t2:${n}`))
      .run();
    assert.deepEqual(out, [2, 4, 6]);
    assert.deepEqual(calls, ['t1:1', 't1:2', 't1:3', 't2:2', 't2:4', 't2:6']);
  });

  test('dropping undefined/null removes the item', async () => {
    const out = await Pipeline.of([1, 2, 3, 4])
      .map((n) => (n % 2 === 0 ? n : undefined))
      .run();
    assert.deepEqual(out, [2, 4]);
  });

  test('filter keeps truthy items', async () => {
    const out = await Pipeline.of([1, 2, 3, 4])
      .filter((n) => n % 2 === 0)
      .run();
    assert.deepEqual(out, [2, 4]);
  });

  test('tap does not transform items', async () => {
    let seen = [];
    const out = await Pipeline.of(['a', 'b', 'c'])
      .tap((s) => seen.push(s))
      .run();
    assert.deepEqual(out, ['a', 'b', 'c']);
    assert.deepEqual(seen, ['a', 'b', 'c']);
  });
});

// ---------------------------------------------------------------------------
// Batch, flatten, pluck
// ---------------------------------------------------------------------------

describe('List-level stages', () => {
  test('batch splits items into chunks of N', async () => {
    const out = await Pipeline.of([1, 2, 3, 4, 5, 6, 7]).batch(3).run();
    assert.deepEqual(out, [[1, 2, 3], [4, 5, 6], [7]]);
  });

  test('batch with size >= items produces a single batch', async () => {
    const out = await Pipeline.of([1, 2, 3]).batch(10).run();
    assert.deepEqual(out, [[1, 2, 3]]);
  });

  test('batch rejects invalid size', () => {
    assert.throws(() => Pipeline.of([]).batch(0), /positive integer/);
    assert.throws(() => Pipeline.of([]).batch(-1), /positive integer/);
    assert.throws(() => Pipeline.of([]).batch(1.5), /positive integer/);
  });

  test('flatten spreads nested arrays one level by default', async () => {
    const out = await Pipeline.of([[1, 2], [3, 4], [5]])
      .flatten()
      .run();
    assert.deepEqual(out, [1, 2, 3, 4, 5]);
  });

  test('flatten with depth flattens deeply', async () => {
    const out = await Pipeline.of([[1, [2, [3, 4]]]]).flatten(3).run();
    assert.deepEqual(out, [1, 2, 3, 4]);
  });

  test('pluck extracts a single field', async () => {
    const out = await Pipeline.of([
      { id: 1, name: 'a' },
      { id: 2, name: 'b' },
    ])
      .pluck('name')
      .run();
    assert.deepEqual(out, ['a', 'b']);
  });

  test('pluck on missing key drops the item', async () => {
    const out = await Pipeline.of([{ a: 1 }, { b: 2 }])
      .pluck('a')
      .run();
    assert.deepEqual(out, [1]);
  });

  test('batch → flatten round trip', async () => {
    const items = Array.from({ length: 250 }, (_, i) => i);
    const out = await Pipeline.of(items).batch(100).flatten().run();
    assert.equal(out.length, 250);
    assert.equal(out[0], 0);
    assert.equal(out[249], 249);
  });
});

// ---------------------------------------------------------------------------
// Parallel execution
// ---------------------------------------------------------------------------

describe('Parallel execution', () => {
  test('parallel preserves input order', async () => {
    const delay = (n) =>
      new Promise((r) => setTimeout(() => r(n * 2), n * 5));
    const out = await Pipeline.of([4, 1, 3, 2, 0])
      .parallel(2)
      .pipe(delay)
      .run();
    assert.deepEqual(out, [8, 2, 6, 4, 0]);
  });

  test('parallel actually runs concurrently', async () => {
    let inflight = 0;
    let peak = 0;
    const fn = async (n) => {
      inflight++;
      peak = Math.max(peak, inflight);
      await new Promise((r) => setTimeout(r, 20));
      inflight--;
      return n;
    };
    await Pipeline.of([1, 2, 3, 4, 5, 6, 7, 8]).parallel(4).pipe(fn).run();
    assert.ok(peak <= 4, `peak concurrency ${peak} should be <= 4`);
    assert.ok(peak >= 2, `peak concurrency ${peak} should be >= 2`);
  });

  test('parallel defaults to 4 workers when no arg given', async () => {
    let peak = 0;
    let inflight = 0;
    const fn = async (n) => {
      inflight++;
      peak = Math.max(peak, inflight);
      await new Promise((r) => setTimeout(r, 10));
      inflight--;
      return n;
    };
    await Pipeline.of([1, 2, 3, 4, 5, 6, 7, 8, 9]).parallel().pipe(fn).run();
    assert.ok(peak <= 4, `peak concurrency ${peak} should be <= 4`);
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('Error handling', () => {
  test('abort (default) throws on stage failure', async () => {
    await assert.rejects(
      Pipeline.of([1, 2, 3])
        .map((n) => {
          if (n === 2) throw new Error('boom');
          return n;
        })
        .run(),
      /boom/
    );
  });

  test('skip drops failing items', async () => {
    const out = await Pipeline.with([1, 2, 3, 4], ErrorStrategy.SKIP)
      .map((n) => {
        if (n === 2 || n === 4) throw new Error('skip me');
        return n;
      })
      .run();
    assert.deepEqual(out, [1, 3]);
  });

  test('continue keeps the original item on failure', async () => {
    const out = await Pipeline.with([1, 2, 3, 4], ErrorStrategy.CONTINUE)
      .map((n) => {
        if (n === 2 || n === 4) throw new Error('keep me');
        return n;
      })
      .run();
    assert.deepEqual(out, [1, 2, 3, 4]);
  });

  test('onError callback receives (err, stage, item)', async () => {
    const events = [];
    await Pipeline.with(
      [1, 2, 3],
      ErrorStrategy.SKIP,
      (err, stage, item) => events.push({ msg: err.message, item })
    )
      .map((n) => {
        if (n === 2) throw new Error(`bad:${n}`);
        return n;
      })
      .run();
    assert.deepEqual(events, [{ msg: 'bad:2', item: 2 }]);
  });

  test('onError callback throwing does not crash the pipeline', async () => {
    const out = await Pipeline.with(
      [1, 2, 3],
      ErrorStrategy.SKIP,
      () => { throw new Error('cb exploded'); }
    )
      .map((n) => {
        if (n === 2) throw new Error('boom');
        return n;
      })
      .run();
    assert.deepEqual(out, [1, 3]);
  });

  test('per-stage onError override applies to subsequent stages', async () => {
    const events = [];
    const out = await Pipeline.of([1, 2, 3, 4])
      .onError(ErrorStrategy.SKIP, (err) => events.push(err.message))
      .map((n) => {
        if (n === 2) throw new Error('boom');
        return n;
      })
      .map((n) => n * 10)
      .run();
    assert.deepEqual(out, [10, 30, 40]);
    assert.deepEqual(events, ['boom']);
  });

  test('rejects invalid error strategy at construction', () => {
    assert.throws(
      () => Pipeline.with([], 'whatever'),
      /Invalid errorStrategy/
    );
  });
});

// ---------------------------------------------------------------------------
// Edge cases & empty input
// ---------------------------------------------------------------------------

describe('Edge cases', () => {
  test('empty input runs through every stage', async () => {
    const out = await Pipeline.of([])
      .map((n) => n + 1)
      .filter(() => true)
      .tap(() => {})
      .run();
    assert.deepEqual(out, []);
  });

  test('batch on empty list yields empty list of batches', async () => {
    const out = await Pipeline.of([]).batch(10).run();
    assert.deepEqual(out, []);
  });

  test('chained stages produce a fluent pipeline (returns Pipeline)', async () => {
    const p = Pipeline.of([
      { id: 1, n: 2 },
      { id: 2, n: 3 },
      { id: 3, n: 4 },
    ])
      .pluck('n')
      .filter((n) => n > 1)
      .tap(() => {})
      .batch(2)
      .flatten();
    assert.ok(p instanceof Pipeline);
    const out = await p.run();
    assert.deepEqual(out, [2, 3, 4]);
  });

  test('pipeline is reusable? — run() consumes and returns; subsequent run still works', async () => {
    const p = Pipeline.of([1, 2, 3]).map((n) => n * 2);
    const a = await p.run();
    const b = await p.run();
    assert.deepEqual(a, [2, 4, 6]);
    assert.deepEqual(b, [2, 4, 6]);
  });

  test('pluck rejects null/undefined key', () => {
    assert.throws(() => Pipeline.of([]).pluck(null), /non-null key/);
    assert.throws(() => Pipeline.of([]).pluck(undefined), /non-null key/);
  });

  test('pipe/map/filter/tap reject non-function input', () => {
    assert.throws(() => Pipeline.of([]).pipe(123), /requires a function/);
    assert.throws(() => Pipeline.of([]).map('not-a-fn'), /requires a function/);
    assert.throws(() => Pipeline.of([]).filter(null), /requires a function/);
    assert.throws(() => Pipeline.of([]).tap(42), /requires a function/);
  });
});

// ---------------------------------------------------------------------------
// Built-in stage functions (exposed via index.js)
// ---------------------------------------------------------------------------

describe('Stage factories', () => {
  test('mapStage wraps a transform', async () => {
    const fn = mapStage((x) => x * 3);
    assert.equal(await fn(2), 6);
  });

  test('filterStage returns the item when predicate truthy', async () => {
    const fn = filterStage((x) => x > 0);
    assert.equal(await fn(5), 5);
    assert.equal(await fn(-1), undefined);
  });

  test('tapStage runs side effect and returns the item', async () => {
    let seen;
    const fn = tapStage((x) => { seen = x; });
    const out = await fn('hi');
    assert.equal(out, 'hi');
    assert.equal(seen, 'hi');
  });

  test('flattenStage flattens by 1 by default', () => {
    const fn = flattenStage();
    assert.deepEqual(fn([[1, 2], [3]]), [1, 2, 3]);
  });

  test('pluckStage returns the field', () => {
    const fn = pluckStage('a');
    assert.equal(fn({ a: 7 }), 7);
    assert.equal(fn({ b: 7 }), undefined);
  });

  test('debounceStage delays items within window', async () => {
    const fn = debounceStage(30);
    const start = Date.now();
    await fn('first');
    const firstElapsed = Date.now() - start;
    await fn('second'); // should be delayed
    const secondElapsed = Date.now() - start;
    assert.ok(secondElapsed >= firstElapsed + 10);
  });

  test('identityStage passes through', async () => {
    const fn = identityStage();
    const result = await fn({ a: 1 });
    assert.deepEqual(result, { a: 1 });
  });

  test('stage factories throw on bad args', () => {
    assert.throws(() => flattenStage(-1), TypeError);
    assert.throws(() => pluckStage(null), /non-null key/);
    assert.throws(() => debounceStage(-5), /non-negative/);
  });
});