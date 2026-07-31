# tiny-pipeline

> Async stream pipeline builder for AI agent data transformation workflows.

`tiny-pipeline` is a tiny, zero-dependency, pure-Node.js library for
chaining async data transformations with a fluent `.pipe()` API. It is
designed for AI agents that need to push records through multi-stage
workflows — `parse → validate → transform → enrich → output` — without
pulling in a heavy stream framework.

```
   Pipeline.of(data)
     │  source
     ▼
   ┌─────────┐
   │  pipe   │── fn(item) ──▶
   └─────────┘
     ▼
   ┌─────────┐
   │ filter  │── keep ──▶   ── drop ──▶ (✗)
   └─────────┘
     ▼
   ┌─────────┐
   │   map   │── fn(item) ──▶
   └─────────┘
     ▼
   ┌─────────┐
   │  batch  │──▶ [ [...], [...] ]
   └─────────┘
     ▼
   ┌─────────┐
   │ flatten │──▶ spread back into a flat list
   └─────────┘
     ▼
   result: Promise<Array>
```

---

## Install

```bash
npm install tiny-pipeline
```

Or just copy `pipeline.js` and `stages.js` — there are no dependencies.

Requires Node.js ≥ 16.

---

## Quick start

```javascript
import { Pipeline } from 'tiny-pipeline';

const result = await Pipeline.of(rawRecords)
  .pipe(parseJson)              // sync or async transform
  .filter((row) => row.active)   // drop inactive rows
  .map(enrich)                   // add computed fields
  .tap((row) => console.log('processed', row.id))
  .run();

console.log(result);
```

The example below demonstrates batching for memory-bounded bulk processing:

```javascript
const flat = await Pipeline.of(records)
  .batch(100)                   // group into arrays of 100
  .pipe(processBatch)           // each batch → processed batch
  .flatten()                    // spread batches back out
  .run();
```

---

## API

### `Pipeline.of(data)`

Create a new pipeline seeded with `data`. `data` may be an array, any
iterable (Set, Map values, generator), or a single scalar (which gets
wrapped into a one-item stream). `null` and `undefined` produce an empty
pipeline.

### `Pipeline.with(data, errorStrategy, onError?)`

Like `Pipeline.of`, but sets the default error strategy and an optional
`onError(err, stage, item)` callback for the whole pipeline.

### Chainable methods

Every chainable method returns `this`, so they can be chained freely.

| Method                       | Purpose                                                                                  |
|------------------------------|------------------------------------------------------------------------------------------|
| `.pipe(fn)`                  | Generic transform stage. `fn(item)` returns the next item (or `undefined`/`null` to drop).|
| `.map(fn)`                   | 1-to-1 transform. Same as `.pipe(fn)` but signals intent.                                |
| `.filter(fn)`                | Predicate stage. Falsy items are dropped.                                                |
| `.tap(fn)`                   | Side-effect stage. Runs `fn(item)` and passes the item through unchanged.                |
| `.batch(size)`               | Group items into arrays of `size`. List-level operation.                                 |
| `.flatten(depth = 1)`        | Concatenate array items across the item list. List-level operation.                      |
| `.pluck(key)`                | Extract a single field per item. Missing keys drop the item.                             |
| `.debounce(wait)`            | Drop items that arrive within `wait` ms of the previous one. Per-item async.             |
| `.parallel(size = 4)`        | Run the next stage with bounded concurrency (preserving order).                          |
| `.onError(strategy, onError?)` | Override the error strategy for subsequent stages.                                      |
| `.run()`                     | Execute the pipeline. Returns `Promise<Array<any>>`.                                    |

### Error strategies

`ErrorStrategy.ABORT` (default) — first error throws and the pipeline stops.

`ErrorStrategy.SKIP` — drop the failing item and continue.

`ErrorStrategy.CONTINUE` — keep the failing item unchanged and continue.

```javascript
import { Pipeline, ErrorStrategy } from 'tiny-pipeline';

const result = await Pipeline.with(rows, ErrorStrategy.SKIP, (err, stage, item) => {
  console.warn(`stage ${stage.kind} failed on item`, item, err);
})
  .pipe(maybeFragileTransform)
  .map(cheapTransform)
  .run();
```

---

## Built-in stage factories

The factories exported from `tiny-pipeline` produce plain `(item) => any`
functions. They are the same stages the Pipeline methods use under the
hood — useful when you want to compose stages manually or reuse them
inside your own helpers.

```javascript
import {
  mapStage,
  filterStage,
  tapStage,
  flattenStage,
  pluckStage,
  debounceStage,
  identityStage,
} from 'tiny-pipeline';
```

| Factory                   | Returns                                                    |
|---------------------------|------------------------------------------------------------|
| `mapStage(fn)`            | `(item) => Promise<fn(item)>`                              |
| `filterStage(predicate)`  | `(item) => Promise<item|undefined>` (kept/dropped)         |
| `tapStage(fn)`            | `(item) => Promise<item>` (runs side effect, passes through)|
| `flattenStage(depth=1)`   | `(item) => Array.flat(depth)` for arrays, passthrough otherwise |
| `pluckStage(key)`         | `(item) => item[key]` (or `undefined` when missing)        |
| `debounceStage(ms)`       | `(item) => Promise<item>` with per-item throttling         |
| `identityStage()`         | `(item) => Promise<item>`                                  |

---

## Real-world examples

### Parse → validate → transform → enrich

```javascript
import { Pipeline } from 'tiny-pipeline';

const cleaned = await Pipeline.of(rawApiPayloads)
  .pipe(JSON.parse)
  .filter((row) => row.status === 'active')
  .map((row) => ({ ...row, score: scoreRow(row) }))
  .tap((row) => metrics.increment('rows.processed'))
  .run();
```

### Concurrent enrichment

```javascript
const enriched = await Pipeline.of(userIds)
  .parallel(8)                       // up to 8 concurrent fetches
  .pipe((id) => fetchUser(id))       // async stage
  .pluck('email')
  .filter((email) => !email.endsWith('@example.com'))
  .run();
```

### Bounded batch processing

```javascript
const flat = await Pipeline.of(items)
  .batch(500)
  .pipe(async (batch) => {
    const result = await db.insertMany(batch);
    return result.rows;
  })
  .flatten()
  .run();
```

### Defensive pipelines

```javascript
const safe = await Pipeline.with(
  untrustedInput,
  ErrorStrategy.CONTINUE,
  (err) => log.warn({ err }, 'stage failed; keeping original item')
)
  .pipe(safeParse)
  .map(transform)
  .run();
```

---

## Design notes

- **Pure Node.js.** No dependencies. Works on any modern Node runtime.
- **Synchronous stages are fine.** Each stage can be sync or async — the
  engine awaits the return value transparently.
- **Drop semantics.** A stage that returns `undefined` or `null` removes
  the item from the stream. Use this for filtering without an extra
  `.filter()` call.
- **List-level vs per-item stages.** `.batch()` and `.flatten()` operate
  on the entire accumulated item list. Every other stage is per-item,
  and `.parallel(size)` wraps the *next* stage with bounded concurrency.
- **TypeScript-ready.** JSDoc types are included so editors can provide
  autocomplete and type checking without any build step.

---

## License

MIT