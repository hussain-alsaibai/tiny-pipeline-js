'use strict';

/**
 * tiny-pipeline — Async stream pipeline builder for AI agent data workflows.
 *
 * @example
 * import { Pipeline } from 'tiny-pipeline';
 *
 * const result = await Pipeline.of(data)
 *   .pipe(parseJson)
 *   .filter(x => x.active)
 *   .map(transform)
 *   .run();
 */

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

export {
  Pipeline,
  ErrorStrategy,
  mapStage,
  filterStage,
  tapStage,
  flattenStage,
  pluckStage,
  debounceStage,
  identityStage,
};

export default Pipeline;