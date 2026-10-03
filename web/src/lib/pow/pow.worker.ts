/// <reference lib="webworker" />
// Solves a waiting-room challenge off the main thread, so the page keeps
// rendering and stays responsive while the CPU works. Messages:
//   in:  { challenge: string, difficulty: number }
//   out: { type: 'progress', hashes } every 65,536 hashes,
//        then { type: 'done', nonce, hashes, ms }
import type { SolveMessage, SolveRequest } from './messages.ts';
import { solve } from './solve.ts';

const scope = self as unknown as DedicatedWorkerGlobalScope;

scope.onmessage = (event: MessageEvent<SolveRequest>) => {
  const { challenge, difficulty } = event.data;
  const start = performance.now();
  const result = solve(challenge, difficulty, (hashes) => {
    scope.postMessage({ type: 'progress', hashes } satisfies SolveMessage);
  });
  if (result) {
    scope.postMessage({ type: 'done', ...result, ms: performance.now() - start } satisfies SolveMessage);
  }
};
