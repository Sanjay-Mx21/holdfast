// The page's side of proof of work: start a worker, wait for the nonce.
import type { SolveMessage } from './pow.worker.ts';

export interface Solving {
  /** Resolves with the nonce; rejects if the worker fails or is cancelled. */
  result: Promise<{ nonce: string; hashes: number; ms: number }>;
  /** Stops the worker (navigating away, or a fresh challenge). */
  cancel: () => void;
}

/**
 * Solves a challenge from GET /v1/queue/{eventId}/challenge in a Web
 * Worker. onProgress gets the hashes tried so far; expected work is
 * 2^difficulty hashes, which the waiting room can show as a progress hint.
 */
export function solveInWorker(
  challenge: string,
  difficulty: number,
  onProgress?: (hashes: number) => void,
): Solving {
  const worker = new Worker(new URL('./pow.worker.ts', import.meta.url), { type: 'module' });
  let reject: (reason: Error) => void = () => {};
  const result = new Promise<{ nonce: string; hashes: number; ms: number }>((resolve, rej) => {
    reject = rej;
    worker.onmessage = (event: MessageEvent<SolveMessage>) => {
      const msg = event.data;
      if (msg.type === 'progress') {
        onProgress?.(msg.hashes);
        return;
      }
      worker.terminate();
      resolve({ nonce: msg.nonce, hashes: msg.hashes, ms: msg.ms });
    };
    worker.onerror = (event) => {
      worker.terminate();
      rej(new Error(`proof-of-work worker failed: ${event.message}`));
    };
  });
  worker.postMessage({ challenge, difficulty });
  return {
    result,
    cancel: () => {
      worker.terminate();
      reject(new Error('proof of work cancelled'));
    },
  };
}
