// Messages between the page and the proof-of-work worker.

export interface SolveRequest {
  challenge: string;
  difficulty: number;
}

export type SolveMessage =
  | { type: "progress"; hashes: number }
  | { type: "done"; nonce: string; hashes: number; ms: number };
