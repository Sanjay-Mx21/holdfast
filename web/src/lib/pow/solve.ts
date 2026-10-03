// Proof-of-work solver for the waiting room (internal/pow on the server).
//
// Find a nonce, a decimal string, such that SHA-256(challenge + ":" + nonce)
// starts with at least `difficulty` zero bits. Expected work: 2^difficulty
// hashes. SHA-256 is implemented here, synchronously: crypto.subtle.digest
// is asynchronous, and a promise per hash is several times slower for
// millions of tiny inputs. A challenge is longer than one 64-byte block, and
// its first block never changes, so that block is compressed once (the
// midstate) and each nonce costs one compression instead of two. Run it in a
// Web Worker (pow.worker.ts) so the page stays responsive.

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const IV = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);
const W = new Uint32Array(64);
const H = new Uint32Array(8);

// finish hashes msg[0:len], starting from state, which is the state after
// the first `from` bytes (a multiple of 64; 0 with IV). msg must have room
// for the padding: len rounded up to a multiple of 64, plus 64. The digest is
// left in H.
function finish(msg: Uint8Array, len: number, from: number, state: Uint32Array): void {
  H.set(state);
  const total = (((len + 8) >> 6) + 1) << 6;
  msg[len] = 0x80;
  msg.fill(0, len + 1, total - 4);
  const bitLen = len * 8;
  msg[total - 4] = bitLen >>> 24;
  msg[total - 3] = (bitLen >>> 16) & 0xff;
  msg[total - 2] = (bitLen >>> 8) & 0xff;
  msg[total - 1] = bitLen & 0xff;
  for (let off = from; off < total; off += 64) compress(msg, off);
}

// compress folds the 64-byte block at msg[off] into H.
function compress(msg: Uint8Array, off: number): void {
  for (let i = 0; i < 16; i++) {
    const j = off + i * 4;
    W[i] = (msg[j] << 24) | (msg[j + 1] << 16) | (msg[j + 2] << 8) | msg[j + 3];
  }
  for (let i = 16; i < 64; i++) {
    const a = W[i - 15], b = W[i - 2];
    const s0 = ((a >>> 7) | (a << 25)) ^ ((a >>> 18) | (a << 14)) ^ (a >>> 3);
    const s1 = ((b >>> 17) | (b << 15)) ^ ((b >>> 19) | (b << 13)) ^ (b >>> 10);
    W[i] = (W[i - 16] + s0 + W[i - 7] + s1) | 0;
  }
  let a = H[0], b = H[1], c = H[2], d = H[3], e = H[4], f = H[5], g = H[6], h = H[7];
  for (let i = 0; i < 64; i++) {
    const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
    const t1 = (h + S1 + ((e & f) ^ (~e & g)) + K[i] + W[i]) | 0;
    const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
    const t2 = (S0 + ((a & b) ^ (a & c) ^ (b & c))) | 0;
    h = g; g = f; f = e; e = (d + t1) | 0; d = c; c = b; b = a; a = (t1 + t2) | 0;
  }
  H[0] = (H[0] + a) | 0; H[1] = (H[1] + b) | 0; H[2] = (H[2] + c) | 0; H[3] = (H[3] + d) | 0;
  H[4] = (H[4] + e) | 0; H[5] = (H[5] + f) | 0; H[6] = (H[6] + g) | 0; H[7] = (H[7] + h) | 0;
}

function newBuffer(len: number): Uint8Array {
  return new Uint8Array((((len + 8) >> 6) + 2) << 6);
}

/** SHA-256 of a UTF-8 string, as lower-case hex (for tests and tools). */
export function sha256Hex(s: string): string {
  const bytes = new TextEncoder().encode(s);
  const buf = newBuffer(bytes.length);
  buf.set(bytes);
  finish(buf, bytes.length, 0, IV);
  return Array.from(H, (w) => (w >>> 0).toString(16).padStart(8, '0')).join('');
}

/** Leading zero bits of the digest left in H. */
function leadingZeroBits(): number {
  let n = 0;
  for (let i = 0; i < 8; i++) {
    if (H[i] !== 0) return n + Math.clz32(H[i]);
    n += 32;
  }
  return n;
}

export interface SolveResult {
  nonce: string;
  hashes: number;
}

/**
 * Finds the first nonce from 0 up that solves the challenge. onProgress, if
 * given, is called every 2^16 hashes with the count so far; returning false
 * from it stops the search (the result is then null).
 */
export function solve(
  challenge: string,
  difficulty: number,
  onProgress?: (hashes: number) => boolean | void,
): SolveResult | null {
  const prefix = new TextEncoder().encode(challenge + ':');
  const buf = newBuffer(prefix.length + 20); // a nonce has at most 20 digits
  buf.set(prefix);
  // The midstate: the state after every whole block of the prefix.
  const from = prefix.length & ~63;
  H.set(IV);
  for (let off = 0; off < from; off += 64) compress(buf, off);
  const mid = H.slice();
  const digits = new Uint8Array(20);
  for (let n = 0; n < Number.MAX_SAFE_INTEGER; n++) {
    // Write n's decimal digits after the prefix.
    let len = 0;
    let v = n;
    do {
      digits[len++] = 48 + (v % 10);
      v = Math.floor(v / 10);
    } while (v > 0);
    for (let i = 0; i < len; i++) buf[prefix.length + i] = digits[len - 1 - i];
    finish(buf, prefix.length + len, from, mid);
    if (leadingZeroBits() >= difficulty) return { nonce: String(n), hashes: n + 1 };
    if (onProgress && (n & 0xffff) === 0xffff && onProgress(n + 1) === false) return null;
  }
  return null;
}
