// Run with: node --test web/src/lib/pow/
// The vectors are the Go side's (internal/pow, TestHashVector), computed
// independently with Python's hashlib, so both solvers follow one rule.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { sha256Hex, solve } from './solve.ts';

const token = '1700000000000.8.AAAAAAAAAAAAAAAAAAAAAA.mac';

test('SHA-256 matches the standard', () => {
  assert.equal(sha256Hex(''), 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  assert.equal(sha256Hex('abc'), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
  // 56 to 64 bytes: the padding spills into a second block.
  assert.equal(
    sha256Hex('abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq'),
    '248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1',
  );
});

test('the work function matches the server', () => {
  assert.equal(sha256Hex(token + ':0').slice(0, 8), 'dce31f38');
  assert.deepEqual(solve(token, 8), { nonce: '458', hashes: 459 });
});

test('progress can stop the search', () => {
  let calls = 0;
  const result = solve(token, 40, () => {
    calls++;
    return false;
  });
  assert.equal(result, null);
  assert.equal(calls, 1);
});
