import test from 'node:test';
import assert from 'node:assert/strict';
import { uploadStream, downloadTo, streamCapabilities, TransferError, objectUrl } from '../dist/index.js';
import { checkedAdd } from '../dist/internal.js';

const url = 'https://example.test/objects/key';
const chunk = (...values) => new Uint8Array(values);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
function source(chunks, hooks = {}) {
  let index = 0;
  return new ReadableStream({
    pull(controller) { hooks.pull?.(); if (index < chunks.length) controller.enqueue(chunks[index++]); else controller.close(); },
    cancel(reason) { hooks.cancel?.(reason); },
  }, { highWaterMark: 0 });
}
async function readAll(body) {
  const result = [];
  const reader = body.getReader();
  try { while (true) { const value = await reader.read(); if (value.done) return result; result.push(...value.value); } }
  finally { reader.releaseLock(); }
}
function commitFetch(extra = {}) {
  return async (_url, init) => {
    const bytes = await readAll(init.body);
    return Response.json({ committed: true, key: 'key', bytes: bytes.length, etag: 'opaque-etag', ...extra }, { status: 201 });
  };
}
function response(chunks, headers = {}, status = 200, hooks = {}) {
  const length = chunks.reduce((total, value) => total + value.length, 0);
  return new Response(source(chunks, hooks), { status, headers: {
    'Content-Length': String(length), ETag: 'opaque-etag', 'Accept-Ranges': 'bytes',
    'Content-Type': 'application/octet-stream', ...headers,
  } });
}
function sink(hooks = {}) {
  const bytes = [];
  let closed = false;
  let aborted;
  return { bytes, get closed() { return closed; }, get aborted() { return aborted; },
    stream: new WritableStream({
      async write(value) { await hooks.write?.(value); bytes.push(...value); },
      close() { closed = true; return hooks.close?.(); },
      abort(reason) { aborted = reason; return hooks.abort?.(reason); },
    }, { highWaterMark: 1 }) };
}
const isCode = code => error => error instanceof TransferError && error.code === code;

test('capability probe explicitly does not verify the transport', () => {
  assert.equal(streamCapabilities().requestStreamSyntax, true);
  assert.equal(streamCapabilities().transportVerified, false);
});

test('raw upload validates server commit after EOF and separates source progress', async () => {
  const events = [];
  let headers;
  const result = await uploadStream({ url, key: 'key', maxBytes: 10, knownLength: 3,
    source: source([chunk(1, 2), chunk(3)]), onProgress: value => events.push(value),
    fetch: async (input, init) => {
      headers = init.headers;
      assert.equal(init.method, 'PUT'); assert.equal(init.duplex, 'half'); assert.equal(init.redirect, 'error');
      assert.equal(init.headers.get('content-length'), null);
      const body = await readAll(init.body);
      assert.deepEqual(events.map(event => event.phase), ['reading', 'reading']);
      return Response.json({ committed: true, key: 'key', bytes: body.length, etag: 'etag' }, { status: 201 });
    } });
  assert.equal(headers.get('x-upload-length'), '3');
  assert.equal(result.bytes, 3);
  assert.deepEqual(events.map(event => event.phase), ['reading', 'reading', 'committed']);
  assert.equal(Object.isFrozen(result), true);
});

test('empty and unknown-length uploads are supported without a Blob fallback', async () => {
  for (const input of [[], [chunk(1), chunk(2, 3)]]) {
    const result = await uploadStream({ url, key: 'key', maxBytes: 3, source: source(input),
      fetch: async (input, init) => { assert.equal(init.headers.get('x-upload-length'), null); return commitFetch()(input, init); } });
    assert.equal(result.bytes, input.flatMap(value => [...value]).length);
  }
});

test('known size above limit rejects before source ownership or fetch', async () => {
  let pulls = 0;
  const input = source([chunk(1)], { pull() { pulls++; } });
  await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 1, knownLength: 2, source: input,
    fetch: () => { throw new Error('must not fetch'); } }), isCode('LIMIT_EXCEEDED'));
  assert.equal(pulls, 0); assert.equal(input.locked, false);
});

test('source oversize and length mismatches cancel source and reject commit', async () => {
  for (const [knownLength, limit, chunks, code] of [
    [undefined, 2, [chunk(1), chunk(2, 3)], 'LIMIT_EXCEEDED'],
    [2, 10, [chunk(1)], 'LENGTH_MISMATCH'],
    [1, 10, [chunk(1, 2)], 'LENGTH_MISMATCH'],
  ]) {
    let cancelled = false;
    const input = source(chunks, { cancel() { cancelled = true; } });
    await assert.rejects(uploadStream({ url, key: 'key', maxBytes: limit, knownLength,
      source: input, fetch: commitFetch() }), isCode(code));
    assert.equal(input.locked, false);
    // A source that already reached EOF cannot be cancelled again through its underlying source.
    if (chunks.flatMap(value => [...value]).length > (knownLength ?? limit)) assert.equal(cancelled, true);
  }
});

test('invalid chunks and source errors abort the transport', async () => {
  for (const input of [source(['bad']), new ReadableStream({ pull() { throw new Error('broken'); } })]) {
    let signal;
    await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 100, source: input,
      fetch: async (input, init) => { signal = init.signal; return commitFetch()(input, init); } }), isCode('SOURCE_ERROR'));
    assert.equal(signal.aborted, true);
  }
});

test('abort cancels a pending source read and rejects promptly', async () => {
  const abort = new AbortController();
  let cancelled = false;
  let signal;
  const input = new ReadableStream({ cancel() { cancelled = true; } }, { highWaterMark: 0 });
  const operation = uploadStream({ url, key: 'key', maxBytes: 100, source: input, signal: abort.signal,
    fetch: async (_url, init) => { signal = init.signal; await readAll(init.body); return new Promise(() => {}); } });
  abort.abort('stop');
  await assert.rejects(operation, error => isCode('ABORTED')(error) && error.uploadOutcome === 'unknown');
  assert.equal(cancelled, true); assert.equal(signal.aborted, true); assert.equal(input.locked, false);
});

test('a pre-aborted upload does not invoke fetch and cancels its source', async () => {
  let cancelled = false;
  const input = source([chunk(1)], { cancel() { cancelled = true; } });
  await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 100, source: input,
    signal: AbortSignal.abort('before'), fetch: () => assert.fail('fetch must not run') }),
    error => isCode('ABORTED')(error) && error.uploadOutcome === 'not-started');
  assert.equal(cancelled, true);
});

test('commit status, exact key, byte count, JSON and etag are validated', async () => {
  for (const value of [{ key: 'other' }, { bytes: 2 }, { committed: false }, { etag: '' }, { bytes: 2 ** 53 }]) {
    await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 10, source: source([chunk(1)]), fetch: commitFetch(value) }),
      error => isCode('PROTOCOL_ERROR')(error) && error.uploadOutcome === 'unknown');
  }
  await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 10, source: source([chunk(1)]),
    fetch: async (_url, init) => { await readAll(init.body); return new Response('x'.repeat(20_000), { status: 201, headers: { 'Content-Type': 'application/json' } }); } }), isCode('PROTOCOL_ERROR'));
});

test('early fake success does not mean a completed upload', async () => {
  let cancelled = false;
  await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 10,
    source: source([chunk(1)], { cancel() { cancelled = true; } }),
    fetch: async () => Response.json({ committed: true, key: 'key', bytes: 0, etag: 'fake' }, { status: 201 }) }), isCode('PROTOCOL_ERROR'));
  assert.equal(cancelled, true);
});

test('network failure is an unknown outcome; structured server rejection is distinct', async () => {
  await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 10, source: source([chunk(1)]),
    fetch: async () => { throw new TypeError('offline'); } }),
    error => isCode('NETWORK_ERROR')(error) && error.uploadOutcome === 'unknown');
  await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 10, source: source([chunk(1)]),
    fetch: async () => Response.json({ committed: false, code: 'too-large', message: 'rejected' }, { status: 413 }) }),
    error => isCode('HTTP_ERROR')(error) && error.uploadOutcome === 'rejected' && error.status === 413);
});

test('progress observer exceptions cannot turn confirmed commit into failure', async () => {
  const result = await uploadStream({ url, key: 'key', maxBytes: 1, source: source([chunk(1)]), fetch: commitFetch(),
    onProgress() { throw new Error('UI failed'); } });
  assert.equal(result.committed, true);
});

test('download waits for slow sink writes and closes only after all bytes', async () => {
  let pulls = 0;
  let writes = 0;
  let active = 0;
  let maxActive = 0;
  const destination = sink({ async write() { active++; maxActive = Math.max(maxActive, active); await sleep(5); writes++; active--; } });
  const progress = [];
  const result = await downloadTo({ url, maxBytes: 3, destination: destination.stream,
    onProgress(value) { progress.push(value.bytesWritten); assert.equal(value.bytesWritten, writes); },
    fetch: async () => response([chunk(1), chunk(2), chunk(3)], {}, 200, { pull() { pulls++; assert.ok(pulls <= writes + 1); } }) });
  assert.deepEqual(destination.bytes, [1, 2, 3]); assert.equal(destination.closed, true);
  assert.equal(maxActive, 1); assert.equal(result.bytesWritten, 3); assert.deepEqual(progress, [1, 2, 3]);
});

test('single closed, open and suffix ranges require exact Content-Range semantics', async () => {
  for (const range of [{ start: 2, end: 4 }, { start: 2 }, { suffix: 3 }]) {
    const destination = sink();
    const result = await downloadTo({ url, maxBytes: 3, range, destination: destination.stream,
      fetch: async (_url, init) => {
        assert.ok(init.headers.get('range').startsWith('bytes='));
        return response([chunk(2, 3, 4)], { 'Content-Range': 'bytes 2-4/5' }, 206);
      } });
    assert.deepEqual(result.range, { start: 2, end: 4, total: 5 });
  }
});

test('invalid download headers/status are rejected before writing', async () => {
  const bad = [
    [200, {}, { start: 0 }],
    [206, { 'Content-Range': 'bytes 0-0/2' }, undefined],
    [206, { 'Content-Range': 'bytes 1-1/2' }, { start: 0, end: 0 }],
    [200, { 'Content-Length': '9007199254740992' }, undefined],
    [200, { 'Content-Encoding': 'gzip' }, undefined],
    [200, { ETag: '' }, undefined],
    [200, { 'Accept-Ranges': 'none' }, undefined],
    [200, { 'Content-Length': '' }, undefined],
  ];
  for (const [status, headers, range] of bad) {
    const destination = sink();
    await assert.rejects(downloadTo({ url, maxBytes: 100, range, destination: destination.stream,
      fetch: async () => response([chunk(1)], headers, status) }), isCode('PROTOCOL_ERROR'));
    assert.deepEqual(destination.bytes, []); assert.ok(destination.aborted); assert.equal(destination.stream.locked, false);
  }
});

test('truncated and oversized responses abort destination without a success result', async () => {
  for (const length of ['1', '3']) {
    const destination = sink();
    await assert.rejects(downloadTo({ url, maxBytes: 10, destination: destination.stream,
      fetch: async () => response([chunk(1, 2)], { 'Content-Length': length }) }), isCode('LENGTH_MISMATCH'));
    assert.equal(destination.closed, false); assert.ok(destination.aborted);
  }
});

test('download cap is enforced before writing and a sink error cancels network source', async () => {
  const destination = sink();
  await assert.rejects(downloadTo({ url, maxBytes: 1, destination: destination.stream,
    fetch: async () => response([chunk(1, 2)]) }), isCode('LIMIT_EXCEEDED'));
  assert.deepEqual(destination.bytes, []);
  let cancelled = false;
  const broken = sink({ write() { throw new Error('disk full'); } });
  await assert.rejects(downloadTo({ url, maxBytes: 10, destination: broken.stream,
    fetch: async () => response([chunk(1), chunk(2)], {}, 200, { cancel() { cancelled = true; } }) }), isCode('SINK_ERROR'));
  assert.equal(cancelled, true); assert.equal(broken.closed, false);
});

test('abort during an uncooperative sink write returns promptly and cancels network', async () => {
  const abort = new AbortController();
  let cancel = false;
  let began;
  const ready = new Promise(resolve => { began = resolve; });
  let finish;
  const destination = sink({ write() { began(); return new Promise(resolve => { finish = resolve; }); } });
  const operation = downloadTo({ url, maxBytes: 10, signal: abort.signal, destination: destination.stream,
    fetch: async () => response([chunk(1), chunk(2)], {}, 200, { cancel() { cancel = true; } }) });
  await ready;
  abort.abort('stop');
  await assert.rejects(operation, isCode('ABORTED'));
  assert.equal(cancel, true); assert.equal(destination.stream.locked, false);
  finish();
  await sleep(0);
  assert.ok(destination.aborted);
});

test('zero-byte download closes sink and HTTP errors never write their error payload', async () => {
  const empty = sink();
  assert.equal((await downloadTo({ url, maxBytes: 0, destination: empty.stream,
    fetch: async () => response([]) })).bytesWritten, 0);
  assert.equal(empty.closed, true);
  const rejected = sink();
  await assert.rejects(downloadTo({ url, maxBytes: 10, destination: rejected.stream,
    fetch: async () => new Response('error', { status: 416 }) }),
    error => isCode('HTTP_ERROR')(error) && error.status === 416);
  assert.deepEqual(rejected.bytes, []);
});

test('large counters cross 2 GiB without signed truncation and reject unsafe totals', () => {
  assert.equal(checkedAdd(2 ** 31 - 1, 2, Number.MAX_SAFE_INTEGER), 2 ** 31 + 1);
  assert.equal(checkedAdd(Number.MAX_SAFE_INTEGER - 1, 1, Number.MAX_SAFE_INTEGER), Number.MAX_SAFE_INTEGER);
  assert.throws(() => checkedAdd(Number.MAX_SAFE_INTEGER, 1, Number.MAX_SAFE_INTEGER), isCode('LIMIT_EXCEEDED'));
});

test('upload and download traverse more than 2 GiB logically with one reused 64 KiB buffer', async () => {
  // Controlled transports count chunks without copying them: this is not a network/storage benchmark.
  const total = 2 ** 31 + 17;
  const reusable = new Uint8Array(64 * 1024);
  const generated = () => {
    let remaining = total;
    return new ReadableStream({ pull(controller) {
      if (!remaining) { controller.close(); return; }
      const size = Math.min(remaining, reusable.byteLength);
      controller.enqueue(size === reusable.byteLength ? reusable : reusable.subarray(0, size));
      remaining -= size;
    } }, { highWaterMark: 0 });
  };
  let sourceProgress = 0, sinkProgress = 0, written = 0, closed = false;
  const committed = await uploadStream({ url, key: 'key', maxBytes: total, knownLength: total,
    source: generated(), onProgress(event) { if (event.phase === 'reading') sourceProgress = event.bytesRead; },
    fetch: async (_url, init) => {
      assert.equal(init.headers.get('x-upload-length'), String(total));
      const reader = init.body.getReader(); let count = 0;
      try { while (true) { const next = await reader.read(); if (next.done) break; count += next.value.byteLength; } }
      finally { reader.releaseLock(); }
      return Response.json({ committed: true, key: 'key', bytes: count, etag: 'logical-only' }, { status: 201 });
    } });
  const downloaded = await downloadTo({ url, maxBytes: total,
    destination: new WritableStream({ write(chunk) { written += chunk.byteLength; }, close() { closed = true; } }),
    onProgress(event) { sinkProgress = event.bytesWritten; },
    fetch: async () => new Response(generated(), { headers: {
      'Content-Length': String(total), ETag: 'logical-only', 'Accept-Ranges': 'bytes',
    } }) });
  assert.equal(committed.bytes, total); assert.equal(sourceProgress, total);
  assert.equal(downloaded.bytesWritten, total); assert.equal(sinkProgress, total);
  assert.equal(written, total); assert.equal(closed, true);
});

test('unsafe limits and malformed ranges reject before acquiring sink', async () => {
  for (const maxBytes of [-1, 0.5, Infinity, 2 ** 53]) {
    const destination = sink();
    await assert.rejects(downloadTo({ url, maxBytes, destination: destination.stream }), isCode('INVALID_ARGUMENT'));
    assert.equal(destination.stream.locked, false);
  }
  for (const range of [{ suffix: 0 }, { start: 2, end: 1 }, { start: -1 }]) {
    await assert.rejects(downloadTo({ url, maxBytes: 10, range, destination: sink().stream }), isCode('INVALID_ARGUMENT'));
  }
});

test('objectUrl encodes a key into one segment without URL query injection', () => {
  assert.equal(objectUrl('https://example.test/objects/', 'a/b?c#d').href,
    'https://example.test/objects/a%2Fb%3Fc%23d');
});

test('unsupported request-stream syntax rejects without reading or buffering source', async () => {
  const original = globalThis.Request;
  let pulls = 0;
  const input = source([chunk(1)], { pull() { pulls++; } });
  globalThis.Request = class { constructor() { throw new TypeError('Streaming bodies unsupported'); } };
  try {
    await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 10, source: input, fetch: () => assert.fail() }),
      error => isCode('UNSUPPORTED')(error) && error.uploadOutcome === 'not-started');
    assert.equal(pulls, 0); assert.equal(input.locked, false);
  } finally { globalThis.Request = original; }
});

test('server finalization uncertainty is not classified as rejected', async () => {
  await assert.rejects(uploadStream({ url, key: 'key', maxBytes: 10, source: source([chunk(1)]),
    fetch: async () => Response.json({ committed: null, code: 'STORAGE_FAILURE', message: 'Lost completion response' }, { status: 502 }) }),
    error => isCode('HTTP_ERROR')(error) && error.uploadOutcome === 'unknown'
      && error.message.includes('STORAGE_FAILURE: Lost completion response'));
});

test('async observer rejection is observational, and pre-aborted download does not fetch', async () => {
  await uploadStream({ url, key: 'key', maxBytes: 1, source: source([chunk(1)]), fetch: commitFetch(),
    async onProgress() { throw new Error('async observer failed'); } });
  await sleep(0);
  const destination = sink();
  await assert.rejects(downloadTo({ url, maxBytes: 1, destination: destination.stream,
    signal: AbortSignal.abort('before'), fetch: () => assert.fail() }), isCode('ABORTED'));
  assert.equal(destination.stream.locked, false); assert.ok(destination.aborted);
});

test('dot keys and ambiguous range objects are rejected instead of normalized silently', async () => {
  for (const key of ['.', '..', '']) assert.throws(() => objectUrl('https://example.test/objects', key));
  await assert.rejects(downloadTo({ url, maxBytes: 10, destination: sink().stream, range: { start: 0, suffix: 1 } }),
    isCode('INVALID_ARGUMENT'));
});
