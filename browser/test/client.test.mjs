import test from 'node:test';
import assert from 'node:assert/strict';
import { createFileStreams, fileProfiles, TransferError } from '../dist/index.js';

test('configured file upload derives length/type/key and preserves transport/auth while validating commit', async () => {
  let seen;
  const files = createFileStreams({ objectsUrl: 'https://files.example/objects', profile: 'attachments',
    headers: { Authorization: 'Bearer application-token' }, credentials: 'include',
    fetch: async (url, init) => {
      seen = { url: String(url), headers: init.headers, credentials: init.credentials };
      const bytes = await new Response(init.body).arrayBuffer();
      assert.equal(new TextDecoder().decode(bytes), 'invoice');
      return Response.json({ committed: true, key: 'account/invoice', bytes: bytes.byteLength, etag: 'opaque' }, { status: 201 });
    } });
  const commit = await files.uploadFile('account/invoice', new Blob(['invoice'], { type: 'text/plain' }));
  assert.equal(commit.bytes, 7);
  assert.equal(seen.url, 'https://files.example/objects/account%2Finvoice');
  assert.equal(seen.headers.get('x-upload-length'), '7');
  assert.equal(seen.headers.get('content-type'), 'text/plain');
  assert.equal(seen.headers.get('authorization'), 'Bearer application-token');
  assert.equal(seen.credentials, 'include');
});

test('configured bounds reject before transport and do not hide cancellation or uncertain acknowledgements', async () => {
  let calls = 0;
  const bounded = createFileStreams({ objectsUrl: 'https://files.example/objects', maxBytes: 2,
    fetch: async () => { calls++; throw Error('must not start'); } });
  await assert.rejects(bounded.uploadFile('invoice', new Blob(['large'])), error =>
    error instanceof TransferError && error.code === 'LIMIT_EXCEEDED' && error.uploadOutcome === 'not-started');
  const controller = new AbortController(); controller.abort();
  await assert.rejects(bounded.uploadFile('empty', new Blob(), { signal: controller.signal }), { code: 'ABORTED' });
  assert.equal(calls, 0);
  const uncertain = createFileStreams({ objectsUrl: 'https://files.example/objects', fetch: async () => { throw Error('lost'); } });
  await assert.rejects(uncertain.uploadFile('invoice', new Blob(['ok'])), { uploadOutcome: 'unknown' });
});

test('download forwards range, per-operation authorization and destination lifetime', async () => {
  const written = [];
  let closed = false;
  const files = createFileStreams({ objectsUrl: new URL('https://files.example/objects'), maxBytes: fileProfiles.attachments.maxBytes,
    headers: { Authorization: 'Bearer old' }, fetch: async (url, init) => {
      assert.equal(String(url), 'https://files.example/objects/invoice');
      assert.equal(init.headers.get('authorization'), 'Bearer refreshed');
      assert.equal(init.headers.get('range'), 'bytes=0-2');
      return new Response(new TextEncoder().encode('inv'), { status: 206, headers: {
        'Content-Length': '3', 'Content-Range': 'bytes 0-2/7', 'Accept-Ranges': 'bytes', ETag: 'opaque',
      } });
    } });
  const destination = new WritableStream({ write(bytes) { written.push(...bytes); }, close() { closed = true; } });
  const result = await files.download('invoice', destination, { range: { start: 0, end: 2 }, headers: { Authorization: 'Bearer refreshed' } });
  assert.equal(result.bytesWritten, 3);
  assert.equal(new TextDecoder().decode(new Uint8Array(written)), 'inv');
  assert.equal(closed, true);
});


test('relative browser endpoint resolves from location while non-browser consumers need an absolute URL', async () => {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'location');
  try {
    delete globalThis.location;
    assert.throws(() => createFileStreams({ objectsUrl: '/attachments' }), TypeError);
    assert.equal(typeof createFileStreams({ objectsUrl: 'https://files.example/attachments' }).uploadFile, 'function');
    Object.defineProperty(globalThis, 'location', { configurable: true,
      value: { href: 'https://app.example/tickets/42?tab=files' } });
    const files = createFileStreams({ objectsUrl: '/attachments', fetch: async (url, init) => {
      assert.equal(String(url), 'https://app.example/attachments/alice%3Ainvoice.pdf');
      const bytes = await new Response(init.body).arrayBuffer();
      return Response.json({ committed: true, key: 'alice:invoice.pdf', bytes: bytes.byteLength, etag: 'opaque' }, { status: 201 });
    } });
    assert.equal((await files.uploadFile('alice:invoice.pdf', new Blob(['invoice']))).bytes, 7);
  } finally {
    if (previous) Object.defineProperty(globalThis, 'location', previous);
    else delete globalThis.location;
  }
});
