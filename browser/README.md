# Browser streams for s3-file-streams

Version **0.1.0** · TypeScript / ES modules · Web Streams + Fetch · MIT

English · [Русский](README.ru.md)

One continuous browser stream goes through your HTTP application to storage. Downloads go into a caller-provided writable stream. The core has no runtime dependencies and does not require React. It never collects a file into a Blob or invents client-visible multipart sessions.

This package is implemented locally; it has not been published to npm. Package ownership and release credentials still need verification before a registry release.

## Build and consume

Use Node 20+ for tooling; local checks use Node 24 and TypeScript 7. Browsers need the APIs and transport described below.

```sh
npm ci
npm test
npm run test:package
npm pack
```

Install the resulting `59alex-s3-file-streams-0.1.0.tgz` in a consumer project:

```sh
npm install ./59alex-s3-file-streams-0.1.0.tgz
```

```ts
import { uploadStream, downloadTo, objectUrl, streamCapabilities, TransferError }
  from '@59alex/s3-file-streams';
```

The package exports ES modules and TypeScript declarations. Include the DOM library in the consumer's TypeScript configuration. Node is used for tests/tooling; a Node fetch implementation is not evidence of browser transport compatibility.

## Capabilities at a glance

| Capability | Contract |
|---|---|
| Upload | One raw PUT body from `ReadableStream<Uint8Array>`; known or unknown total length. |
| Commit | Resolve only after HTTP 201 JSON matches the expected key, consumed bytes, and a non-empty opaque ETag. |
| Download | GET body to `WritableStream<Uint8Array>`, awaiting every write and closing only after length validation. |
| Ranges | One closed, open-ended, or suffix byte range with strict 206/header validation. |
| Cancellation | Abort the fetch and cancel/abort owned streams; remote commit or prior sink effects may still have happened. |
| Limits | Required `maxBytes`, optional upload `knownLength`, safe integer arithmetic, and at most 16 KiB of commit/error JSON. |
| Progress | Source bytes accepted for upload; separate server commit event. Download bytes accepted by completed sink writes. |
| Frameworks | Use directly from application code. No hidden React state, timers, polling, or automatic retries. |

## 1. Check syntax support, then verify your real deployment

```ts
const capabilities = streamCapabilities();
if (!capabilities.requestStreamSyntax) {
  throw new Error('This browser cannot use streaming request bodies');
}
// capabilities.transportVerified is deliberately always false.
```

The probe constructs a request but sends no data. It cannot prove that the browser, negotiated HTTP protocol, proxy, server, or storage adapter streams end to end. `uploadStream` rejects unsupported syntax explicitly with `UNSUPPORTED`; it never falls back to buffering the complete input.

Chromium request streams use `duplex: 'half'` and require an HTTP/2 or HTTP/3 path. HTTP/1 streaming upload is rejected, despite a successful constructor probe. The response is not exposed until the request body finishes. Cross-origin requests require CORS preflight; redirects are disabled by this library because a one-shot source cannot be replayed safely. See the [Chrome request-streaming guide](https://developer.chrome.com/docs/capabilities/web-apis/fetch-streaming-requests).

Configure your endpoint, including any reverse proxy, to stream bodies and disable response compression for raw object downloads. A browser progress counter alone does not establish server-side streaming. The included transport check observes bytes through a separate request while the upload source is still open.

## 2. Upload a file without materializing it

```ts
const file = document.querySelector<HTMLInputElement>('#file')!.files![0]!;
const key = 'uploads/report.bin';
const stop = new AbortController();

const commit = await uploadStream({
  url: objectUrl('https://files.example.test/objects', key),
  key,
  source: file.stream(),
  knownLength: file.size,
  maxBytes: 512 * 1024 * 1024,
  signal: stop.signal,
  headers: { 'Content-Type': file.type || 'application/octet-stream' },
  onProgress(event) {
    if (event.phase === 'reading') {
      console.log('Source accepted:', event.bytesRead, 'Commit not confirmed');
    } else {
      console.log('Storage commit confirmed:', event.commit);
    }
  },
});
console.log(commit.key, commit.bytes, commit.etag);
```

Expected result: `{ committed: true, key, bytes, etag }` after the server confirms storage completion. The ETag is opaque; it is not assumed to be an MD5 or a checksum of a multipart object. Returned results and progress objects are frozen.

`knownLength` is checked locally at EOF and on every chunk, and sent as `X-Upload-Length` for server validation. Browser code cannot set a reliable `Content-Length` for a stream; caller-supplied `Content-Length`, `X-Upload-Length`, and `Content-Range` upload headers are rejected. Empty sources and known length zero are supported.

`objectUrl` encodes the logical key as one URL segment. It rejects empty keys and `.`/`..` dot segments, which URL parsers normalize. The application must still enforce its own allowed-key and authorization policy. Do not percent-encode a key before passing it.

## 3. Upload generated data of unknown length

```ts
let remainingChunks = 4;
const generated = new ReadableStream<Uint8Array>({
  pull(controller) {
    if (remainingChunks-- === 0) {
      controller.close();
    } else {
      controller.enqueue(new Uint8Array(64 * 1024).fill(7));
    }
  },
}, { highWaterMark: 0 });

const commit = await uploadStream({
  url: objectUrl('https://files.example.test/objects', 'generated.bin'),
  key: 'generated.bin',
  source: generated,
  maxBytes: 1024 * 1024,
});
// commit.bytes === 262144
```

Omit `knownLength` when the final size is not known. `maxBytes` is still required. The first chunk that would exceed a size bound is rejected before it is forwarded; earlier bytes may already be in flight. Progress excludes a rejected chunk. The entire source is never accumulated by the library.

The upload bridge has no prefetch queue and retains at most its current source chunk. The caller controls upstream stream queues/chunk sizes, and the browser/network/proxy may buffer independently. This is not a universal process-memory bound. A very large caller-created chunk is already a large allocation before this library sees it.

All sizes are non-negative safe integer numbers, up to `Number.MAX_SAFE_INTEGER`. Addition is checked before it can overflow. Java `long` values beyond that boundary are rejected rather than rounded. Arithmetic tests cross 2 GiB without allocating 2 GiB; they are not evidence of a real multi-gigabyte storage transfer.

## 4. Cancel and interpret failures

```ts
const controller = new AbortController();
const transfer = uploadStream({
  url: objectUrl('https://files.example.test/objects', 'one-shot.bin'),
  key: 'one-shot.bin',
  source: file.stream(),
  knownLength: file.size,
  maxBytes: 512 * 1024 * 1024,
  signal: controller.signal,
});
// Wire this to an application cancel button:
controller.abort('User cancelled');
try {
  await transfer;
} catch (error) {
  if (error instanceof TransferError) {
    console.log(error.code, error.uploadOutcome, error.status);
  }
}
```

| Upload outcome | Meaning |
|---|---|
| `not-started` | Rejected locally before fetch was invoked. |
| `rejected` | A non-201 server response explicitly supplied `{ committed: false, code, message }`. |
| `unknown` | The request was attempted but no valid commit or explicit rejection was observed. This includes connection loss, cancellation, malformed commit metadata, and `{ committed: null, ... }` storage-finalization failures. |

Abort is not a rollback promise. A server may commit while cancellation or a lost response prevents the caller from learning the result. Reconcile the object through an application-level identity/status design before deciding to retry. There is no automatic retry, resume, delete, or attempt to replay a consumed stream.

Synchronous and asynchronous progress-observer exceptions are ignored so a UI callback cannot turn an observed commit into a failure. Observers are not awaited and should remain inexpensive. They do not apply backpressure.

## 5. Stream a download to a caller-owned sink

The core accepts any writable byte stream. This JavaScript example uses the optional File System Access API; availability depends on the browser and a user gesture.

```js
if (typeof window.showSaveFilePicker !== 'function') {
  throw new Error('Choose a supported writable destination; no Blob fallback is provided');
}
const handle = await window.showSaveFilePicker({ suggestedName: 'report.bin' });
const destination = await handle.createWritable();
const result = await downloadTo({
  url: objectUrl('https://files.example.test/objects', 'uploads/report.bin'),
  destination,
  maxBytes: 512 * 1024 * 1024,
  onProgress({ bytesWritten, responseBytes }) {
    console.log('Sink accepted:', bytesWritten, '/', responseBytes);
  },
});
console.log(result.complete, result.bytesWritten, result.etag);
```

Every `writer.write` is awaited before another response read. Success means the response length matched and the sink's `close` resolved. Progress records completed writes to the sink, not physical disk durability. A sink error cancels the response/fetch. Network errors, mismatched lengths, and abort signal the destination to abort; the sink implementation decides whether partially written data can be discarded.

Cancellation returns promptly even if an in-flight sink write does not cooperate. That write may still have effects, and the underlying sink's abort may finish later. Supply a sink with appropriate cancellation and cleanup behavior for your application.

## 6. Request one range

```ts
const result = await downloadTo({
  url: objectUrl('https://files.example.test/objects', 'uploads/report.bin'),
  destination,
  range: { start: 0, end: 1023 }, // inclusive
  maxBytes: 1024,
});
console.log(result.range); // { start: 0, end: 1023, total: objectSize }
```

Other forms are `{ start: 1024 }` and `{ suffix: 1024 }`. A range response must be 206 with a matching `Content-Range` and `Content-Length`. A server that ignores the range and returns 200 is rejected before bytes are written. A 416 remains an HTTP error. Multi-range requests are not supported.

The bytes go to the supplied sink from its current position. The library does not reposition a file, assemble multiple ranges, or implement resumable download. Applications combining ranges must ensure the object version did not change between requests.

## 7. HTTP contract and resource ownership

The baseline endpoint is `/objects/{encodedKey}`:

- PUT: raw stream, optional `Content-Type` and `X-Upload-Length`. Only HTTP 201 JSON `{ committed: true, key, bytes, etag }` is accepted as success. Metadata is limited to 16 KiB.
- GET: 200 full object or 206 for a requested single range. `Content-Length`, `ETag`, and `Accept-Ranges: bytes` must be visible; a partial response also needs `Content-Range`. Counts must be safe integers.
- Non-identity `Content-Encoding` is rejected because transparent decompression changes byte accounting. Disable compression for this endpoint. Error bodies are never written into the destination.
- For cross-origin access, allow PUT/GET, `Content-Type`, `X-Upload-Length`, and `Range`; expose `Content-Length`, `ETag`, `Accept-Ranges`, `Content-Range`, `Content-Type`, and `Content-Encoding`. Configure credentials/origins for your application, rather than copying a wildcard demo policy to production.

`credentials` defaults to `same-origin`. Authorization headers can be supplied through `headers`. A custom `fetch` function is optional and must preserve streaming/cancellation semantics.

Argument/capability validation leaves streams untouched. Once acquired, the operation owns the source reader or destination writer until completion/error and releases its lock. Upload EOF releases the source; failure/cancellation cancels it. Download success closes the destination; failures abort it. Do not concurrently read, write, or mutate enqueued byte buffers. Neither transfer owns application credentials, the upstream producer's external resources, or the business meaning of a key.

## 8. Tests and runnable demo

```sh
npm test
npm run test:package
npm run demo
```

Open `https://127.0.0.1:9443` in a test browser and use **Run browser checks**. OpenSSL generates a short-lived self-signed certificate in a temporary directory. The fixture listens only on loopback and removes the key/certificate on shutdown. Trust exceptions are for this local test only. No key is stored in the repository.

The demo accepts an object endpoint base URL for the Java HTTP/2 adapter. Its bundled Node server is a bounded in-memory protocol fixture (8 MiB maximum), not S3 and not evidence of storage durability. Pointing it at another endpoint requires that endpoint's TLS/CORS configuration.

Start the repository's Java TLS demo against a disposable S3 backend, set the endpoint field to `https://127.0.0.1:19443/objects`, and select **Run Java adapter checks**. The server must allow the origin `https://127.0.0.1:9443`. The [integration scenarios](demo/java-integration-checks.js) create unique test objects: known-size 6 MiB + 17 bytes, unknown-size 1 MiB + 13 bytes, and empty; validate SHA-256 round trips; exercise boundary/suffix ranges and missing objects; then abort an unfinished upload. They report the cancelled key for separate server-side object/multipart cleanup verification. Test-only bounded buffers calculate the hashes; transfer code does not buffer the complete file.

Observed checks:

| Environment | Evidence |
|---|---|
| Node 24 / Web Streams | 27 unit cases: size boundaries, source/sink failures, stalled-write cancellation, protocol/range validation, observer exceptions, and both transfer paths counting 2 GiB + 17 bytes with one reused 64 KiB buffer. That last case uses controlled transports, not network/storage I/O. |
| Headless Chromium 156 / local HTTP/2 TLS | 10 browser checks. The server observed bytes before source EOF; 262,144-byte content round trip and a 48-byte range succeeded. Abort, network reset, invalid commit, limits, and sink failure were exercised. |
| Same Chromium / local HTTP/1 TLS | Streaming upload rejected despite a successful request constructor probe. |
| Headless Firefox 156 | Upload rejected as `UNSUPPORTED` before reading/locking the source. A two-byte download range into a writable stream succeeded. |
| Chromium 156 → Java HTTP/2 TLS → Garage S3 | 7 integration checks: SHA-256 round trips of 6,291,473 known bytes, 1,048,589 unknown bytes, and an empty object; a 53-byte range crossing 5 MiB and a 31-byte suffix; 404 without sink writes; source cancellation after reading 6 MiB with an explicitly unknown commit outcome. |
| Clean npm tarball consumer | Checked separately with `npm run test:package`; exports, declarations, and zero-byte upload/download. |
| HTTP/3, Safari, production CDN/proxy paths | Not verified. A syntax probe does not establish support. |

For the observed Java/Garage abort case, a separate backend check confirmed HEAD 404 and zero active multipart uploads for that key. This verifies cleanup for that scenario; it does not turn cancellation into a general rollback guarantee. Successful synthetic test objects were removed after verification.

The [browser checks](demo/browser-checks.js) are readable executable scenarios. Use a headless browser for automation; the checks expose their result as `window.__streamResults`. The unit suite uses controlled fetch responses and is not a storage integration test. See the repository's Java/storage evidence for the full backend path.

## License

[MIT](LICENSE). No runtime dependency is bundled. Browser and server behavior remains subject to their own platform contracts.
