// EN: Explicit integration run: requires the Java TLS demo and a disposable storage backend.
// RU: Сквозной запуск требует Java TLS demo и отдельное тестовое хранилище.
// EN: Bounded buffers below serve test assertions; the library itself transfers streams.
// RU: Ограниченные буферы ниже нужны для проверок; сама библиотека передаёт потоки.
import { uploadStream, downloadTo, objectUrl } from '/dist/index.js';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const assert = (condition, message) => { if (!condition) throw new Error(message); };
const byteAt = position => (position * 31 + Math.floor(position / 251)) % 256;
const hex = bytes => Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
const sha256 = async bytes => hex(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)));

export async function runJavaIntegrationChecks(base = 'https://127.0.0.1:19443/objects', report = () => {}) {
  const prefix = 'browser-java-' + Date.now();
  const results = [];
  const check = async (name, work) => {
    report(`Running: ${name}`);
    results.push({ name, ok: true, detail: await work() });
  };
  let knownKey;
  const knownSize = 6 * 1024 * 1024 + 17;
  for (const [name, size, declared] of [['known', knownSize, true], ['unknown', 1024 * 1024 + 13, false], ['empty', 0, true]]) {
    await check(`${name} upload and SHA-256 download round trip`, async () => {
      const key = prefix + '-' + name;
      if (name === 'known') knownKey = key;
      const expected = Uint8Array.from({ length: size }, (_, index) => byteAt(index));
      let position = 0, eof = false;
      const phases = [];
      const source = new ReadableStream({ pull(controller) {
        if (position === size) { eof = true; controller.close(); return; }
        const end = Math.min(position + 64 * 1024, size);
        controller.enqueue(expected.slice(position, end)); position = end;
      } }, { highWaterMark: 0 });
      const commit = await uploadStream({ url: objectUrl(base, key), key, source, maxBytes: size,
        ...(declared ? { knownLength: size } : {}), onProgress(event) {
          phases.push(event.phase);
          if (event.phase === 'committed') assert(eof, 'Commit before source EOF');
        } });
      assert(eof && commit.bytes === size && phases.at(-1) === 'committed', 'Commit mismatch');
      const downloaded = new Uint8Array(size); let written = 0;
      const result = await downloadTo({ url: objectUrl(base, key), maxBytes: size,
        destination: new WritableStream({ async write(chunk) {
          downloaded.set(chunk, written); written += chunk.length;
          await delay(1);
        } }) });
      const expectedHash = await sha256(expected), receivedHash = await sha256(downloaded);
      assert(written === size && result.bytesWritten === size && expectedHash === receivedHash, 'Round-trip content mismatch');
      return { key, bytes: size, sha256: receivedHash, etag: commit.etag };
    });
  }
  for (const [name, range, start, bytes] of [
    ['range across internal 5 MiB boundary', { start: 5 * 1024 * 1024 - 11, end: 5 * 1024 * 1024 + 41 }, 5 * 1024 * 1024 - 11, 53],
    ['suffix range', { suffix: 31 }, knownSize - 31, 31],
  ]) {
    await check(name, async () => {
      let count = 0;
      const result = await downloadTo({ url: objectUrl(base, knownKey), maxBytes: bytes, range,
        destination: new WritableStream({ write(chunk) {
          for (const byte of chunk) { assert(byte === byteAt(start + count), 'Range byte mismatch'); count++; }
        } }) });
      assert(count === bytes && result.range.start === start && result.range.total === knownSize, 'Range metadata mismatch');
      return result.range;
    });
  }
  await check('missing object never reaches the destination', async () => {
    let writes = 0;
    try {
      await downloadTo({ url: objectUrl(base, prefix + '-missing'), maxBytes: 100,
        destination: new WritableStream({ write() { writes++; } }) });
      throw new Error('Missing object unexpectedly succeeded');
    } catch (error) { assert(error.code === 'HTTP_ERROR' && error.status === 404 && writes === 0, 'Missing-object handling mismatch'); }
    return { writes };
  });
  await check('abort after 6 MiB source read cancels source; commit remains unknown', async () => {
    const key = prefix + '-abort'; const abort = new AbortController();
    let cancelled = false, read = 0;
    const source = new ReadableStream({ async pull(controller) {
      await delay(5);
      controller.enqueue(new Uint8Array(64 * 1024));
    }, cancel() { cancelled = true; } }, { highWaterMark: 0 });
    try {
      await uploadStream({ url: objectUrl(base, key), key, source, knownLength: 12 * 1024 * 1024,
        maxBytes: 12 * 1024 * 1024, signal: abort.signal, onProgress(event) {
          if (event.phase === 'reading') { read = event.bytesRead; if (read >= 6 * 1024 * 1024) abort.abort(); }
        } });
      throw new Error('Cancelled upload unexpectedly succeeded');
    } catch (error) { assert(error.code === 'ABORTED' && error.uploadOutcome === 'unknown' && cancelled, 'Abort result mismatch'); }
    return { key, bytesRead: read, cancelled, outcome: 'unknown', backendCleanupRequiresServerVerification: true };
  });
  return { userAgent: navigator.userAgent, base, passed: results.length, results };
}
