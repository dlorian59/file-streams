import { uploadStream, downloadTo, streamCapabilities, objectUrl } from '/dist/index.js';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const assert = (condition, message) => { if (!condition) throw new Error(message); };
const source = values => new ReadableStream({ start(controller) { for (const value of values) controller.enqueue(value); controller.close(); } });
export async function runBrowserChecks(report = () => {}) {
  const results = [];
  const base = location.origin + '/objects';
  const { http1Origin } = await (await fetch('/fixture-info')).json();
  const check = async (name, work) => { report(`Running: ${name}`); const detail = await work(); results.push({ name, ok: true, detail }); };
  let referenceKey;
  await check('syntax probe is not a transport claim', () => {
    const capabilities = streamCapabilities();
    assert(capabilities.requestStreamSyntax && !capabilities.transportVerified, 'Streaming syntax probe failed');
    return capabilities;
  });
  await check('HTTP/2 server sees bytes before source EOF; commit only after EOF', async () => {
    referenceKey = 'h2-' + Date.now();
    const part = 128 * 1024;
    let step = 0, seenBeforeEof = false, protocol;
    const events = [];
    const body = new ReadableStream({ async pull(controller) {
      if (step++ === 0) { controller.enqueue(new Uint8Array(part).fill(1)); return; }
      if (step === 2) {
        for (let attempt = 0; attempt < 60; attempt++) {
          const state = await (await fetch('/stats?key=' + encodeURIComponent(referenceKey))).json();
          if (state.bytes > 0 && !state.ended) { seenBeforeEof = true; protocol = state.protocol; break; }
          await delay(50);
        }
        assert(seenBeforeEof, 'Server did not see bytes before upload EOF (request may be buffered)');
        controller.enqueue(new Uint8Array(part).fill(2)); return;
      }
      controller.close();
    } }, { highWaterMark: 0 });
    const commit = await uploadStream({ url: objectUrl(base, referenceKey), key: referenceKey,
      source: body, maxBytes: part * 2, knownLength: part * 2, onProgress: event => events.push(event.phase) });
    assert(commit.bytes === part * 2 && protocol === '2.0', 'Wrong byte count or negotiated protocol');
    assert(events.at(-1) === 'committed' && events.filter(value => value === 'committed').length === 1, 'Commit phase mismatch');
    return { bytes: commit.bytes, protocol, seenBeforeEof };
  });
  await check('download into a slow caller-owned sink preserves content', async () => {
    let count = 0;
    const result = await downloadTo({ url: objectUrl(base, referenceKey), maxBytes: 256 * 1024,
      destination: new WritableStream({ async write(value) {
        for (const byte of value) { assert(byte === (count < 128 * 1024 ? 1 : 2), 'Byte mismatch'); count++; }
        await delay(2);
      } }) });
    assert(result.bytesWritten === 256 * 1024, 'Download size mismatch');
    return { bytes: count };
  });
  await check('single HTTP byte range is validated', async () => {
    let count = 0;
    const result = await downloadTo({ url: objectUrl(base, referenceKey), maxBytes: 48, range: { start: 11, end: 58 },
      destination: new WritableStream({ write(value) { count += value.length; assert(value.every(byte => byte === 1), 'Range data mismatch'); } }) });
    assert(count === 48 && result.range.start === 11 && result.range.end === 58, 'Range mismatch');
    return result.range;
  });
  await check('AbortSignal cancels one-shot upload source', async () => {
    const controller = new AbortController(); let cancelled = false;
    const key = 'abort-' + Date.now();
    const body = new ReadableStream({ async pull(stream) { await delay(10); stream.enqueue(new Uint8Array(1024)); }, cancel() { cancelled = true; } }, { highWaterMark: 0 });
    try {
      await uploadStream({ url: objectUrl(base, key), key, source: body,
        maxBytes: 1024 * 1024, signal: controller.signal, onProgress() { controller.abort(); } });
      throw new Error('Abort unexpectedly succeeded');
    } catch (error) { assert(error.code === 'ABORTED' && cancelled, 'Abort/source cancellation mismatch'); }
    return { cancelled };
  });
  await check('unknown-length upload enforces maxBytes', async () => {
    try {
      const key = 'limit-' + Date.now();
      await uploadStream({ url: objectUrl(base, key), key, maxBytes: 50, source: source([new Uint8Array(64)]) });
      throw new Error('Oversize upload unexpectedly succeeded');
    } catch (error) { assert(error.code === 'LIMIT_EXCEEDED', 'Wrong size-limit error'); }
  });
  await check('invalid storage commit is not accepted', async () => {
    const key = 'invalid-commit-' + Date.now();
    try { await uploadStream({ url: objectUrl(base, key), key, source: source([new Uint8Array(8)]), maxBytes: 8 }); throw new Error('Bad commit accepted'); }
    catch (error) { assert(error.code === 'PROTOCOL_ERROR' && error.uploadOutcome === 'unknown', 'Wrong commit-validation result'); }
  });
  await check('HTTP/1 TLS fails explicitly despite a successful syntax probe', async () => {
    const key = 'http1-' + Date.now();
    try { await uploadStream({ url: objectUrl(http1Origin + '/objects', key), key, source: source([new Uint8Array(8)]), maxBytes: 8 }); throw new Error('HTTP/1 upload unexpectedly succeeded'); }
    catch (error) { assert(error.code === 'NETWORK_ERROR', 'Expected a network/capability failure on HTTP/1'); }
    return { syntaxProbePassed: streamCapabilities().requestStreamSyntax, transportSucceeded: false };
  });
  await check('network reset cancels source and leaves commit unknown', async () => {
    const key = 'network-drop-' + Date.now(); let cancelled = false;
    const body = new ReadableStream({ async pull(controller) { await delay(20); controller.enqueue(new Uint8Array(1024)); }, cancel() { cancelled = true; } }, { highWaterMark: 0 });
    try { await uploadStream({ url: objectUrl(base, key), key, source: body, maxBytes: 1024 * 1024 }); throw new Error('Reset unexpectedly succeeded'); }
    catch (error) { assert(error.code === 'NETWORK_ERROR' && error.uploadOutcome === 'unknown' && cancelled, 'Reset cancellation/outcome mismatch'); }
    return { cancelled, outcome: 'unknown' };
  });
  await check('sink error fails download instead of reporting success', async () => {
    try { await downloadTo({ url: objectUrl(base, referenceKey), maxBytes: 256 * 1024,
      destination: new WritableStream({ write() { throw new Error('Simulated disk full'); } }) }); throw new Error('Broken sink unexpectedly succeeded'); }
    catch (error) { assert(error.code === 'SINK_ERROR', 'Wrong sink error'); }
  });
  return { userAgent: navigator.userAgent, fixtureOnly: true, passed: results.length, results };
}
