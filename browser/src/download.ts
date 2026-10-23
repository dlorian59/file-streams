import { TransferError } from './errors.js';
import { abortable, byteCount, checkedAdd, decimal, notify } from './internal.js';

export type ByteRange = { readonly start: number; readonly end?: number } | { readonly suffix: number };
export interface DownloadRange { readonly start: number; readonly end: number; readonly total: number }
export interface DownloadProgress { readonly bytesWritten: number; readonly responseBytes: number }
export interface DownloadResult {
  readonly complete: true;
  readonly bytesWritten: number;
  readonly etag: string;
  readonly contentType: string | null;
  readonly range: DownloadRange | undefined;
}
export interface DownloadOptions {
  readonly url: string | URL;
  readonly destination: WritableStream<Uint8Array>;
  readonly maxBytes: number;
  readonly range?: ByteRange;
  readonly signal?: AbortSignal;
  readonly headers?: HeadersInit;
  readonly credentials?: RequestCredentials;
  readonly onProgress?: (progress: DownloadProgress) => void;
  readonly fetch?: typeof globalThis.fetch;
}

function rangeHeader(range: ByteRange): string {
  if ('suffix' in range) {
    if ('start' in range || 'end' in range) throw new TransferError('INVALID_ARGUMENT', 'Choose suffix or start/end range, not both');
    byteCount(range.suffix, 'range.suffix');
    if (range.suffix === 0) throw new TransferError('INVALID_ARGUMENT', 'range.suffix must be positive');
    return `bytes=-${range.suffix}`;
  }
  byteCount(range.start, 'range.start');
  if (range.end !== undefined) {
    byteCount(range.end, 'range.end');
    if (range.end < range.start) throw new TransferError('INVALID_ARGUMENT', 'range.end precedes range.start');
  }
  return `bytes=${range.start}-${range.end ?? ''}`;
}

function responseMetadata(response: Response, requested: ByteRange | undefined, maxBytes: number) {
  const status = requested ? 206 : 200;
  if (response.status !== status) {
    if (response.status === 200 || response.status === 206)
      throw new TransferError('PROTOCOL_ERROR', `Expected HTTP ${status}, got ${response.status}; refusing a mismatched range`);
    throw new TransferError('HTTP_ERROR', `Download returned HTTP ${response.status}`, { status: response.status });
  }
  const encoding = response.headers.get('content-encoding');
  if (encoding !== null && encoding.toLowerCase().trim() !== 'identity')
    throw new TransferError('PROTOCOL_ERROR', 'Encoded responses cannot preserve raw byte/range accounting');
  const length = decimal(response.headers.get('content-length'), 'Content-Length');
  if (length > maxBytes) throw new TransferError('LIMIT_EXCEEDED', 'Response Content-Length exceeds maxBytes');
  const etag = response.headers.get('etag');
  if (!etag?.trim()) throw new TransferError('PROTOCOL_ERROR', 'A visible ETag response header is required');
  if (response.headers.get('accept-ranges')?.trim().toLowerCase() !== 'bytes')
    throw new TransferError('PROTOCOL_ERROR', 'Accept-Ranges: bytes must be visible');
  let range: DownloadRange | undefined;
  const contentRange = response.headers.get('content-range');
  if (!requested && contentRange !== null)
    throw new TransferError('PROTOCOL_ERROR', 'A full response must not carry Content-Range');
  if (requested) {
    const parts = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(contentRange ?? '');
    if (!parts) throw new TransferError('PROTOCOL_ERROR', 'Invalid or inaccessible Content-Range');
    const start = decimal(parts[1] ?? null, 'range start');
    const end = decimal(parts[2] ?? null, 'range end');
    const total = decimal(parts[3] ?? null, 'range total');
    if (total === 0 || start > end || end >= total || end - start + 1 !== length)
      throw new TransferError('PROTOCOL_ERROR', 'Inconsistent Content-Range and Content-Length');
    const expectedStart = 'suffix' in requested ? Math.max(0, total - requested.suffix) : requested.start;
    const expectedEnd = 'suffix' in requested ? total - 1 : Math.min(requested.end ?? total - 1, total - 1);
    if (start !== expectedStart || end !== expectedEnd)
      throw new TransferError('PROTOCOL_ERROR', 'Response range does not match the requested range');
    range = Object.freeze({ start, end, total });
  }
  return { length, etag, range, contentType: response.headers.get('content-type') };
}

/** Write one response to the supplied sink, awaiting each write and closing only after complete validation. */
export async function downloadTo(options: DownloadOptions): Promise<DownloadResult> {
  byteCount(options.maxBytes, 'maxBytes');
  if (!options.destination || options.destination.locked)
    throw new TransferError('INVALID_ARGUMENT', 'An unlocked destination stream is required');
  const headers = new Headers(options.headers);
  if (headers.has('range')) throw new TransferError('INVALID_ARGUMENT', 'Use the range option instead of a Range header');
  if (options.range) headers.set('range', rangeHeader(options.range));

  if (typeof (options.fetch ?? globalThis.fetch) !== 'function')
    throw new TransferError('UNSUPPORTED', 'Fetch is not available in this runtime');
  const writer = options.destination.getWriter();
  const controller = new AbortController();
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let response: Response | undefined;
  let complete = false;
  let stage: 'network' | 'sink' = 'network';
  let bytesWritten = 0;
  const abort = () => {
    const error = new TransferError('ABORTED', 'Download aborted; an in-flight sink write may already have taken effect',
      { cause: options.signal?.reason });
    controller.abort(error);
    void reader?.cancel(error).catch(() => {});
    void writer.abort(error).catch(() => {});
  };
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) abort();
  try {
    if (controller.signal.aborted) throw controller.signal.reason;
    response = await abortable((options.fetch ?? globalThis.fetch)(options.url, {
      method: 'GET', headers, signal: controller.signal, redirect: 'error', mode: 'cors',
      credentials: options.credentials ?? 'same-origin', cache: 'no-store',
    }), controller.signal);
    const metadata = responseMetadata(response, options.range, options.maxBytes);
    if (!response.body) throw new TransferError('PROTOCOL_ERROR', 'Response has no readable body');
    reader = response.body.getReader();
    while (true) {
      stage = 'network';
      const next = await abortable(reader.read(), controller.signal);
      if (next.done) break;
      if (!(next.value instanceof Uint8Array)) throw new TransferError('PROTOCOL_ERROR', 'Response contains a non-byte chunk');
      const nextCount = checkedAdd(bytesWritten, next.value.byteLength, options.maxBytes);
      if (nextCount > metadata.length) throw new TransferError('LENGTH_MISMATCH', 'Response exceeds Content-Length');
      stage = 'sink';
      await abortable(writer.write(next.value), controller.signal);
      bytesWritten = nextCount;
      notify(options.onProgress, { bytesWritten, responseBytes: metadata.length });
    }
    if (bytesWritten !== metadata.length)
      throw new TransferError('LENGTH_MISMATCH', `Response ended at ${bytesWritten} bytes, expected ${metadata.length}`);
    stage = 'sink';
    await abortable(writer.close(), controller.signal);
    complete = true;
    return Object.freeze({ complete: true, bytesWritten, etag: metadata.etag, range: metadata.range,
      contentType: metadata.contentType });
  } catch (cause) {
    const error = controller.signal.aborted ? controller.signal.reason
      : cause instanceof TransferError ? cause
      : new TransferError(stage === 'sink' ? 'SINK_ERROR' : 'NETWORK_ERROR',
        stage === 'sink' ? 'Destination write/close failed' : 'Download transport failed', { cause });
    controller.abort(error);
    void writer.abort(error).catch(() => {});
    throw error;
  } finally {
    options.signal?.removeEventListener('abort', abort);
    if (reader) {
      if (!complete) void reader.cancel(controller.signal.reason).catch(() => {});
      reader.releaseLock();
    } else if (response?.body) {
      void response.body.cancel(controller.signal.reason).catch(() => {});
    }
    writer.releaseLock();
  }
}
