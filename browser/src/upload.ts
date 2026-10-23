import { streamCapabilities } from './capabilities.js';
import { TransferError, type UploadOutcome } from './errors.js';
import { abortable, byteCount, checkedAdd, metadataJson, notify, object } from './internal.js';

export interface UploadCommit {
  readonly committed: true;
  readonly key: string;
  readonly bytes: number;
  readonly etag: string;
}
export type UploadProgress =
  | { readonly phase: 'reading'; readonly bytesRead: number; readonly knownLength: number | undefined }
  | { readonly phase: 'committed'; readonly bytesRead: number; readonly commit: UploadCommit };
export interface UploadOptions {
  readonly url: string | URL;
  readonly key: string;
  readonly source: ReadableStream<Uint8Array>;
  readonly maxBytes: number;
  readonly knownLength?: number;
  readonly signal?: AbortSignal;
  readonly headers?: HeadersInit;
  readonly credentials?: RequestCredentials;
  readonly onProgress?: (progress: UploadProgress) => void;
  /** A custom transport must preserve streaming and AbortSignal semantics. */
  readonly fetch?: typeof globalThis.fetch;
}

/** Transfer a one-shot source as a continuous raw PUT, resolving only on a validated commit. */
export async function uploadStream(options: UploadOptions): Promise<UploadCommit> {
  const { source, knownLength, maxBytes } = options;
  let headers: Headers;
  try {
    byteCount(maxBytes, 'maxBytes');
    if (knownLength !== undefined) {
      byteCount(knownLength, 'knownLength');
      if (knownLength > maxBytes)
        throw new TransferError('LIMIT_EXCEEDED', 'knownLength exceeds maxBytes', { uploadOutcome: 'not-started' });
    }
    if (typeof options.key !== 'string' || options.key.length === 0 || !source || typeof source.getReader !== 'function' || source.locked)
      throw new TransferError('INVALID_ARGUMENT', 'A non-empty key and an unlocked source are required', { uploadOutcome: 'not-started' });
    headers = new Headers(options.headers);
    for (const name of ['content-length', 'x-upload-length', 'content-range']) {
      if (headers.has(name)) throw new TransferError('INVALID_ARGUMENT', `${name} is managed by the transfer API`, { uploadOutcome: 'not-started' });
    }
    if (typeof (options.fetch ?? globalThis.fetch) !== 'function' || !streamCapabilities().requestStreamSyntax)
      throw new TransferError('UNSUPPORTED', 'This runtime does not accept streaming fetch request bodies; no buffering fallback is used', { uploadOutcome: 'not-started' });
    headers.set('content-type', headers.get('content-type') ?? 'application/octet-stream');
    if (knownLength !== undefined) headers.set('x-upload-length', String(knownLength));
  } catch (cause) {
    if (cause instanceof TransferError)
      throw new TransferError(cause.code, cause.message, { cause, uploadOutcome: 'not-started' });
    throw new TransferError('INVALID_ARGUMENT', 'Invalid upload options or headers', { cause, uploadOutcome: 'not-started' });
  }

  const reader = source.getReader();
  const controller = new AbortController();
  let bodyController: ReadableStreamDefaultController<Uint8Array> | undefined;
  let released = false;
  let ended = false;
  let bytesRead = 0;
  let fetchStarted = false;
  let sourceFailure: TransferError | undefined;
  const outcome = (): UploadOutcome => fetchStarted ? 'unknown' : 'not-started';
  const release = () => { if (!released) { released = true; reader.releaseLock(); } };
  const cancelSource = (reason: unknown) => {
    if (released) return;
    void reader.cancel(reason).catch(() => {});
    release();
  };
  const abort = () => {
    const error = new TransferError('ABORTED', 'Upload aborted; a sent request may still have committed',
      { cause: options.signal?.reason, uploadOutcome: outcome() });
    controller.abort(error);
    try { bodyController?.error(error); } catch { /* already closed */ }
    cancelSource(error);
  };
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) abort();

  const body = new ReadableStream<Uint8Array>({
    start(stream) { bodyController = stream; if (controller.signal.aborted) stream.error(controller.signal.reason); },
    async pull(stream) {
      try {
        const next = await reader.read();
        if (controller.signal.aborted) return;
        if (next.done) {
          if (knownLength !== undefined && bytesRead !== knownLength)
            throw new TransferError('LENGTH_MISMATCH', `Source ended at ${bytesRead} bytes, expected ${knownLength}`);
          ended = true;
          release();
          stream.close();
          return;
        }
        if (!(next.value instanceof Uint8Array))
          throw new TransferError('SOURCE_ERROR', 'Source must yield Uint8Array chunks');
        const nextCount = checkedAdd(bytesRead, next.value.byteLength, maxBytes);
        if (knownLength !== undefined && nextCount > knownLength)
          throw new TransferError('LENGTH_MISMATCH', 'Source exceeds knownLength');
        bytesRead = nextCount;
        stream.enqueue(next.value);
        notify(options.onProgress, { phase: 'reading', bytesRead, knownLength } as UploadProgress);
      } catch (cause) {
        if (controller.signal.aborted) return;
        sourceFailure = cause instanceof TransferError
          ? new TransferError(cause.code, cause.message, { cause, uploadOutcome: outcome() })
          : new TransferError('SOURCE_ERROR', 'Upload source failed', { cause, uploadOutcome: outcome() });
        stream.error(sourceFailure);
        controller.abort(sourceFailure);
        cancelSource(sourceFailure);
      }
    },
    cancel(reason) { cancelSource(reason); },
  }, { highWaterMark: 0 });

  try {
    if (controller.signal.aborted) throw controller.signal.reason;
    fetchStarted = true;
    const init: RequestInit & { duplex: 'half' } = {
      method: 'PUT', body, duplex: 'half', headers, signal: controller.signal,
      credentials: options.credentials ?? 'same-origin', mode: 'cors', redirect: 'error', cache: 'no-store',
    };
    const response = await abortable((options.fetch ?? globalThis.fetch)(options.url, init), controller.signal);
    if (response.status !== 201) {
      let rejected = false;
      let message = `Upload returned HTTP ${response.status}`;
      try {
        const failure = await metadataJson(response, controller.signal);
        if (object(failure) && (failure.committed === false || failure.committed === null) && typeof failure.code === 'string'
            && typeof failure.message === 'string') {
          rejected = failure.committed === false;
          message += `: ${failure.code}: ${failure.message}`;
        }
      } catch { /* HTTP status is still actionable; no trustworthy commit outcome was supplied */ }
      throw new TransferError('HTTP_ERROR', message,
        { status: response.status, uploadOutcome: rejected ? 'rejected' : 'unknown' });
    }
    if (!/^application\/json(?:\s*;|$)/i.test(response.headers.get('content-type') ?? '')) {
      void response.body?.cancel().catch(() => {});
      throw new TransferError('PROTOCOL_ERROR', 'Commit response must have application/json Content-Type', { uploadOutcome: 'unknown' });
    }
    let commit: unknown;
    try { commit = await metadataJson(response, controller.signal); }
    catch (cause) {
      if (controller.signal.aborted) throw controller.signal.reason;
      throw new TransferError('PROTOCOL_ERROR', 'Invalid or oversized commit JSON', { cause, uploadOutcome: 'unknown' });
    }
    if (!ended || !object(commit) || commit.committed !== true || commit.key !== options.key
        || typeof commit.bytes !== 'number' || !Number.isSafeInteger(commit.bytes)
        || commit.bytes < 0 || commit.bytes !== bytesRead
        || typeof commit.etag !== 'string' || !commit.etag.trim()) {
      throw new TransferError('PROTOCOL_ERROR', 'Commit does not match the completed source/key/byte count', { uploadOutcome: 'unknown' });
    }
    const result: UploadCommit = Object.freeze({ committed: true, key: options.key, bytes: commit.bytes, etag: commit.etag });
    notify(options.onProgress, { phase: 'committed', bytesRead, commit: result } as UploadProgress);
    return result;
  } catch (cause) {
    if (sourceFailure) throw sourceFailure;
    if (controller.signal.aborted) throw controller.signal.reason;
    if (cause instanceof TransferError) throw cause;
    throw new TransferError('NETWORK_ERROR', 'Streaming upload failed; transport support or commit outcome may be unknown',
      { cause, uploadOutcome: outcome() });
  } finally {
    options.signal?.removeEventListener('abort', abort);
    if (!ended) {
      controller.abort();
      cancelSource(controller.signal.reason);
      try { bodyController?.error(controller.signal.reason); } catch { /* already settled */ }
    }
  }
}
