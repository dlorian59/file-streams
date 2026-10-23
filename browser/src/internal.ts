import { TransferError } from './errors.js';

export function byteCount(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new TransferError('INVALID_ARGUMENT', `${name} must be a non-negative safe integer`);
  return value;
}

/** Subtraction before addition avoids rounding counters beyond the safe integer boundary. */
export function checkedAdd(current: number, increment: number, limit: number): number {
  if (increment > limit - current)
    throw new TransferError('LIMIT_EXCEEDED', `Transfer exceeds maxBytes (${limit})`);
  return current + increment;
}

export function decimal(value: string | null, name: string): number {
  if (value === null || !/^\d+$/.test(value))
    throw new TransferError('PROTOCOL_ERROR', `${name} must be a visible decimal byte count`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0)
    throw new TransferError('PROTOCOL_ERROR', `${name} exceeds JavaScript's safe integer range`);
  return parsed;
}

export function notify<T>(observer: ((progress: T) => void) | undefined, progress: T): void {
  // Observers cannot change the transfer or turn an observed commit into a reported failure.
  try {
    const result: unknown = observer?.(Object.freeze(progress));
    if (result && typeof (result as PromiseLike<unknown>).then === 'function')
      void Promise.resolve(result).catch(() => {});
  } catch { /* observational callback */ }
}

export function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) {
    // A promise may already represent a pending write. Observe its later failure as well.
    void promise.catch(() => {});
    return Promise.reject(signal.reason);
  }
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}

/** Only small protocol metadata is materialized, never a transfer body. */
export async function metadataJson(response: Response, signal: AbortSignal): Promise<unknown> {
  if (!response.body) throw new TransferError('PROTOCOL_ERROR', 'Missing response metadata body');
  const reader = response.body.getReader();
  let bytes = 0;
  let result = '';
  const decoder = new TextDecoder('utf-8', { fatal: true });
  try {
    while (true) {
      const next = await abortable(reader.read(), signal);
      if (next.done) break;
      bytes += next.value.byteLength;
      if (bytes > 16_384) throw new TransferError('PROTOCOL_ERROR', 'Response metadata exceeds 16 KiB');
      result += decoder.decode(next.value, { stream: true });
    }
    result += decoder.decode();
    return JSON.parse(result) as unknown;
  } finally {
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export function object(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
