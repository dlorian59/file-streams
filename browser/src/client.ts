import { uploadStream, type UploadOptions } from './upload.js';
import { downloadTo, type DownloadOptions } from './download.js';
import { byteCount } from './internal.js';
import { objectUrl } from './url.js';

/** Per-object bounds, shared with Java StreamProfile and Spring limits.profile. */
export const fileProfiles = Object.freeze({
  attachments: Object.freeze({ maxBytes: 64 * 1024 * 1024 }),
  media: Object.freeze({ maxBytes: 512 * 1024 * 1024 }),
  archives: Object.freeze({ maxBytes: 16 * 1024 * 1024 * 1024 }),
});
export type FileProfile = keyof typeof fileProfiles;
export interface FileStreamsOptions {
  readonly objectsUrl: string | URL;
  readonly profile?: FileProfile;
  readonly maxBytes?: number;
  readonly headers?: HeadersInit;
  readonly credentials?: RequestCredentials;
  readonly fetch?: typeof globalThis.fetch;
}
export type StreamUploadOptions = Pick<UploadOptions, 'knownLength' | 'signal' | 'headers' | 'onProgress'>;
export type FileUploadOptions = Omit<StreamUploadOptions, 'knownLength'>;
export type StreamDownloadOptions = Pick<DownloadOptions, 'range' | 'signal' | 'headers' | 'onProgress'>;

/** Configure one endpoint; every operation still uses the existing one-shot streaming engine. */
export function createFileStreams(options: FileStreamsOptions) {
  const profile = options.profile ?? 'attachments';
  if (!Object.hasOwn(fileProfiles, profile)) throw new TypeError('Unknown file profile');
  const maxBytes = options.maxBytes ?? fileProfiles[profile].maxBytes;
  byteCount(maxBytes, 'maxBytes');
  const base = new URL(options.objectsUrl, globalThis.location?.href);
  objectUrl(base, 'validate');
  const defaults = new Headers(options.headers);
  const headersFor = (headers?: HeadersInit) => {
    const merged = new Headers(defaults);
    new Headers(headers).forEach((value, key) => merged.set(key, value));
    return merged;
  };
  const transport = { maxBytes, ...(options.credentials === undefined ? {} : { credentials: options.credentials }),
    ...(options.fetch === undefined ? {} : { fetch: options.fetch }) };
  return Object.freeze({
    uploadFile(key: string, file: Blob, operation: FileUploadOptions = {}) {
      const headers = headersFor(operation.headers);
      if (file.type && !headers.has('content-type')) headers.set('content-type', file.type);
      return uploadStream({ ...operation, ...transport, headers, key, url: objectUrl(base, key),
        source: file.stream(), knownLength: file.size });
    },
    upload(key: string, source: ReadableStream<Uint8Array>, operation: StreamUploadOptions = {}) {
      return uploadStream({ ...operation, ...transport, headers: headersFor(operation.headers),
        key, url: objectUrl(base, key), source });
    },
    download(key: string, destination: WritableStream<Uint8Array>, operation: StreamDownloadOptions = {}) {
      return downloadTo({ ...operation, ...transport, headers: headersFor(operation.headers),
        url: objectUrl(base, key), destination });
    },
  });
}
