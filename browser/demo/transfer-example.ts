import { fileProfiles, uploadStream, downloadTo, objectUrl, streamCapabilities, TransferError, type ByteRange } from '@59alex/file-streams';

const objects = 'https://localhost:19443/objects';

// EN: A File is already a streaming source; progress counts source bytes, while completion confirms the server commit.
// RU: File уже предоставляет поток; прогресс считает байты источника, а завершение подтверждает сохранение сервером.
export async function sendFile(file: File, key: string, signal: AbortSignal) {
  return uploadStream({
    url: objectUrl(objects, key), key, source: file.stream(), knownLength: file.size,
    maxBytes: fileProfiles.media.maxBytes, signal,
    onProgress(progress) {
      console.log(progress.phase === 'reading' ? progress.bytesRead : progress.commit.etag);
    },
  });
}

// EN: The caller provides the destination, such as a file-system writable stream; bytes are written incrementally.
// RU: Приложение передаёт приёмник, например файловый writable stream; данные записываются постепенно.
export async function saveFile(key: string, destination: WritableStream<Uint8Array>, signal: AbortSignal) {
  return downloadTo({ url: objectUrl(objects, key), destination, maxBytes: fileProfiles.media.maxBytes, signal });
}

// EN: The probe checks API syntax only; it never sends a request or certifies your proxy path.
// RU: Проба проверяет только синтаксис API; она не отправляет запрос и не подтверждает работу прокси.
export function inspectSupport() {
  return streamCapabilities();
}

// EN: Closed, open-ended and suffix ranges use the same sink contract; the destination must be fresh and unlocked.
// RU: Закрытый, открытый и суффиксный диапазоны используют тот же приёмник; он должен быть новым и незаблокированным.
export function saveRange(key: string, range: ByteRange, destination: WritableStream<Uint8Array>, signal: AbortSignal) {
  return downloadTo({ url: objectUrl(objects, key), destination, range,
    maxBytes: fileProfiles.attachments.maxBytes, signal, onProgress: progress => console.log(progress.bytesWritten, progress.responseBytes) });
}

// EN: Additional headers and credentials are application policy; CORS must allow them on cross-origin requests.
// RU: Дополнительные заголовки и credentials определяет приложение; при cross-origin их должен разрешать CORS.
export function sendAuthorizedFile(file: File, key: string, authorization: string, signal: AbortSignal) {
  return uploadStream({ url: objectUrl(objects, key), key, source: file.stream(), knownLength: file.size,
    maxBytes: fileProfiles.media.maxBytes, signal, headers: { Authorization: authorization }, credentials: 'include' });
}

// EN: Report the remote outcome separately from the local cause; unknown never means that storage is empty.
// RU: Отдельно показываем удалённый исход и локальную причину; unknown не означает отсутствие объекта.
export function describeFailure(error: unknown): string {
  if (error instanceof TransferError) {
    return `${error.code}: ${error.message}; HTTP=${error.status ?? 'none'}; outcome=${error.uploadOutcome ?? 'download'}`;
  }
  return error instanceof Error ? error.message : String(error);
}

// EN: Omit knownLength only for a genuinely unknown total; custom Fetch must preserve streaming and cancellation.
// RU: Не задавайте knownLength только при неизвестном итоге; свой Fetch обязан сохранить streaming и отмену.
export function sendGenerated(key: string, source: ReadableStream<Uint8Array>, signal: AbortSignal,
  transport: typeof globalThis.fetch = globalThis.fetch) {
  return uploadStream({ url: objectUrl(objects, key), key, source, maxBytes: fileProfiles.attachments.maxBytes, signal, fetch: transport });
}
