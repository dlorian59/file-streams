import { uploadStream, downloadTo, objectUrl } from '@59alex/file-streams';

const objects = 'https://localhost:19443/objects';

// EN: A File is already a streaming source; progress counts source bytes, while completion confirms the server commit.
// RU: File уже предоставляет поток; прогресс считает байты источника, а завершение подтверждает сохранение сервером.
export async function sendFile(file: File, key: string, signal: AbortSignal) {
  return uploadStream({
    url: objectUrl(objects, key), key, source: file.stream(), knownLength: file.size,
    maxBytes: 512 * 1024 * 1024, signal,
    onProgress(progress) {
      console.log(progress.phase === 'reading' ? progress.bytesRead : progress.commit.etag);
    },
  });
}

// EN: The caller provides the destination, such as a file-system writable stream; bytes are written incrementally.
// RU: Приложение передаёт приёмник, например файловый writable stream; данные записываются постепенно.
export async function saveFile(key: string, destination: WritableStream<Uint8Array>, signal: AbortSignal) {
  return downloadTo({ url: objectUrl(objects, key), destination, maxBytes: 512 * 1024 * 1024, signal });
}
