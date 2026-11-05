import { createFileStreams, type FileUploadOptions, type StreamDownloadOptions } from '@59alex/file-streams';

// EN: Call once with the existing application's endpoint and transport; do not install a second auth layer.
// RU: Один раз передайте endpoint и транспорт приложения; второй слой аутентификации не нужен.
export function attachmentFiles(objectsUrl: URL, authenticatedFetch: typeof fetch = fetch) {
  const files = createFileStreams({ objectsUrl, profile: 'attachments', fetch: authenticatedFetch });
  return {
    async save(accountId: string, file: File, options: FileUploadOptions = {}) {
      // EN: The server verifies this key against its authenticated principal, not a client-supplied account header.
      // RU: Сервер сверяет ключ со своим principal, а не с account-header клиента.
      const key = `${accountId}:${file.name}`;
      // EN: File.size and File.type are passed automatically; success means a validated storage commit.
      // RU: File.size и File.type передаются автоматически; успех означает проверенный commit хранилища.
      return files.uploadFile(key, file, options);
    },
    async saveToDisk(key: string, destination: WritableStream<Uint8Array>, options: StreamDownloadOptions = {}) {
      // EN: Writes await the destination; no complete Blob or application multipart scheduler is created.
      // RU: Запись ждёт destination; полный Blob и прикладной multipart-планировщик не создаются.
      return files.download(key, destination, options);
    },
  };
}
