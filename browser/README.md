# @59alex/s3-file-streams — browser

<details name="readme-language">
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-en-dark.svg"><img src="assets/lang-en-light.svg" width="42" height="26" alt="EN"></picture></summary>

[← Back to repository README / Назад к README библиотеки](https://github.com/59Alex/s3-file-streams/blob/main/README.md)

Version **0.1.0** · TypeScript / ES modules · Web Streams + Fetch · MIT


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

[← Back to repository README / Назад к README библиотеки](https://github.com/59Alex/s3-file-streams/blob/main/README.md)

</details>

<details name="readme-language" open>
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-ru-dark.svg"><img src="assets/lang-ru-light.svg" width="42" height="26" alt="RU"></picture></summary>

[← Back to repository README / Назад к README библиотеки](https://github.com/59Alex/s3-file-streams/blob/main/README.md)

**0.1.0 · TypeScript / ES modules · Web Streams + Fetch · MIT**


Библиотека передаёт один непрерывный поток из браузера через HTTP-приложение в хранилище. При скачивании данные идут в WritableStream, который предоставляет приложение. Файл целиком не собирается в Blob; отдельных клиентских multipart-сессий нет. Ядро не зависит от React и не содержит runtime-зависимостей.

Пакет собран локально. В npm он ещё не опубликован: доступ к namespace и учётные данные для публикации требуют проверки.

## Сборка и установка

Для инструментов нужен Node 20+. Локальные проверки выполнены на Node 24 и TypeScript 7. Поддержка браузера зависит от Web API и HTTP-транспорта, описанных ниже.

```sh
npm ci
npm test
npm run test:package
npm pack
# Выполнить в проекте-потребителе, указав путь к созданному архиву:
npm install ./59alex-s3-file-streams-0.1.0.tgz
```

Пакет экспортирует ES-модули и TypeScript declarations. В tsconfig приложения нужна библиотека DOM.

```ts
import { uploadStream, downloadTo, objectUrl, streamCapabilities, TransferError }
  from '@59alex/s3-file-streams';
```

## Возможности

| Возможность | Что гарантирует API |
|---|---|
| Загрузка | Один сырой PUT из ReadableStream<Uint8Array>, с известной или неизвестной общей длиной. |
| Подтверждение | Успех только после HTTP 201 и проверки JSON с ключом, числом прочитанных байтов и непустым ETag. |
| Скачивание | GET → WritableStream; следующая порция читается после завершения предыдущей записи. |
| Диапазоны | Один закрытый, открытый или суффиксный byte range с проверкой HTTP 206 и заголовков. |
| Отмена | AbortSignal отменяет fetch и принадлежащие операции потоки. Уже случившийся удалённый commit или запись в sink не откатываются автоматически. |
| Ограничения | Обязательный maxBytes, необязательный knownLength, безопасные целые счётчики, максимум 16 КиБ JSON-метаданных. |
| Прогресс | Отдельно чтение исходного потока и подтверждённый commit. При скачивании — байты, принятые завершившимися записями sink. |

## 1. Проверить браузер и реальный HTTP-путь

```ts
const capabilities = streamCapabilities();
if (!capabilities.requestStreamSyntax) {
  throw new Error('Браузер не поддерживает потоковое тело запроса');
}
console.log(capabilities.transportVerified); // всегда false
```

Проверка создаёт Request, но не отправляет данные. Она не проверяет HTTP-протокол, прокси, сервер и хранилище. При неподдерживаемом синтаксисе uploadStream выдаёт UNSUPPORTED до чтения исходного потока. Полного буферизующего обходного пути нет.

В Chromium потоковый fetch требует duplex: 'half' и HTTP/2 либо HTTP/3. HTTP/1 отклоняется даже после успешного конструктора Request. Ответ становится доступен только после отправки тела. Между разными origin нужен CORS preflight; библиотека отключает редиректы, поскольку одноразовый поток нельзя безопасно отправить заново. Эти ограничения описаны в [документации Chrome](https://developer.chrome.com/docs/capabilities/web-apis/fetch-streaming-requests).

Настройте сервер и прокси так, чтобы они передавали тело по мере получения. Счётчик чтения в браузере не доказывает, что сервер уже получил данные. Локальная транспортная проверка проверяет это отдельным запросом до закрытия исходного потока.

## 2. Загрузить файл

```ts
const file = document.querySelector<HTMLInputElement>('#file')!.files![0]!;
const key = 'uploads/report.bin';
const stop = new AbortController();
const commit = await uploadStream({
  url: objectUrl('https://files.example.test/objects', key),
  key, source: file.stream(), knownLength: file.size,
  maxBytes: 512 * 1024 * 1024, signal: stop.signal,
  headers: { 'Content-Type': file.type || 'application/octet-stream' },
  onProgress(event) {
    if (event.phase === 'reading') console.log('Прочитано:', event.bytesRead);
    else console.log('Commit подтверждён:', event.commit);
  },
});
console.log(commit.key, commit.bytes, commit.etag);
```

Ожидаемый результат: Promise возвращает проверенный commit после окончания исходного потока и ответа сервера. ETag — непрозрачный идентификатор, не обещание MD5. Фаза reading означает только чтение источника, а не получение байтов сетью или S3. Ошибки наблюдателя прогресса игнорируются; его асинхронное выполнение не задерживает передачу.

knownLength передаётся как X-Upload-Length: браузер не позволяет задать Content-Length вручную. Источник короче или длиннее knownLength завершается LENGTH_MISMATCH. Размеры должны быть неотрицательными безопасными целыми числами не больше Number.MAX_SAFE_INTEGER.

## 3. Передать поток неизвестной длины

```ts
let remaining = 4;
const source = new ReadableStream<Uint8Array>({
  pull(controller) {
    if (remaining-- > 0) controller.enqueue(new Uint8Array(64 * 1024));
    else controller.close();
  },
}, { highWaterMark: 0 });
const key = 'generated.bin';
const commit = await uploadStream({
  url: objectUrl('https://files.example.test/objects', key),
  key, source, maxBytes: 256 * 1024,
});
console.log(commit.bytes); // 262144 при успешном commit
```

Если убрать knownLength, библиотека считает байты до EOF и всё равно соблюдает maxBytes. Превышение лимита отменяет источник и fetch. Память источника зависит от размера выдаваемых им порций: не создавайте целый файл одним огромным Uint8Array. Внутренними буферами браузера и сервера библиотека не управляет.

## 4. Отменить операцию и обработать неопределённый исход

```ts
// Для операции из примера загрузки:
stop.abort('Пользователь отменил загрузку');
```

| uploadOutcome ошибки TransferError | Значение |
|---|---|
| not-started | Запрос не был запущен: например, неверные аргументы или неподдерживаемый API. |
| rejected | Сервер явно сообщил committed:false. По контракту это отказ в проверке до commit. |
| unknown | Запрос мог сохраниться на сервере, но подтверждения нет: отмена, разрыв связи, неверный ответ, committed:null. |

Автоматических повторов нет. Для повторной попытки приложение создаёт новый исходный поток и решает, как проверить или перезаписать предыдущий объект. Отмена не является подтверждением удаления объекта. При ошибках финализации сервер может вернуть committed:null; это тоже unknown.

## 5. Скачать в поток приложения

```ts
// destination предоставляет приложение: например, handle.createWritable()
// после поддерживаемого браузером showSaveFilePicker().
const result = await downloadTo({
  url: objectUrl('https://files.example.test/objects', 'uploads/report.bin'),
  destination,
  maxBytes: 512 * 1024 * 1024,
  onProgress(event) { console.log('Записано:', event.bytesWritten); },
});
console.log(result.complete, result.bytesWritten, result.etag);
```

Каждый writer.write ожидается перед следующим чтением. Успех возвращается после проверки длины и завершения close у sink. Это не гарантия физического fsync. Ошибка sink отменяет ответ и fetch. При отмене или ошибке destination получает abort; решение о сохранении уже записанных байтов остаётся за реализацией sink.

Отмена возвращается быстро даже при зависшем writer.write. Такая незавершённая запись ещё может иметь побочный эффект; её abort может закончиться позже. Выбирайте sink с подходящей вашему приложению семантикой очистки.

## 6. Скачать один диапазон

```ts
const result = await downloadTo({
  url: objectUrl('https://files.example.test/objects', 'uploads/report.bin'),
  destination, range: { start: 0, end: 1023 }, maxBytes: 1024,
});
console.log(result.range); // start, end включительно; total — размер объекта
```

Другие формы: { start: 1024 } и { suffix: 1024 }. Ответ должен иметь статус 206 и согласованные Content-Range/Content-Length. Если сервер игнорирует Range и возвращает 200, данные не записываются. HTTP 416 остаётся HTTP_ERROR.

Библиотека не перемещает позицию файла, не собирает несколько диапазонов и не реализует возобновление скачивания. При сборке диапазонов приложение должно проверить, что версия объекта между запросами не изменилась.

## 7. HTTP-контракт и владение ресурсами

- PUT /objects/{encodedKey}: непрерывное тело, необязательные Content-Type и X-Upload-Length. Успех — только HTTP 201 application/json с { committed: true, key, bytes, etag }.
- GET: 200 для полного объекта или 206 для запрошенного диапазона. Нужны видимые Content-Length, ETag и Accept-Ranges: bytes; для диапазона также Content-Range.
- Content-Encoding кроме identity отклоняется: автоматическая распаковка нарушает учёт исходных байтов. Отключите сжатие ответов для этого endpoint.
- CORS: разрешите PUT/GET, Content-Type, X-Upload-Length и Range; откройте чтение Content-Length, ETag, Accept-Ranges, Content-Range, Content-Type и Content-Encoding. Политику origin и credentials выбирает приложение.

credentials по умолчанию same-origin. Authorization можно передать через headers. Необязательная реализация fetch должна сохранять потоковую передачу и отмену.

Проверка аргументов не захватывает потоки. Затем операция владеет reader/writer до завершения и снимает блокировку. Успех скачивания закрывает destination; ошибка вызывает abort. Не читайте исходный поток параллельно и не меняйте уже переданные байтовые буферы.

## 8. Проверки и демонстрация

```sh
npm test
npm run test:package
npm run demo
```

Откройте https://127.0.0.1:9443 в тестовом браузере. Run browser checks проверяет локальный HTTP/2 fixture, который держит максимум 8 МиБ в памяти. Это не S3. OpenSSL создаёт временный самоподписанный сертификат; ключ удаляется при остановке процесса. Исключение доверия сертификату применяется только для локального теста.

Для Java запустите TLS demo из репозитория с отдельным тестовым S3 backend. Укажите https://127.0.0.1:19443/objects в поле endpoint и нажмите Run Java adapter checks. CORS сервера должен разрешать origin https://127.0.0.1:9443. [Сценарии](demo/java-integration-checks.js) создают уникальные объекты известных 6 МиБ + 17 байт, неизвестных 1 МиБ + 13 байт и пустой; сверяют SHA-256, диапазоны, отсутствующий объект и отмену. Для отменённого ключа отдельно проверяется отсутствие объекта и незавершённых multipart-загрузок на backend. Полные небольшие буферы здесь нужны только для тестового хеширования.

| Среда | Проверено |
|---|---|
| Node 24 / Web Streams | 27 тестов: ограничения, ошибки, отмена, протокол, диапазоны. Обе функции считают 2 ГиБ + 17 байт с одним повторно используемым буфером 64 КиБ; это controlled transport, не передача такого объёма по сети или в хранилище. |
| Chromium 156 / HTTP/2 TLS fixture | 10 проверок; сервер видит данные до EOF, содержимое и диапазон совпадают; проверены отмена, разрыв, ошибочный commit, лимит и ошибка sink. |
| Chromium / HTTP/1 TLS | Потоковый PUT отклонён после успешной проверки Request. |
| Firefox 156 | Upload выдаёт UNSUPPORTED без чтения и блокировки source; двухбайтовый диапазон скачивается корректно. |
| Chromium 156 → Java HTTP/2 TLS → Garage S3 | 7 сценариев: SHA-256 для 6 291 473 известных байтов, 1 048 589 неизвестных байтов и пустого объекта; диапазон 53 байта через границу 5 МиБ и суффикс 31 байт; 404 без записи; отмена источника после чтения 6 МиБ с исходом unknown. |
| Чистый потребитель npm-архива | Экспорты, declarations, пустые upload/download; без runtime-зависимостей. |
| HTTP/3, Safari, production CDN/proxy | Не проверены. |

Для проверенного сценария отмены отдельная backend-проверка подтвердила HEAD 404 и отсутствие активных multipart-загрузок по этому ключу. Это подтверждает очистку в данном сценарии, но не даёт общей гарантии отката при отмене. Успешные синтетические тестовые объекты после проверки удалены.

[Транспортные сценарии](demo/browser-checks.js) сохраняют результат в window.__streamResults; Java-сценарии — в window.__javaStreamResults. Node unit-тесты сами по себе не проверяют S3. Актуальные данные о Java/backend приведены в документации корневого репозитория.

## Лицензия

[MIT](LICENSE), © 2026 Aleksandr Kashevarov.

[← Back to repository README / Назад к README библиотеки](https://github.com/59Alex/s3-file-streams/blob/main/README.md)

</details>
