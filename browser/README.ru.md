# Браузерные потоки s3-file-streams

**0.1.0 · TypeScript / ES modules · Web Streams + Fetch · MIT**

[English](README.md) · Русский

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
