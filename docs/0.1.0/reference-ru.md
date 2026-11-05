# 0.1.0 — полный справочник API и возможностей

[Configured integration / Готовое подключение](integration-ru.md)

[← Руководство](ru.md) · [Версии](../index.md) · [README](../../README.md) · [EN](reference-en.md)

[Карта возможностей](#карта-возможностей) · [Java API](#java-api) · [Профиль S3](#профиль-s3) · [Browser API](#browser-api) · [Ошибки и HTTP](#ошибки-и-http) · [Ресурсы и AOP](#ресурсы-и-aop) · [Диагностика](#диагностика)

Справочник охватывает публичный API core, S3, WebFlux и browser. Установка и первая передача описаны в [руководстве](ru.md). Нужны JDK 17+, для браузерных инструментов — Node 20+ и TypeScript с DOM types. Реальная browser upload требует поддержки Fetch request streams и проверенного маршрута HTTP/2 с TLS либо HTTP/3. Доступ к хранилищу, bucket и credentials нужны только S3-примерам. [Отчёт проверок](../VERIFICATION.md) отделяет новые запуски от исторических измерений.

## Карта возможностей

| Задача | API и исполняемый/компилируемый пример | Граница |
| --- | --- | --- |
| Проверить байты и объявленную длину | `CheckedStream`, `StreamLimits`; [core consumer](../../examples/consumer/src/main/java/CoreCapabilities.java) | Одна подписка, ограничение порции источника; это не storage commit. |
| Передать ключ, MIME и metadata | `UploadRequest`; [core consumer](../../examples/consumer/src/main/java/CoreCapabilities.java) | HTTP намеренно не принимает произвольные metadata. |
| Принять HTTP-поток и подтвердить запись | `ObjectStreams.upload`; [Java receive/receiveBefore](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java) ↔ [TypeScript sendFile](../../browser/demo/transfer-example.ts) | Прогресс отличается от commit; timeout не означает откат. |
| Скачать объект или диапазон | `ObjectDownload`, `ByteRange`; [Java countDownload](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java) ↔ [TypeScript saveFile/saveRange](../../browser/demo/transfer-example.ts) | Тело нужно прочитать или закрыть; автоматического resume нет. |
| Настроить ограниченный AWS multipart | `S3Clients`, `S3ObjectStreams`; [настройка приложения](../../demo/src/main/java/io/github/alex59/filestreams/demo/DemoServer.java) | Multipart скрыт внутри; клиентом владеет приложение. |
| Подключить HTTP PUT/GET | `ObjectStreamHandler`; [исходник](../../webflux/src/main/java/io/github/alex59/filestreams/webflux/ObjectStreamHandler.java) | Фиксированный `/objects/**`; безопасность, CORS и ключи задаёт host. |
| Передать неизвестный объём или свой transport | [sendGenerated](../../browser/demo/transfer-example.ts) | maxBytes обязателен; свой Fetch сохраняет streaming/отмену. |
| Проверить browser API и вывести ошибку | `streamCapabilities`, `TransferError`; [inspectSupport/describeFailure](../../browser/demo/transfer-example.ts) | Проверка синтаксиса не подтверждает транспорт. |
| Передать Authorization или cookies | `headers`, `credentials`; [sendAuthorizedFile](../../browser/demo/transfer-example.ts) | CORS приложения должен разрешать Authorization и credentials; локальный demo по умолчанию этого не делает. |

После [локальной сборки](ru.md#1-подключение-модулей) выполните из корня:

```bash
./gradlew publishToMavenLocal -Dmaven.repo.local="$PWD/build/local-repository"
./gradlew -p examples/consumer run
./gradlew :demo:compileJava
cd browser
npm ci
npm test
```

Consumer выводит `Core examples passed: metadata, checked bytes, ranges, cleanup, typed failure` и `Published-artifact consumer passed: core + S3 + WebFlux`. `npm test` также компилирует TypeScript-пример с комментариями. Браузерные функции вызываются приложением с настоящими File, AbortSignal и writable destination; это не CLI-команды Node.

## Java API

Пакет — `io.github.alex59.filestreams`, адаптеры находятся в `.s3` и `.webflux`. Records предоставляют accessors своих полей и стандартное равенство records, без изменяемых setters.

| Публичный API | Контракт |
| --- | --- |
| `ObjectStreams.upload(UploadRequest, Publisher<ByteBuffer>) → Mono<UploadResult>` | Работа адаптера начинается при подписке. Успех требует проверенного EOF и подтверждения хранилища. Новая передача получает новый источник; retry/repeat для уже прочитанного тела не подходит. |
| `ObjectStreams.download(String key, String range) → Mono<ObjectDownload>` | `null` означает полный объект, иначе строгий одиночный HTTP byte range. Возвращённый download владеет живым телом/ресурсом. |
| `UploadRequest(key, length, contentType, metadata)` | Все аргументы non-null. Ключ занимает 1–1024 байта UTF-8 без NUL. `OptionalLong.empty()` — неизвестная длина; заданная длина неотрицательна. MIME непустой, без CR/LF. Metadata копируется через `Map.copyOf`, null-элементы запрещены. Ограничения metadata самого S3 продолжают действовать. |
| `UploadRequest.unknownLength(key)` | Неизвестная длина, `application/octet-stream`, пустая metadata. |
| `UploadResult(committed, key, bytes, etag)` | Принимает только committed=true, неотрицательные bytes, non-null key и непустой ETag. Это подтверждённый commit, не состояние ошибки. ETag непрозрачен. |
| `StreamLimits(maxBytes, maxBufferBytes)` / `defaults()` | Неотрицательный long maxBytes, положительный int maxBufferBytes. По умолчанию 16 GiB / 1 MiB. В `S3ObjectStreams` это ограничения **upload**, а не размера уже существующего скачиваемого объекта. |
| `CheckedStream(source, expectedLength, limits)` | Проверяет аргументы и объявленный размер до чтения. Запрашивает по одному буферу. Возвращает read-only view, сохраняя позицию исходного ByteBuffer; underlying bytes нельзя менять до завершения передачи. |
| `CheckedStream.content()` | Одноразовый Flux с проверкой порций, суммы и EOF. Повторная подписка даёт `SOURCE_REUSED`. Целый объект не накапливается. |
| `CheckedStream.bytes()` / `completed()` | Проверенное число байтов long; completed означает корректный EOF источника, **не** storage commit. |
| `CheckedStream.cancel()` | Отменяет источник либо запоминает отмену до подписки; активное чтение получает `CANCELLED`. Объекты хранилища не удаляет. |
| `ObjectDownload(length, contentType, etag, range, source, cleanup)` | Length неотрицателен; type, ETag, source, cleanup non-null. Если range задан, его длина совпадает с body length. Конструктор полезен адаптерам и собственным хранилищам. |
| `ObjectDownload.length/contentType/etag/range()` | Объявленные metadata; для полного ответа range=null. Фактическая длина проверяется при чтении отдельно. |
| `ObjectDownload.body()` / `close()` | Одна подписка; повторное чтение/доступ после close завершается ошибкой. Завершение, ошибка, downstream cancel и close запускают cleanup ровно один раз. Активный close сообщает `CANCELLED`; до чтения он отменяет ещё не открытый источник. |
| `ByteRange(start, end, total)` | Включительные индексы: `0 ≤ start ≤ end < total`. `length()` = end-start+1; три поля доступны через accessors. |
| `ByteRange.parse(header, total)` | `bytes=a-b`, `bytes=a-`, `bytes=-suffix`; слишком большой end/suffix обрезается. Не принимает несколько ranges, пустой объект, нулевой suffix, неверные/переполненные числа и заголовки длиннее 100 символов. |
| `ByteRange.requestHeader()` / `responseHeader()` | Формируют `bytes=a-b` и `bytes a-b/total`. |
| `ByteRange.RangeNotSatisfiable(total)` / `total()` | Типизированная ошибка; отрицательный total означает неизвестный размер, поэтому `Content-Range: bytes */total` не формируется. |
| `StreamFailure(code, message)` / `code()` | Ошибка источника/admission. Коды: `TOO_LARGE`, `LENGTH_MISMATCH`, `BUFFER_TOO_LARGE`, `CANCELLED`, `SOURCE_REUSED`, `BUSY`. |
| `ObjectUnavailable(reason, cause)` / `reason()` | `NOT_FOUND` — объекта нет; `CHANGED` — версия изменилась между HEAD и условным GET. Причина сохраняется. |

[Исходники core](../../core/src/main/java/io/github/alex59/filestreams) · [пример времени жизни download](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java).

## Профиль S3

`S3Clients.bounded(URI endpoint, Region region, AwsCredentialsProvider credentials)` возвращает S3AsyncClient приложения; endpoint=null использует AWS. Region и credentials задаёт приложение. `new S3ObjectStreams(client, bucket, limits, maxTransfers)` требует non-null client/bucket/limits и положительное число слотов; клиент не закрывает. За пределами Boot starter приложению также принадлежит жизненный цикл credentials provider.

| Фиксированная настройка / публичная константа | Значение и смысл |
| --- | --- |
| `PART_BYTES` | 5 242 880 байт (5 MiB), внутренний размер части/порог S3. |
| `SDK_BUFFER_BYTES` | 10 485 760 байт (10 MiB), настройка буфера API SDK; не лимит RSS процесса. |
| Одновременные части | 1 на upload. |
| Максимум настроенного upload | 10 000 × 5 MiB = 52 428 800 000 байт. Даже известная длина передаётся SDK как unknown-length body ради проверки полного EOF и лишних байтов. Это граница профиля, не общий максимум сервиса S3. |
| Admission | Общий semaphore для upload/download. Лишний запрос получает BUSY, не становится в неограниченную очередь. Upload удерживает слот до завершения SDK future, включая очистку после отмены. Download — до завершения/закрытия тела. |
| HTTP connections / pending acquisition | 8 / 16. |
| Connection / read / write timeout | 5 с / 60 с / 60 с. |
| SDK attempt / API call timeout | 60 с / 10 мин. Не заменяют deadline приложения при бесконечно зависшем источнике. |
| Retry | Одна попытка; прозрачного replay нет. |
| Endpoint / proxy | Path-style addressing; наследование HTTP-прокси из окружения/system properties отключено. Для явного прокси или иного профиля передайте совместимый клиент. |
| Checksums | Политики AWS request/response `WHEN_REQUIRED`; ETag при этом не становится проверкой целостности приложения. |

Для полного скачивания выполняются HEAD и If-Match GET с явным полным диапазоном, чтобы SDK не менял metadata через multipart download. До выдачи тела проверяются длина, ETag, диапазон и identity encoding; при чтении — фактическая длина. `limits.maxBytes/maxBufferBytes` не задают политику размера download. При необходимости ограничьте его в host-приложении, а browser maxBytes задавайте отдельно.

Локальная отмена Java-подписки до EOF передаёт ошибку источника, оставляя SDK future работать ради abort path. Очистка best effort: падение процесса, сеть или запрет AbortMultipartUpload могут оставить части; настройте lifecycle. Завершение HTTP может быть представлено обычным EOF, и при неизвестной длине backend способен принять полученный префикс. Отменённый browser promise остаётся unknown. Передавайте известную длину и сверяйте неоднозначный результат перед новой передачей. [Архитектура](../ARCHITECTURE.md) · [настройки SDK](../../s3/src/main/java/io/github/alex59/filestreams/s3/S3Clients.java).

## Browser API

Публичные экспорты перечислены в [index.ts](../../browser/src/index.ts). Framework dependency во время исполнения не требуется.

### Параметры

| Поле | UploadOptions | DownloadOptions | Значение / default |
| --- | --- | --- | --- |
| `url: string \| URL` | обязательно | обязательно | Адрес HTTP-адаптера; objectUrl формирует его безопасно. |
| `key: string` | обязательно | — | Непустой логический ключ; commit должен совпасть с ним точно. |
| `source: ReadableStream<Uint8Array>` | обязательно | — | Незаблокированный одноразовый источник байтов. |
| `destination: WritableStream<Uint8Array>` | — | обязательно | Незаблокированный приёмник; операция владеет writer, закрывает при успехе и abort при ошибке. |
| `maxBytes: number` | обязательно | обязательно | Неотрицательное безопасное целое, включая 0. Upload проверяет каждую порцию до передачи; download — metadata до записи и фактические байты при чтении. |
| `knownLength?: number` | необязательно | — | Безопасное целое от 0 до maxBytes; локальная проверка и X-Upload-Length. Для File передавайте file.size. |
| `range?: ByteRange` | — | необязательно | `{start,end?}` либо `{suffix}`. start/end — неотрицательные безопасные целые, end ≥ start, suffix > 0. Формы не смешиваются. |
| `signal?: AbortSignal` | необязательно | необязательно | Отменяет транспорт и принадлежащие операции потоки. Уже отменённый signal не запускает Fetch. Записанные данные/commit не откатываются. |
| `headers?: HeadersInit` | необязательно | необязательно | Заголовки приложения. Upload запрещает Content-Length, X-Upload-Length, Content-Range; download запрещает Range (используйте option). Content-Type upload по умолчанию application/octet-stream. |
| `credentials?: RequestCredentials` | необязательно | необязательно | По умолчанию same-origin. Для cross-origin cookies нужны соответствующие CORS/host policies. |
| `onProgress?` | callback UploadProgress | callback DownloadProgress | Наблюдение: исключения и отклонённые callback-promises игнорируются и не меняют передачу. |
| `fetch?: typeof fetch` | необязательно | необязательно | По умолчанию global Fetch. Замена должна сохранять streaming и AbortSignal; это не буферизующий fallback. |

Обе функции используют CORS mode, запрещают redirects и задают cache:no-store. Счётчики не превышают Number.MAX_SAFE_INTEGER (9 007 199 254 740 991), независимо от большего диапазона Java long. Библиотека не ограничивает заранее выделенные caller chunks, очереди источника, буферы браузера/сети/прокси и произвольного sink.

### Функции и значения

| Экспорт | Результат и границы |
| --- | --- |
| `uploadStream(options): Promise<UploadCommit>` | Успех только после EOF и HTTP 201 application/json с совпадающими committed:true, key, bytes и непустым ETag. Commit/error JSON ограничен 16 KiB. Чтение источника не подтверждает storage. |
| `UploadCommit` | Замороженный `{committed:true, key:string, bytes:number, etag:string}`. |
| `UploadProgress` | Замороженный union: `{phase:'reading', bytesRead, knownLength}` либо `{phase:'committed', bytesRead, commit}`. Процент при неизвестной длине не придумывается. |
| `downloadTo(options): Promise<DownloadResult>` | Проверяет статус, видимые заголовки, ranges, identity encoding и длину; ждёт каждую запись, затем close после EOF. Целый Blob не собирается. |
| `DownloadResult` | Замороженный `{complete:true, bytesWritten, etag, contentType:string|null, range:DownloadRange|undefined}`. Успех означает завершение close приёмника, не физический fsync. |
| `DownloadProgress` | Замороженный `{bytesWritten, responseBytes}` после завершённых записей в sink. |
| `ByteRange` / `DownloadRange` | Request union выше; response `{start,end,total}` с включительными индексами. Range требует точно совпадающих 206/Content-Range; полный запрос отвергает лишний Content-Range. |
| `objectUrl(objectsBaseUrl, key): URL` | Абсолютный base без query/fragment; не принимает пустой ключ, `.` и `..`. Кодирует логический ключ ровно один раз как один segment. Не кодируйте заранее. Неверный ввод может дать native TypeError. Это не builder presigned S3 URL/session. |
| `streamCapabilities(): StreamCapabilities` | Замороженный `{requestStreamSyntax, responseStreams, transportVerified:false}`. Запрос не отправляется; реальную совместимость браузера и прокси проверяют отдельно. |
| `TransferError(code,message,details?)` | Error с name=TransferError, code, необязательными status, uploadOutcome, cause. Экспортируются unions TransferErrorCode и UploadOutcome. У download нет uploadOutcome. |

### Интерпретация ошибок

| TransferErrorCode | Значение / действие |
| --- | --- |
| `INVALID_ARGUMENT` | Неверные лимиты/options/состояние source; исправьте до создания новой передачи. |
| `UNSUPPORTED` | Нет Fetch/request-stream syntax; явно выберите поддерживаемую среду. |
| `ABORTED` | Signal отменён; проверьте uploadOutcome и сверяйте storage при unknown. |
| `LIMIT_EXCEEDED` | Превышен maxBytes или объявленный размер; не повышайте лимит молча. |
| `LENGTH_MISMATCH` | Фактические байты не совпали с объявленной/ответной длиной. |
| `HTTP_ERROR` | Неожиданный статус находится в status. Корректный ответ committed:false задаёт uploadOutcome=rejected. |
| `PROTOCOL_ERROR` | Скрытые/несогласованные headers, неверный range, encoding, неправильный/слишком большой/несовпадающий commit JSON. |
| `SOURCE_ERROR` | Ошибка чтения или upload chunk другого типа, чем Uint8Array. |
| `SINK_ERROR` | Ошибка write/close приёмника; сетевой источник отменяется. |
| `NETWORK_ERROR` | Сбой Fetch/response transport; исход upload может быть неизвестен. |

Исходы upload: not-started — Fetch не вызывался; rejected — получен явный структурированный отказ; unknown — запрос начат, но корректного commit/достоверного отказа нет. Ни один исход не доказывает отсутствие старого объекта с тем же ключом. Некорректный native browser input может дать TypeError до захвата ресурса (например objectUrl, download Headers/getWriter). Ловите unknown и сужайте через instanceof TransferError, как в [describeFailure](../../browser/demo/transfer-example.ts).

## Ошибки и HTTP

`ObjectStreamHandler(ObjectStreams, StreamLimits)` реализует WebHandler; handle(exchange) обслуживает только `/objects/{key}`. `declaredLength(HttpHeaders)` читает единственный decimal Content-Length и/или X-Upload-Length: 1–19 цифр в неотрицательном long. Дубликаты, списки через запятую, знаки и несовпадение значений запрещены. WebFlux копирует bounded-порцию из pooled DataBuffer и освобождает его. JSON-ошибка возможна только до commit HTTP-ответа.

| HTTP-статус | Code / тело | Причина |
| --- | --- | --- |
| 201 | `{committed:true,key,bytes,etag}` | Подтверждённая загрузка. |
| 200 / 206 | Байтовый поток и length/type/ETag/range headers | Полный объект / диапазон. |
| 400 | `INVALID_REQUEST`, `LENGTH_MISMATCH`, `BUFFER_TOO_LARGE`, `CANCELLED`, `SOURCE_REUSED`; committed=false | Ошибка metadata/источника до HTTP commit. |
| 413 | `TOO_LARGE`; committed=false | Превышение предела байтового потока. |
| 503 | `BUSY`; committed=false | Общие слоты заняты. |
| 404 | `NOT_FOUND`; committed=false | Объекта нет. Неверный endpoint/пустой ключ вместо этого даёт пустой 404. |
| 405 | Пустое тело, `Allow: GET, PUT` | Метод не поддерживается. OPTIONS preflight обслуживает CORS-фильтр приложения. |
| 412 | `CHANGED`; committed=false | Объект изменился между HEAD и условным GET. |
| 416 | `INVALID_RANGE`; committed=false | Неверный диапазон; Content-Range bytes */total только при известном размере. |
| 502 | `STORAGE_FAILURE`; committed=null | Хранилище не подтвердило исход или неожиданная ошибка backend. |

401/403 и дополнительные статусы задаются host-приложением. Заголовки аутентификации не копируются в S3 metadata. Поздняя ошибка download разрывает поток: JSON уже не заменит отправленные байты. [HTTP-контракт](../HTTP-CONTRACT.md).

## Ресурсы и AOP

Операция владеет полученным browser reader/writer до завершения и снимает lock. Ошибка/abort отменяет чтение; ошибка приёмника прерывает запись. Abort возвращается быстро даже при зависшем write, чей побочный эффект может завершиться позже. На Java нельзя закрывать S3-клиент сразу после возврата неподписанного Mono: дождитесь завершения активной работы. ObjectDownload нельзя использовать как общий кэшируемый value.

Retry/cache/repeat aspect не установлен: такие операторы способны повторно подписаться либо удерживать value с живым ресурсом. Замер обычного вызова метода измеряет создание ленивого Mono, а не передачу; успешный download() возвращает открытое тело ещё до конца его чтения. Наблюдайте terminal signal reactive-операции или HTTP-цепочку фильтров приложения. Авторизация должна выполняться до доступа к телу в host filters. Если приложение выбирает AOP, оно сохраняет одну подписку, отмену, close и различие локального завершения/удалённого исхода. Ограничения Spring proxy, self-invocation и final-классов также действуют. [Spring proxy](https://docs.spring.io/spring-framework/reference/core/aop/proxying.html) · [Reactor Mono](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html).

## Диагностика

| Симптом | Проверка / восстановление |
| --- | --- |
| Maven/npm не находит 0.1.0 | Соберите локальные артефакты и установите npm-архив; публикация в реестрах не заявлена. |
| Probe успешен, Fetch upload падает | Проверьте HTTP version, доверие TLS, redirects, CORS preflight и буферизацию реального прокси. Скрытого Blob fallback нет. |
| PROTOCOL_ERROR при правильных байтах | Откройте через CORS ETag/length/range/encoding, отключите compression, проверьте commit JSON/status/key. |
| SOURCE_REUSED / закрытый download | Получите свежий source/download; уберите retry/repeat/cache вокруг resource-backed потоков. |
| BUSY остаётся после cancel | SDK cleanup ещё может удерживать слот; прочитайте или закройте открытые downloads. Не обходите admission. |
| BUFFER_TOO_LARGE | Уменьшите порцию publisher/сети либо осознанно задайте совместимый лимит буфера. Одного file-size cap недостаточно. |
| После отмены unknown-length остался короткий объект | Сверяйте по upload identity; передавайте knownLength, если известен. В HTTP/2 fixture наблюдался префикс 8 192 байта; причина транспорта не установлена. |
| Объект изменился между ranges | 412 означает смену версии; не склеивайте ranges разных версий. If-Range/resume protocol не предоставляется. |
| Падают смешанные версии Netty | Сохраните экспортируемый BOM и проверьте dependency resolution; не смешивайте codec families 4.1/4.2. |
| Multipart остался после сбоя процесса/сети | Проверьте разрешение AbortMultipartUpload и lifecycle. Client cancel сам по себе не доказывает очистку/откат. |
| Недоступны Docker snapshot или JDK 27 | Зафиксируйте непроведённую проверку. Историческое evidence не является новым запуском. |

[← Руководство](ru.md) · [Версии](../index.md) · [README](../../README.md) · [EN](reference-en.md)

## Configured client and profiles / Настроенный клиент и профили

`createFileStreams(options)` accepts `objectsUrl` (required; browser-relative or absolute, absolute in Node/SSR), `profile` (attachments default), optional `maxBytes`, `headers`, `credentials`, `fetch`. It returns `uploadFile(key, Blob, options?)`, `upload(key, ReadableStream, options?)`, `download(key, WritableStream, options?)`. All operations accept `signal`, `headers`, `onProgress`; stream upload adds `knownLength`; download adds `range`. No automatic retry or new protocol. Full TypeScript types: `FileProfile`, `FileStreamsOptions`, `FileUploadOptions`, `StreamUploadOptions`, `StreamDownloadOptions`. Invalid configuration or URL/key may throw synchronously; transfer errors retain existing TransferError semantics. File.type is used unless Content-Type is already set.

`fileProfiles` exports immutable attachments/media/archives bounds. Java `StreamProfile.ATTACHMENTS/MEDIA/ARCHIVES` exposes `maxBytes()` and `limits()` with 1 MiB source buffer. `StreamLimits.defaults()` remains ARCHIVES. `UploadRequest.knownLength(key,bytes,contentType)` omits empty metadata boilerplate; `ObjectStreams.download(key)` requests a complete object with unchanged resource ownership. `S3Clients.bounded(region,credentials)` selects AWS without a null endpoint. `new S3ObjectStreams(client,bucket,profile)` uses `DEFAULT_MAX_TRANSFERS` (2) and leaves the client application-owned. Explicit existing overloads remain.

`ObjectStreamHandler.handle(exchange,key)` accepts an already authorized logical key from a custom controller; the original handle(exchange) parses /objects. It reuses the same PUT/GET, byte/range checks, error mapping and resource cleanup. It does not authorize the supplied key.

`createFileStreams` принимает обязательный objectsUrl (относительный или абсолютный в браузере, абсолютный в Node/SSR), profile (attachments по умолчанию), optional maxBytes/headers/credentials/fetch. Методы: uploadFile(key,Blob,options?), upload(key,ReadableStream,options?), download(key,WritableStream,options?). Общие параметры операций: signal/headers/onProgress, для stream-upload knownLength, для скачивания range. Автоповтора и второго протокола нет. Неверная конфигурация или URL/key могут выбросить синхронную ошибку; TransferError сохраняет прежний смысл. File.type используется, если Content-Type не задан.

fileProfiles — immutable attachments/media/archives. Java StreamProfile даёт maxBytes()/limits() с буфером 1 МиБ; defaults() остаётся ARCHIVES. knownLength factory убирает пустую metadata, download(key) — null для полного файла. S3Clients.bounded(region,credentials) выбирает AWS без null; S3ObjectStreams(client,bucket,profile) использует DEFAULT_MAX_TRANSFERS=2 и не закрывает клиент приложения. handle(exchange,key) позволяет контроллеру повторно использовать HTTP/cleanup после собственной проверки прав; сам handler эти права не проверяет.
