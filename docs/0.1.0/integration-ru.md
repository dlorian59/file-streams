# Вложения в существующем приложении

[← README](../../README.md) · [EN](integration-en.md) · [Справочник](reference-ru.md) · [Подробное Java-руководство](ru.md)

Настройте клиент один раз, если действия UI используют общий endpoint и лимит размера. Для настройки каждого запроса отдельно остаются `uploadStream` и `downloadTo`: новый клиент вызывает те же движки.

## Настроить один раз

```ts
import { createFileStreams } from '@59alex/file-streams';
const files = createFileStreams({
  objectsUrl: '/attachments',
  profile: 'attachments',
  credentials: 'same-origin',
});
```

`objectsUrl` — абсолютный или относительный браузерный URL без query/fragment. В браузере относительный адрес разрешается от URL текущей страницы. Создание клиента не обращается к сети; в Node и SSR нужен абсолютный URL, поскольку browser location отсутствует. Профиль по умолчанию — `attachments`, credentials — `same-origin`. `maxBytes` заменяет предел профиля для загрузки и скачивания. `headers` копируются при создании; headers операции заменяют одноимённые значения. Для обновляемых credentials передавайте headers операции или authenticated `fetch` приложения, сохраняющий стрим и AbortSignal. Клиент не выполняет вход и refresh credentials.

| Профиль | Максимум объекта/ответа | Java |
| --- | --- | --- |
| `attachments` | 64 МиБ | `StreamProfile.ATTACHMENTS` |
| `media` | 512 МиБ | `StreamProfile.MEDIA` |
| `archives` | 16 ГиБ | `StreamProfile.ARCHIVES` |

`StreamLimits.defaults()` сохраняет прежний Java-предел `ARCHIVES`; новый браузерный клиент начинает с более консервативного attachments. Настройте обе стороны явно. Java-профили допускают отдельный буфер источника до 1 МиБ. Это ограничения, не объём выделенной памяти и не обещание производительности. Лимит скачивания в браузере и загрузки на сервере проверяются отдельно.

## Сохранить выбранный файл

```ts
const commit = await files.uploadFile('alice:invoice.pdf', file, {
  signal: controller.signal,
  onProgress: progress => updateProgress(progress),
});
```

`file` — File/Blob. Метод сам передаёт размер через `X-Upload-Length`, непустой content type при отсутствии override и открывает поток для этого вызова. Promise завершается успешно только после проверки HTTP 201, ключа, числа байтов и ETag. Буферизация, multipart-сессия и повтор не добавляются. Новый вызов — решение приложения: перед повторной записью нужно проверить неизвестный результат предыдущей.

[Полный пример приложения с комментариями](../../browser/demo/application-files.ts) содержит imports и функции для выбора/сохранения файла. Серверный [контроллер с проверкой аккаунта](https://github.com/dlorian59/file-streams-spring-boot-starter/blob/main/examples/consumer/src/main/java/com/example/attachments/AttachmentController.java) проверяет доверенный principal до подписки на тело. Account ID для ключа берётся из сессии приложения; клиентский header не является аутентификацией.

## Генерируемые данные

```ts
const committed = await files.upload('alice:report.csv', reportStream, {
  headers: { 'Content-Type': 'text/csv' },
  signal: controller.signal,
});
```

`reportStream` — одноразовый `ReadableStream<Uint8Array>` приложения. Настроенный предел размера сохраняется. Если итоговый размер известен, передайте `knownLength`; не задавайте `X-Upload-Length` вручную. Для разрешённого приложением повтора нужен новый источник. [Пример с комментариями](../../browser/demo/transfer-example.ts).

## Скачать файл или диапазон

```ts
await files.download('alice:invoice.pdf', destination, {
  signal: controller.signal,
  onProgress: progress => updateDownload(progress),
});
await files.download('alice:report.csv', previewDestination, { range: { start: 0, end: 255 } });
```

Каждый destination — незаблокированный WritableStream приложения. Успешная передача закрывает его; ошибка вызывает abort. Файловый picker вызывается из пользовательского действия и доступен не во всех браузерах. Fallback с целым Blob отсутствует. Диапазон требует HTTP 206 и соответствующий Content-Range; проигнорированный Range — ошибка. Для каждой операции нужен новый destination. [Полные примеры диапазонов и транспорта](../../browser/demo/transfer-example.ts).

## Результаты

`onProgress` наблюдает прочитанные/записанные байты и не превращает ошибку в успех. Сохранение подтверждает только возвращённый commit. Отмена и потеря ответа могут оставить неизвестный результат; клиент сохраняет `TransferError.code`, `.status`, `.uploadOutcome`. Если транспорт неизвестной длины представит обрыв как обычный EOF, сервер может сохранить полученный префикс. Известный размер обнаруживает короткое тело, но не гарантирует rollback. [HTTP и ошибки](../HTTP-CONTRACT.md).

## Java без Boot

Maven-зависимости: `io.github.59alex:file-streams-s3:0.1.0` и для HTTP `io.github.59alex:file-streams-webflux:0.1.0`; core входит транзитивно. [Java-руководство](ru.md) показывает Gradle; Maven использует те же group/artifact/version в обычных `<dependency>`. Сейчас артефакты собираются локально; публикация ещё не выполнена.

```java
try (var credentials = DefaultCredentialsProvider.builder().build();
     var client = S3Clients.bounded(Region.EU_WEST_1, credentials)) {
    var objects = new S3ObjectStreams(client, "application-attachments", StreamProfile.ATTACHMENTS);
    // Область ресурсов должна жить до завершения всех передач.
}
```

[Полный Java-исходник и imports](../../examples/consumer/src/main/java/NamedProfiles.java). Null endpoint и позиционное число передач не нужны; клиентом по-прежнему владеет приложение. Старый конструктор с явными limits/concurrency сохранён.

```java
StreamLimits limits = StreamProfile.ATTACHMENTS.limits();
UploadRequest request = UploadRequest.knownLength(key, size, contentType);
Mono<UploadResult> commit = objects.upload(request, body);
Mono<ObjectDownload> opened = objects.download(key);
```

Эти строки применяются внутри [Java-примера с полными комментариями](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java); imports — `io.github.alex59.filestreams.*` и `reactor.core.publisher.Mono`. Приложение владеет S3-клиентом и закрывает открытые скачивания при любом завершении. [Управление ресурсами](ru.md). В Boot это делает [starter](https://github.com/dlorian59/file-streams-spring-boot-starter).

## Диагностика

- HTTP 401/403: проверьте аутентификацию приложения и права на объект; библиотека их не обходит.
- Сетевая ошибка до commit: проверьте request streaming в браузере/прокси/HTTP2-TLS; ошибка не доказывает rollback.
- Ошибка протокола скачивания: откройте ETag, Accept-Ranges, Content-Length, Content-Range в CORS; выключите сжатие на этих byte-маршрутах.
- Превышение лимита: согласуйте профили или явные пределы обеих сторон. Сервер независимо от браузера ограничивает хранилище.
- Автоповторы в HTTP-wrapper: уберите retry одноразового тела. Повтор аутентификации допустим только с новым телом и выясненным результатом предыдущей записи.

[Справочник исходных примитивов](reference-ru.md) · [Разработка и storage demo](ru.md) · [← README](../../README.md)
