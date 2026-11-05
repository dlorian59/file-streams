# file-streams

<details name="readme-language">
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-en-dark.svg"><img src="assets/lang-en-light.svg" width="42" height="26" alt="EN"></picture></summary>

Send files from the browser to Java/S3 and stream them back without loading the entire file into memory. Useful for attachments, media and exports: one continuous stream, confirmed upload results and explicit cancellation.

## Get started in your application

Browser dependency (publication pending; [local artifact workflow](docs/0.1.0/en.md)):

```json
{ "dependencies": { "@59alex/file-streams": "0.1.0" } }
```

```ts
import { createFileStreams } from "@59alex/file-streams";

const files = createFileStreams({
  objectsUrl: "/attachments",
  profile: "attachments",
});
const commit = await files.uploadFile("alice:invoice.pdf", file);
await files.download(commit.key, destination);
```

`file` is the selected File; `destination` is a WritableStream obtained by the application. File size/type and URL encoding are automatic. `attachments` limits each transfer to 64 MiB. Successful upload means a validated storage commit, not just a fully read source. [Complete commented browser example](browser/demo/application-files.ts).

### Receive it in Spring WebFlux

Maven:

```xml
<dependency>
  <groupId>io.github.59alex</groupId>
  <artifactId>file-streams-spring-boot-starter</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle:

```kotlin
dependencies {
    implementation("io.github.59alex:file-streams-spring-boot-starter:0.1.0")
}
```

```yaml
file-streams:
  enabled: true
  limits:
    profile: attachments
  s3:
    bucket: ${ATTACHMENTS_BUCKET}
    region: ${AWS_REGION}
```

Java 17+, Boot 4.1.2 WebFlux; the dependency includes WebFlux. Add the ready [account-scoped AttachmentController](https://github.com/dlorian59/file-streams-spring-boot-starter/blob/main/examples/consumer/src/main/java/com/example/attachments/AttachmentController.java) to your application: it checks the principal, then delegates streaming to `ObjectStreamHandler`. [Complete file tree and integration](https://github.com/dlorian59/file-streams-spring-boot-starter#readme). Credentials and authentication remain application-owned. The starter artifacts are not yet published in Central.

## What you can do

| Task | Short API | Result and full example |
| --- | --- | --- |
| Save a selected file | `files.uploadFile(key, file)` | Validated commit; [source](browser/demo/application-files.ts) |
| Stream a generated report | `files.upload(key, source)` | Unknown-length stream with bounded size; [guide](docs/0.1.0/integration-en.md#generated-content) |
| Download or preview | `files.download(key, sink, { range })` | Backpressured writes, checked byte range; [source](browser/demo/transfer-example.ts) |
| Cancel or show progress | `{ signal, onProgress }` | Distinguishes reading from committed; [guide](docs/0.1.0/integration-en.md#outcomes) |
| Use Java without Spring | `StreamProfile.ATTACHMENTS.limits()` | Same core/S3 engines; [Java guide](docs/0.1.0/en.md) |
| Add a ready React UI | `FileAttachment` | Upload/download states; [React integration](https://github.com/dlorian59/file-streams-react) |

Cancellation or a lost response can leave the upload result unknown, including before intended EOF for unknown-length HTTP bodies. Do not automatically retry a one-shot stream. Supply a known size whenever available. Browser upload needs verified HTTP/2 or HTTP/3/TLS transport; no whole-file buffering fallback is provided.

[Integration / Подключение](docs/0.1.0/integration-en.md) · [Guide / Руководство](docs/0.1.0/en.md) · [Reference / Справочник](docs/0.1.0/reference-en.md) · [Versions / Версии](docs/index.md) · [Verification / Проверки](docs/VERIFICATION.md) · [MIT](LICENSE)

For video messages, [video-streams](https://github.com/dlorian59/video-streams) adds FFmpeg preparation and HLS segment playback over this file storage. Use its [Spring Boot starter](https://github.com/dlorian59/video-streams-spring-boot-starter) and [React components](https://github.com/dlorian59/video-streams-react) for the complete upload-to-playback flow.

</details>

<details name="readme-language" open>
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-ru-dark.svg"><img src="assets/lang-ru-light.svg" width="42" height="26" alt="RU"></picture></summary>

Передавайте файлы из браузера в Java/S3 и обратно без загрузки файла целиком в память. Для вложений, медиа и экспортов: один непрерывный поток, подтверждение сохранения и явная отмена.

## Подключение к вашему приложению

Зависимость браузерного приложения (публикация ещё не выполнена; [локальные артефакты](docs/0.1.0/ru.md)):

```json
{ "dependencies": { "@59alex/file-streams": "0.1.0" } }
```

```ts
import { createFileStreams } from "@59alex/file-streams";

const files = createFileStreams({
  objectsUrl: "/attachments",
  profile: "attachments",
});
const commit = await files.uploadFile("alice:invoice.pdf", file);
await files.download(commit.key, destination);
```

`file` — выбранный File, `destination` — WritableStream приложения. Размер, тип файла и кодирование URL передаются автоматически. `attachments` ограничивает передачу 64 МиБ. Успех загрузки — проверенный commit хранилища, а не только прочитанное тело. [Полный браузерный пример с комментариями](browser/demo/application-files.ts).

### Принять файл в Spring WebFlux

Maven:

```xml
<dependency>
  <groupId>io.github.59alex</groupId>
  <artifactId>file-streams-spring-boot-starter</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle:

```kotlin
dependencies {
    implementation("io.github.59alex:file-streams-spring-boot-starter:0.1.0")
}
```

```yaml
file-streams:
  enabled: true
  limits:
    profile: attachments
  s3:
    bucket: ${ATTACHMENTS_BUCKET}
    region: ${AWS_REGION}
```

Java 17+, Boot 4.1.2 WebFlux; WebFlux уже входит в зависимость. Добавьте готовый [AttachmentController с проверкой аккаунта](https://github.com/dlorian59/file-streams-spring-boot-starter/blob/main/examples/consumer/src/main/java/com/example/attachments/AttachmentController.java): он проверяет principal и передаёт поток в `ObjectStreamHandler`. [Полная структура файлов и подключение](https://github.com/dlorian59/file-streams-spring-boot-starter#readme). Credentials и аутентификацией управляет приложение. Артефакты starter пока не опубликованы в Central.

## Возможности

| Задача | Короткий API | Результат и полный пример |
| --- | --- | --- |
| Сохранить выбранный файл | `files.uploadFile(key, file)` | Проверенный commit; [исходник](browser/demo/application-files.ts) |
| Передать сформированный отчёт | `files.upload(key, source)` | Неизвестная длина с ограничением размера; [руководство](docs/0.1.0/integration-ru.md#генерируемые-данные) |
| Скачать или посмотреть диапазон | `files.download(key, sink, { range })` | Запись с backpressure и проверкой range; [исходник](browser/demo/transfer-example.ts) |
| Отменить или показать прогресс | `{ signal, onProgress }` | Различает чтение и commit; [руководство](docs/0.1.0/integration-ru.md#результаты) |
| Использовать Java без Spring | `StreamProfile.ATTACHMENTS.limits()` | Те же core/S3 engines; [Java guide](docs/0.1.0/ru.md) |
| Добавить готовый React UI | `FileAttachment` | Состояния загрузки/скачивания; [React-интеграция](https://github.com/dlorian59/file-streams-react) |

Отмена или потерянный ответ могут оставить результат загрузки неизвестным, в том числе до ожидаемого конца HTTP-тела неизвестной длины. Одноразовый поток автоматически не повторяется. Передавайте известный размер всегда, когда он доступен. Для браузерной загрузки нужен проверенный HTTP/2 или HTTP/3/TLS; fallback с полным буфером файла не предусмотрен.

[Integration / Подключение](docs/0.1.0/integration-ru.md) · [Guide / Руководство](docs/0.1.0/ru.md) · [Reference / Справочник](docs/0.1.0/reference-ru.md) · [Versions / Версии](docs/index.md) · [Verification / Проверки](docs/VERIFICATION.md) · [MIT](LICENSE)

Для видеосообщений [video-streams](https://github.com/dlorian59/video-streams) добавляет подготовку FFmpeg и воспроизведение HLS-сегментов поверх этого файлового хранилища. [Spring Boot стартер](https://github.com/dlorian59/video-streams-spring-boot-starter) и [React-компоненты](https://github.com/dlorian59/video-streams-react) связывают загрузку исходника с просмотром.

</details>
