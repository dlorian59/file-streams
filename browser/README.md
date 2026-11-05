# @59alex/file-streams

<details name="readme-language">
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-en-dark.svg"><img src="assets/lang-en-light.svg" width="42" height="26" alt="EN"></picture></summary>

Continuous file uploads/downloads for existing browser applications. One configured client, known file sizes, validated commits and bounded destination writes.

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

Registry publication is pending. `file` is a File/Blob; `destination` is an application-provided WritableStream. `attachments` allows 64 MiB, `media` 512 MiB, `archives` 16 GiB. Optional `maxBytes` overrides the profile. `fileProfiles` exposes these bounds for components.

Use `files.upload(key, source, { knownLength, signal, onProgress })` for generated content and `files.download(key, destination, { range, signal, onProgress })` for download/ranges. Operation `headers` override configured headers. Credentials and custom fetch can be configured once; authentication stays with the application. The original `uploadStream`, `downloadTo`, `objectUrl`, `streamCapabilities` and `TransferError` APIs remain available.

Only a returned commit confirms upload. Cancellation/network failure can have an unknown outcome; no automatic retry or whole-file buffering fallback exists. Working HTTP2/HTTP3 over TLS, visible range/ETag/length headers and an unlocked destination are required.

[Full integration / Подключение](https://github.com/dlorian59/file-streams/blob/main/docs/0.1.0/integration-en.md) · [All API/options/errors / API и ошибки](https://github.com/dlorian59/file-streams/blob/main/docs/0.1.0/reference-en.md) · [Commented example / Пример](https://github.com/dlorian59/file-streams/blob/main/browser/demo/application-files.ts) · [Spring receiver](https://github.com/dlorian59/file-streams-spring-boot-starter) · [React UI](https://github.com/dlorian59/file-streams-react) · [← README](https://github.com/dlorian59/file-streams/blob/main/README.md)

</details>

<details name="readme-language" open>
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-ru-dark.svg"><img src="assets/lang-ru-light.svg" width="42" height="26" alt="RU"></picture></summary>

Непрерывная загрузка и скачивание файлов в браузерном приложении. Один настроенный клиент, известный размер файла, проверка commit и запись с backpressure.

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

Публикация в реестре ещё не выполнена. `file` — File/Blob, `destination` — WritableStream приложения. `attachments` допускает 64 МиБ, `media` — 512 МиБ, `archives` — 16 ГиБ. `maxBytes` заменяет лимит профиля; `fileProfiles` экспортирует пределы для компонентов.

Для генерируемых данных — `files.upload(key, source, { knownLength, signal, onProgress })`, для скачивания/диапазонов — `files.download(key, destination, { range, signal, onProgress })`. Headers операции заменяют настроенные headers. Credentials и custom fetch задаются один раз; аутентификацией управляет приложение. Исходные API `uploadStream`, `downloadTo`, `objectUrl`, `streamCapabilities`, `TransferError` сохранены.

Загрузку подтверждает только возвращённый commit. Отмена/сетевая ошибка могут оставить неизвестный результат; автоповторов и fallback с полным буфером нет. Нужны работающий HTTP2/HTTP3 с TLS, видимые range/ETag/length headers и незаблокированный destination.

[Full integration / Подключение](https://github.com/dlorian59/file-streams/blob/main/docs/0.1.0/integration-ru.md) · [All API/options/errors / API и ошибки](https://github.com/dlorian59/file-streams/blob/main/docs/0.1.0/reference-ru.md) · [Commented example / Пример](https://github.com/dlorian59/file-streams/blob/main/browser/demo/application-files.ts) · [Spring receiver](https://github.com/dlorian59/file-streams-spring-boot-starter) · [React UI](https://github.com/dlorian59/file-streams-react) · [← README](https://github.com/dlorian59/file-streams/blob/main/README.md)

</details>
