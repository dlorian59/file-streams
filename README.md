# file-streams

<details name="readme-language">
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-en-dark.svg"><img src="assets/lang-en-light.svg" width="42" height="26" alt="EN"></picture></summary>

[Version 0.1.0 guide](docs/0.1.0/en.md) · [All versions](docs/index.md) · [Complete reference](docs/0.1.0/reference-en.md)

Continuous browser ↔ Java ↔ S3 file transfer. Applications provide one stream; the S3 adapter manages bounded internal multipart requests. Upload progress and confirmed storage commit are different states.

```text
ReadableStream → HTTP/2 request → WebFlux → Reactor → AWS SDK → S3
WritableStream ← HTTP response  ← WebFlux ← Reactor ← AWS SDK ← S3
```

| Module | What it provides |
| --- | --- |
| [`file-streams`](docs/0.1.0/en.md#3-upload-unknown-or-known-length) | One-shot byte streams, checked lengths and size limits, long counters, single ranges, explicit resource ownership |
| [`file-streams-s3`](docs/0.1.0/en.md#2-create-the-application-owned-storage-client) | Stock AWS async multipart with a 10 MiB SDK buffer setting, one in-flight part, bounded transfer admission and cancellation cleanup |
| [`file-streams-webflux`](docs/0.1.0/en.md#7-webflux-raw-body-adapter) | Raw-body PUT, streamed GET, declared-size checks, pooled DataBuffer release and commit-aware errors |
| [`@59alex/file-streams`](browser/README.md) | Browser ReadableStream / WritableStream, AbortSignal, progress and strict response validation |

Java 17 is the minimum; Java 21, 25 and 27 run the same API. [Features used by runtime](docs/JAVA-SUPPORT.md). React integration is maintained as a separate companion package.

## Build and use

Requires JDK 17+ and Node.js 20+. These checks need no running S3 service. The consumer prints `Published-artifact consumer passed: core + S3 + WebFlux`.

```bash
./gradlew build publishToMavenLocal -Dmaven.repo.local="$PWD/build/local-repository"
./gradlew -p examples/consumer run
cd browser
npm ci
npm test
npm run test:package
```

Java coordinates are `io.github.59alex:file-streams[-s3|-webflux]:0.1.0`. The command above publishes to an isolated local repository for the example consumer. Use `./gradlew publishToMavenLocal` to install into your regular Maven Local repository; registry publication is a separate [release gate](ROADMAP.md). The [versioned guide](docs/0.1.0/en.md) shows dependencies and a code example for each capability. The [independent consumer](examples/consumer) verifies packaged artifacts rather than source-project dependencies.

## Browser and Spring in one flow

A document upload should not require separate multipart/session logic in React and Java. `file-streams` carries one stream through both sides, checks size boundaries and distinguishes progress from confirmed storage. Downloads await each destination write. These contracts reduce integration code without claiming production adoption or universal performance results.

```ts
const commit = await uploadStream({
  url: objectUrl(new URL('/objects', location.origin), key), key, source: file.stream(),
  knownLength: file.size, maxBytes: 512 * 1024 * 1024,
});
await downloadTo({ url: objectUrl(new URL('/objects', location.origin), key), destination, maxBytes: 512 * 1024 * 1024 });
```

The corresponding Java operations are:

```java
Mono<UploadResult> committed = files.upload(request, requestBody);
Mono<ObjectDownload> opened = files.download(key, rangeHeader);
```

The existing [WebFlux handler](webflux/src/main/java/io/github/alex59/filestreams/webflux/ObjectStreamHandler.java) connects those operations to HTTP and owns download cleanup. [Annotated TypeScript](browser/demo/transfer-example.ts) · [annotated Java](demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java). The companion [Spring Boot starter](https://github.com/59Alex/file-streams-spring-boot-starter) supplies opt-in configuration and `/objects/**`; [React integration](https://github.com/59Alex/file-streams-react) supplies UI lifecycle around the same browser API. Companion repositories are currently prepared locally; remote creation and registry publication are pending.

## What is verified

Garage 2.4.1 round trips cover empty and unknown-length bodies, 5 MiB internal part boundaries, SHA-256 equality, ranges, source failures and multipart cancellation cleanup. A real 2 GiB + 1 byte object passed with a 96 MiB Java heap; Garage compressed the repeating test payload. This is functional/resource evidence, not a throughput benchmark or an AWS/MinIO certification. [Exact measurements and remaining gates](docs/VERIFICATION.md).

The browser requires an actually working request-stream transport, usually HTTP/2 with TLS. A successful feature probe alone does not prove proxy or browser support. No whole-file Blob fallback is used. [Browser compatibility and API](browser/README.md).

## Demo and boundaries

Docker and OpenSSL are required for the complete demo. [Run the isolated HTTP/2 demo](docs/0.1.0/en.md#local-end-to-end-demo). It has no authentication and binds to loopback. Production applications supply authorization, keys, CORS, storage policy and proxy configuration.

Cancelling the local Java subscription before EOF signals a source error so the SDK can abort an incomplete multipart upload. Cancellation during finalization may race with a successful commit; a lost acknowledgement means an unknown outcome. Local subscription cancellation and source errors are distinct from HTTP termination. If a transport presents termination as normal EOF for an unknown-length request, a backend can accept the received prefix; a browser abort therefore remains an unknown outcome, even before the intended source ends. Supply `knownLength` / `X-Upload-Length` whenever the final size is known. This detects a short body but does not make cancellation a rollback guarantee. Streams are not automatically retried. ETags are opaque identifiers, not checksums. [HTTP contract](docs/HTTP-CONTRACT.md) · [Architecture](docs/ARCHITECTURE.md).

[MIT license](LICENSE) · Copyright 2026 Aleksandr Kashevarov.

</details>

<details name="readme-language" open>
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-ru-dark.svg"><img src="assets/lang-ru-light.svg" width="42" height="26" alt="RU"></picture></summary>

[Руководство 0.1.0](docs/0.1.0/ru.md) · [Все версии](docs/index.md) · [Полный справочник](docs/0.1.0/reference-ru.md)

Непрерывная передача файлов между браузером, Java-сервером и S3. Приложение передаёт один поток; адаптер сам выполняет внутренние multipart-запросы S3 с ограниченным буфером. Прочитанные байты и подтверждённое сохранение — разные состояния.

| Модуль | Возможности |
| --- | --- |
| [`file-streams`](docs/0.1.0/ru.md#3-загрузка-с-неизвестной-или-известной-длиной) | Одноразовые потоки, проверка размера, счётчики long, одиночные byte ranges и управление ресурсами |
| [`file-streams-s3`](docs/0.1.0/ru.md#2-клиент-и-ограничения) | AWS async SDK: настройка буфера 10 MiB, одна часть в полёте, ограничение числа передач и очистка при отмене |
| [`file-streams-webflux`](docs/0.1.0/ru.md#7-webflux) | PUT с сырым телом, потоковый GET, освобождение DataBuffer, проверка длины и ошибки с состоянием commit |
| [`@59alex/file-streams`](browser/README.md) | ReadableStream / WritableStream, AbortSignal, прогресс и проверка ответа сервера |

Минимум Java 17. На Java 21, 25 и 27 работает тот же API; [какие возможности языка используются](docs/JAVA-SUPPORT.md). React-интеграция вынесена в отдельный companion-пакет.

## Браузер и Spring в одном потоке

Для загрузки документов не нужно отдельно реализовывать multipart-сессии на React и Java. `file-streams` проводит один поток через обе стороны, проверяет ограничения размера и различает прогресс и подтверждённое сохранение. При скачивании следующая порция ждёт записи предыдущей. Это сокращает код интеграции; библиотека не обещает универсальную скорость и не заявляет вымышленные внедрения.

```ts
const commit = await uploadStream({
  url: objectUrl(new URL('/objects', location.origin), key), key,
  source: file.stream(), knownLength: file.size, maxBytes: 512 * 1024 * 1024,
});
await downloadTo({
  url: objectUrl(new URL('/objects', location.origin), key), destination, maxBytes: 512 * 1024 * 1024,
});
```

На Java эти действия соответствуют:

```java
Mono<UploadResult> committed = files.upload(request, requestBody);
Mono<ObjectDownload> opened = files.download(key, rangeHeader);
```

[WebFlux handler](webflux/src/main/java/io/github/alex59/filestreams/webflux/ObjectStreamHandler.java) связывает операции с HTTP и освобождает ресурсы скачивания. [TypeScript с комментариями](browser/demo/transfer-example.ts) · [Java с комментариями](demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java). [Spring Boot starter](https://github.com/59Alex/file-streams-spring-boot-starter) подключает конфигурацию и `/objects/**`, [React-интеграция](https://github.com/59Alex/file-streams-react) управляет состоянием интерфейса поверх того же API. Эти companion-репозитории пока подготовлены локально; удалённое создание и публикация пакетов ещё не выполнены.

## Запуск

Нужны JDK 17+ и Node.js 20+. Для этих проверок S3 не требуется. Consumer выводит `Published-artifact consumer passed: core + S3 + WebFlux`.

```bash
./gradlew build publishToMavenLocal -Dmaven.repo.local="$PWD/build/local-repository"
./gradlew -p examples/consumer run
cd browser
npm ci
npm test
npm run test:package
```

Координаты Java: `io.github.59alex:file-streams[-s3|-webflux]:0.1.0`. Команда выше публикует артефакты в отдельный локальный каталог для примера consumer. Для установки в обычный Maven Local выполните `./gradlew publishToMavenLocal`; публикация в реестры — отдельный [этап выпуска](ROADMAP.md). В [руководстве](docs/0.1.0/ru.md) каждая возможность сопровождается примером.

На Garage 2.4.1 проверены пустой и неизвестный размер, границы внутренних частей 5 MiB, диапазоны, отмена и ошибки. Реальный объект 2 GiB + 1 байт прошёл загрузку и скачивание с совпадающим SHA-256 при heap 96 MiB. Повторяющиеся тестовые данные сжимались самим Garage; это проверка корректности и ресурсов, а не универсальный бенчмарк. [Измерения и ограничения](docs/VERIFICATION.md).

Для браузерной загрузки нужен реально работающий транспорт request streams, обычно HTTP/2 с TLS. Проверка конструктора Request не доказывает поддержку прокси или сети. Скрытого накопления файла в Blob нет.

Для полной демонстрации нужны Docker и OpenSSL. [Демонстрация](docs/0.1.0/ru.md#локальная-демонстрация) работает на loopback без авторизации. Приложение отвечает за доступ, ключи объектов, CORS, настройки S3 и прокси. Отмена при финализации может пересечься с успешным сохранением; потерянный ответ означает неизвестный результат. Локальная отмена подписки или ошибка источника отличаются от завершения HTTP-запроса. Если транспорт передаёт завершение как обычный EOF при неизвестной длине, backend может принять полученный префикс. Поэтому browser abort остаётся неизвестным исходом даже до предполагаемого конца источника. Передавайте `knownLength` / `X-Upload-Length`, когда итоговый размер известен: это обнаруживает короткое тело, но не превращает отмену в гарантию отката. Автоповтора одноразового потока нет. ETag — непрозрачный идентификатор, не контрольная сумма.

[MIT](LICENSE) · 2026 Aleksandr Kashevarov.

</details>
