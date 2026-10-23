# s3-file-streams

<details name="readme-language">
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-en-dark.svg"><img src="assets/lang-en-light.svg" width="42" height="26" alt="EN"></picture></summary>

[Version 0.1.0 guide](docs/0.1.0/en.md) · [All versions](docs/index.md)

Continuous browser ↔ Java ↔ S3 file transfer. Applications provide one stream; the S3 adapter manages bounded internal multipart requests. Upload progress and confirmed storage commit are different states.

```text
ReadableStream → HTTP/2 request → WebFlux → Reactor → AWS SDK → S3
WritableStream ← HTTP response  ← WebFlux ← Reactor ← AWS SDK ← S3
```

| Module | What it provides |
| --- | --- |
| `s3-file-streams` | One-shot byte streams, checked lengths and size limits, long counters, single ranges, explicit resource ownership |
| `s3-file-streams-s3` | Stock AWS async multipart with a 10 MiB SDK buffer setting, one in-flight part, bounded transfer admission and cancellation cleanup |
| `s3-file-streams-webflux` | Raw-body PUT, streamed GET, declared-size checks, pooled DataBuffer release and commit-aware errors |
| [`@59alex/s3-file-streams`](browser/README.md) | Browser ReadableStream / WritableStream, AbortSignal, progress and strict response validation |

Java 17 is the minimum; Java 21, 25 and 27 run the same API. [Features used by runtime](docs/JAVA-SUPPORT.md). A React adapter is outside this version.

## Build and use

```bash
./gradlew build publishToMavenLocal
cd browser
npm ci
npm test
npm run test:package
```

Java coordinates are `io.github.59alex:s3-file-streams[-s3|-webflux]:0.1.0`. Build to Maven Local before resolving them; registry publication is a separate [release gate](ROADMAP.md). The [versioned guide](docs/0.1.0/en.md) shows dependencies and a code example for each capability. The [independent consumer](examples/consumer) verifies packaged artifacts rather than source-project dependencies.

## What is verified

Garage 2.4.1 round trips cover empty and unknown-length bodies, 5 MiB internal part boundaries, SHA-256 equality, ranges, source failures and multipart cancellation cleanup. A real 2 GiB + 1 byte object passed with a 96 MiB Java heap; Garage compressed the repeating test payload. This is functional/resource evidence, not a throughput benchmark or an AWS/MinIO certification. [Exact measurements and remaining gates](docs/VERIFICATION.md).

The browser requires an actually working request-stream transport, usually HTTP/2 with TLS. A successful feature probe alone does not prove proxy or browser support. No whole-file Blob fallback is used. [Browser compatibility and API](browser/README.md).

## Demo and boundaries

[Run the isolated HTTP/2 demo](docs/0.1.0/en.md#local-end-to-end-demo). It has no authentication and binds to loopback. Production applications supply authorization, keys, CORS, storage policy and proxy configuration.

Cancellation before EOF signals a source error so the SDK can abort an incomplete multipart upload. Cancellation during finalization may race with a successful commit; a lost acknowledgement means an unknown outcome. Streams are not automatically retried. ETags are opaque identifiers, not checksums. [HTTP contract](docs/HTTP-CONTRACT.md) · [Architecture](docs/ARCHITECTURE.md).

[MIT license](LICENSE) · Copyright 2026 Aleksandr Kashevarov.

</details>

<details name="readme-language" open>
<summary><picture><source media="(prefers-color-scheme: dark)" srcset="assets/lang-ru-dark.svg"><img src="assets/lang-ru-light.svg" width="42" height="26" alt="RU"></picture></summary>

[Руководство 0.1.0](docs/0.1.0/ru.md) · [Все версии](docs/index.md)

Непрерывная передача файлов между браузером, Java-сервером и S3. Приложение передаёт один поток; адаптер сам выполняет внутренние multipart-запросы S3 с ограниченным буфером. Прочитанные байты и подтверждённое сохранение — разные состояния.

| Модуль | Возможности |
| --- | --- |
| `s3-file-streams` | Одноразовые потоки, проверка размера, счётчики long, одиночные byte ranges и управление ресурсами |
| `s3-file-streams-s3` | AWS async SDK: настройка буфера 10 MiB, одна часть в полёте, ограничение числа передач и очистка при отмене |
| `s3-file-streams-webflux` | PUT с сырым телом, потоковый GET, освобождение DataBuffer, проверка длины и ошибки с состоянием commit |
| [`@59alex/s3-file-streams`](browser/README.md) | ReadableStream / WritableStream, AbortSignal, прогресс и проверка ответа сервера |

Минимум Java 17. На Java 21, 25 и 27 работает тот же API; [какие возможности языка используются](docs/JAVA-SUPPORT.md). React-адаптер в эту версию не входит.

## Запуск

```bash
./gradlew build publishToMavenLocal
cd browser
npm ci
npm test
npm run test:package
```

Координаты Java: `io.github.59alex:s3-file-streams[-s3|-webflux]:0.1.0`. Сначала соберите артефакты в Maven Local; публикация в реестры — отдельный [этап выпуска](ROADMAP.md). В [руководстве](docs/0.1.0/ru.md) каждая возможность сопровождается примером.

На Garage 2.4.1 проверены пустой и неизвестный размер, границы внутренних частей 5 MiB, диапазоны, отмена и ошибки. Реальный объект 2 GiB + 1 байт прошёл загрузку и скачивание с совпадающим SHA-256 при heap 96 MiB. Повторяющиеся тестовые данные сжимались самим Garage; это проверка корректности и ресурсов, а не универсальный бенчмарк. [Измерения и ограничения](docs/VERIFICATION.md).

Для браузерной загрузки нужен реально работающий транспорт request streams, обычно HTTP/2 с TLS. Проверка конструктора Request не доказывает поддержку прокси или сети. Скрытого накопления файла в Blob нет.

[Демонстрация](docs/0.1.0/ru.md#локальная-демонстрация) работает на loopback без авторизации. Приложение отвечает за доступ, ключи объектов, CORS, настройки S3 и прокси. Отмена при финализации может пересечься с успешным сохранением; потерянный ответ означает неизвестный результат. Автоповтора одноразового потока нет. ETag — непрозрачный идентификатор, не контрольная сумма.

[MIT](LICENSE) · 2026 Aleksandr Kashevarov.

</details>
