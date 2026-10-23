# s3-file-streams

[EN](README.md) · [RU](README.ru.md) · [Version 0.1.0 guide](docs/0.1.0/en.md) · [All versions](docs/README.md)

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
