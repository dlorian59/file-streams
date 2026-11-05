# Attachments in an existing application

[← README](../../README.md) · [RU](integration-ru.md) · [Reference](reference-en.md) · [Detailed Java guide](en.md)

Use a configured client when several UI actions share an endpoint and size policy. Keep `uploadStream` and `downloadTo` for per-request control; the facade calls those same engines.

## Configure once

```ts
import { createFileStreams } from '@59alex/file-streams';
const files = createFileStreams({
  objectsUrl: '/attachments',
  profile: 'attachments',
  credentials: 'same-origin',
});
```

`objectsUrl` is an absolute or browser-relative URL without query/fragment. In a browser, relative URLs resolve against the current page URL. Constructing the client performs no network I/O; Node and SSR require an absolute URL because there is no browser location. Default profile: `attachments`; default credentials: `same-origin`. Optional `maxBytes` overrides the profile for both upload and download. `headers` are copied at construction; operation headers override matching defaults. For refreshed credentials use operation headers or the application's authenticated `fetch`, which must preserve streaming and abort behavior. The client does not log in or refresh credentials.

| Profile | Maximum object/response | Java equivalent |
| --- | --- | --- |
| `attachments` | 64 MiB | `StreamProfile.ATTACHMENTS` |
| `media` | 512 MiB | `StreamProfile.MEDIA` |
| `archives` | 16 GiB | `StreamProfile.ARCHIVES` |

Java `StreamLimits.defaults()` retains its original `ARCHIVES` bound; the convenience browser client starts more conservatively with attachments. Configure both ends deliberately. Java profiles use a 1 MiB maximum individual source buffer. These are limits, not memory allocations or performance claims. Browser download limits and server upload limits are separate checks.

## Save a selected file

```ts
const commit = await files.uploadFile('alice:invoice.pdf', file, {
  signal: controller.signal,
  onProgress: progress => updateProgress(progress),
});
```

`file` is a File/Blob. The method forwards its size as `X-Upload-Length`, uses its nonempty content type unless overridden, and opens a new stream for this call. The promise resolves only after strict HTTP 201/key/byte-count/ETag validation. No buffering, multipart session or retry loop is added. A new call is still an application decision: a previous unknown result must be reconciled before repeating a write.

[Complete commented application example](../../browser/demo/application-files.ts), with imports and functions to bind to a file input and a save action. The server [account-scoped controller](https://github.com/dlorian59/file-streams-spring-boot-starter/blob/main/examples/consumer/src/main/java/com/example/attachments/AttachmentController.java) checks the trusted principal before body subscription. Account IDs in keys come from the application's authenticated session; a client header is not authentication.

## Generated content

```ts
const committed = await files.upload('alice:report.csv', reportStream, {
  headers: { 'Content-Type': 'text/csv' },
  signal: controller.signal,
});
```

`reportStream` is a one-shot `ReadableStream<Uint8Array>` produced by the application. The configured maximum still applies. If the final size is known, pass `knownLength`; do not set `X-Upload-Length` manually. A fresh source is necessary for any explicitly authorized retry. [Annotated generated-stream example](../../browser/demo/transfer-example.ts).

## Download and preview

```ts
await files.download('alice:invoice.pdf', destination, {
  signal: controller.signal,
  onProgress: progress => updateDownload(progress),
});
await files.download('alice:report.csv', previewDestination, { range: { start: 0, end: 255 } });
```

Each destination is an unlocked application-owned WritableStream. Successful transfer closes it; failure aborts it. A file picker must be invoked from a user gesture in a browser that supports it. There is no fallback to a whole-file Blob. A range uses HTTP 206 and matching Content-Range; an ignored range is an error. Each operation needs a fresh destination. [Complete range and transport examples](../../browser/demo/transfer-example.ts).

## Outcomes

`onProgress` observes bytes read/written and cannot turn a failed transfer into success. Only the returned upload commit confirms storage. Cancellation and lost responses can leave an unknown outcome; the facade preserves `TransferError.code`, `.status` and `.uploadOutcome`. For unknown-length HTTP, transport termination observed as normal EOF may allow a received prefix to commit. Known length detects a short body but does not guarantee rollback. [Full HTTP/error contract](../HTTP-CONTRACT.md).

## Java without Boot

Use Maven dependencies `io.github.59alex:file-streams-s3:0.1.0` and, for HTTP, `io.github.59alex:file-streams-webflux:0.1.0` (the core is transitive). The [Java guide](en.md#1-install-modules-locally) gives the Gradle configuration; Maven uses the same group/artifact/version in ordinary `<dependency>` elements. Artifacts must currently be built locally; registry publication is pending.

```java
try (var credentials = DefaultCredentialsProvider.builder().build();
     var client = S3Clients.bounded(Region.EU_WEST_1, credentials)) {
    var objects = new S3ObjectStreams(client, "application-attachments", StreamProfile.ATTACHMENTS);
    // Keep this scope alive until all transfers finish.
}
```

[Complete plain Java source and imports](../../examples/consumer/src/main/java/NamedProfiles.java). No null endpoint or positional transfer count is needed; the application still owns and closes the client. For custom limits/concurrency, the existing explicit constructor remains.

```java
StreamLimits limits = StreamProfile.ATTACHMENTS.limits();
UploadRequest request = UploadRequest.knownLength(key, size, contentType);
Mono<UploadResult> commit = objects.upload(request, body);
Mono<ObjectDownload> opened = objects.download(key);
```

These statements belong in the [fully annotated Java transfer example](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java); imports are `io.github.alex59.filestreams.*` and `reactor.core.publisher.Mono`. The application owns its S3 client and closes opened downloads on all termination paths. [Resource-safe download recipe](en.md#5-download-and-close). For Boot, let the [starter](https://github.com/dlorian59/file-streams-spring-boot-starter) own the clients and HTTP lifecycle.

## Diagnose integration

- HTTP 401/403: check application authentication and object authorization; the library does not bypass them.
- Network failure before a commit: verify request streaming through browser/proxy/HTTP2-TLS; do not infer rollback.
- Protocol error on download: expose ETag, Accept-Ranges, Content-Length and Content-Range through CORS; disable content compression for these byte routes.
- Limit error: use the same named profile at both ends or explicitly configure matching bounds. Server limits still enforce storage independently of the client.
- Unexpected automatic retries in an HTTP wrapper: remove retry of one-shot bodies. An authentication wrapper may retry only if it can supply a fresh body and has resolved the earlier write outcome.

[Reference with all original primitives](reference-en.md) · [Development and storage demo](en.md#local-end-to-end-demo) · [← README](../../README.md)
