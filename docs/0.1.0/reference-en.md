# 0.1.0 — complete API and capability reference

[← Guide](en.md) · [Versions](../index.md) · [README](../../README.md) · [RU](reference-ru.md)

[Capability map](#capability-map) · [Java API](#java-api) · [S3 profile](#s3-profile) · [Browser API](#browser-api) · [Errors and HTTP](#errors-and-http) · [Ownership and AOP](#ownership-and-aop) · [Troubleshooting](#troubleshooting)

This reference inventories the public surface of the core, S3, WebFlux and browser modules. Use the [guide](en.md) for installation and the first transfer. Java requires JDK 17+; browser tooling requires Node 20+ and TypeScript with DOM types. Real browser uploads require supported Fetch request streams and a verified HTTP/2/TLS or HTTP/3 route. Storage access, a bucket and credentials are needed only for the S3 examples. [Verification](../VERIFICATION.md) distinguishes current runs from historical measurements.

## Capability map

| Need | API and runnable/compiled source | Boundary |
| --- | --- | --- |
| Validate bytes and declared length | `CheckedStream`, `StreamLimits`; [core consumer](../../examples/consumer/src/main/java/CoreCapabilities.java) | One subscription, bounded source chunks; no storage commit implied. |
| Supply key, MIME type and S3 metadata | `UploadRequest`; [core consumer](../../examples/consumer/src/main/java/CoreCapabilities.java) | HTTP deliberately supplies no arbitrary metadata. |
| Receive a raw request and confirm storage | `ObjectStreams.upload`; [Java receive/receiveBefore](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java) ↔ [TypeScript sendFile](../../browser/demo/transfer-example.ts) | Progress is distinct from commit; timeout is not rollback. |
| Download or inspect a range | `ObjectDownload`, `ByteRange`; [Java countDownload](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java) ↔ [TypeScript saveFile/saveRange](../../browser/demo/transfer-example.ts) | One body subscription; consume or close. No automatic resume. |
| Configure bounded AWS multipart | `S3Clients`, `S3ObjectStreams`; [application setup](../../demo/src/main/java/io/github/alex59/filestreams/demo/DemoServer.java) | Internal multipart only; caller owns the client. |
| Expose HTTP PUT/GET | `ObjectStreamHandler`; [handler source](../../webflux/src/main/java/io/github/alex59/filestreams/webflux/ObjectStreamHandler.java) | Fixed `/objects/**`; host supplies security, CORS and key policy. |
| Generate an unknown total or supply a custom transport | [sendGenerated](../../browser/demo/transfer-example.ts) | Always maxBytes; a custom Fetch must preserve streaming/cancellation. |
| Check browser APIs and report failures | `streamCapabilities`, `TransferError`; [inspectSupport/describeFailure](../../browser/demo/transfer-example.ts) | Syntax probe is not transport certification. |
| Use authentication headers or cookies | `headers`, `credentials`; [sendAuthorizedFile](../../browser/demo/transfer-example.ts) | Host CORS must allow Authorization and credentials; the local demo does not enable these by default. |

Run the storage-free examples from the repository root after the [local build](en.md#1-install-modules-locally):

```bash
./gradlew publishToMavenLocal -Dmaven.repo.local="$PWD/build/local-repository"
./gradlew -p examples/consumer run
./gradlew :demo:compileJava
cd browser
npm ci
npm test
```

Expected consumer lines include `Core examples passed: metadata, checked bytes, ranges, cleanup, typed failure` and `Published-artifact consumer passed: core + S3 + WebFlux`. `npm test` compiles the annotated TypeScript example as well as running browser-module unit tests. Browser functions are called from the application with a real File, AbortSignal and writable destination; they are not Node CLI commands.

## Java API

All types below use `io.github.alex59.filestreams`, except `.s3` and `.webflux` adapters. Value records expose their named accessors and standard record equality; no mutable setters are generated.

| Public surface | Contract |
| --- | --- |
| `ObjectStreams.upload(UploadRequest, Publisher<ByteBuffer>) → Mono<UploadResult>` | Adapter operation starts on subscription. A successful result requires validated EOF and storage acknowledgement. Give each new transfer a fresh source; do not apply retry/repeat to a consumed body. |
| `ObjectStreams.download(String key, String range) → Mono<ObjectDownload>` | `null` range requests the full object; otherwise strict single HTTP byte-range syntax. The emitted download owns a live body/resource. |
| `UploadRequest(key, length, contentType, metadata)` | All inputs non-null; key is 1–1024 UTF-8 bytes without NUL. `OptionalLong.empty()` means unknown length; present length is nonnegative. MIME type is nonblank without CR/LF. Metadata is defensively copied via `Map.copyOf` (null entries rejected). S3 service metadata limits still apply. |
| `UploadRequest.unknownLength(key)` | Uses unknown length, `application/octet-stream`, empty metadata. |
| `UploadResult(committed, key, bytes, etag)` | Constructor accepts only `committed=true`, nonnegative bytes, non-null key and nonblank ETag. It describes a confirmed commit, not an error state. ETag is opaque. |
| `StreamLimits(maxBytes, maxBufferBytes)` / `defaults()` | Nonnegative `long` maxBytes; positive `int` maxBufferBytes. Defaults: 16 GiB / 1 MiB. In `S3ObjectStreams` these are **upload** limits; they do not restrict the length of an existing object downloaded later. |
| `CheckedStream(source, expectedLength, limits)` | Validates non-null arguments and declared size before reading. Requests one source buffer at a time. Emits read-only views without changing the original position; caller must not mutate the underlying bytes until transfer termination. |
| `CheckedStream.content()` | One-shot `Flux<ByteBuffer>`; verifies chunks, total size and EOF. A second subscription produces `SOURCE_REUSED`. No whole-object aggregation. |
| `CheckedStream.bytes()` / `completed()` | Validated byte count as `long`; completed means valid source EOF, **not** storage commit. |
| `CheckedStream.cancel()` | Cancels an active source, or remembers cancellation before subscription; active content terminates with `CANCELLED`. It does not delete storage objects. |
| `ObjectDownload(length, contentType, etag, range, source, cleanup)` | Nonnegative length; type, ETag, source and cleanup non-null. Optional range length must equal body length. Intended for adapters/custom stores. |
| `ObjectDownload.length/contentType/etag/range()` | Announced metadata; range is null for a full response. The body length is checked independently while consuming it. |
| `ObjectDownload.body()` / `close()` | Body is one-shot; a repeat/closed-body access fails. Success, error, downstream cancellation or explicit close runs cleanup once. Close on active content signals `CANCELLED`; close before reading cancels the unopened source. |
| `ByteRange(start, end, total)` | Inclusive indices with `0 ≤ start ≤ end < total`. `length()` is `end-start+1`; accessors expose all three values. |
| `ByteRange.parse(header, total)` | `bytes=a-b`, `bytes=a-`, `bytes=-suffix`; clips an oversized end/suffix. Rejects multiple ranges, empty objects, zero suffix, invalid/overflowing numbers and headers over 100 characters. |
| `ByteRange.requestHeader()` / `responseHeader()` | Produce `bytes=a-b` and `bytes a-b/total`. |
| `ByteRange.RangeNotSatisfiable(total)` / `total()` | Typed range failure; a negative total means size is unknown, so no `Content-Range: bytes */total` can be emitted. |
| `StreamFailure(code, message)` / `code()` | Source/admission error. Codes: `TOO_LARGE`, `LENGTH_MISMATCH`, `BUFFER_TOO_LARGE`, `CANCELLED`, `SOURCE_REUSED`, `BUSY`. |
| `ObjectUnavailable(reason, cause)` / `reason()` | `NOT_FOUND` for missing download; `CHANGED` for HEAD/conditional-GET mismatch. Preserves the cause. |

[Core source directory](../../core/src/main/java/io/github/alex59/filestreams) · [download lifetime example](../../demo/src/main/java/io/github/alex59/filestreams/demo/TransferExamples.java).

## S3 profile

`S3Clients.bounded(URI endpoint, Region region, AwsCredentialsProvider credentials)` returns an application-owned `S3AsyncClient`; `endpoint=null` uses the AWS endpoint. Region/credentials belong to the application. `new S3ObjectStreams(client, bucket, limits, maxTransfers)` requires a non-null client/bucket/limits and positive admission count; it does not close the client. Credentials provider ownership is also the application's responsibility outside the Boot starter.

| Fixed profile / public constants | Value and consequence |
| --- | --- |
| `PART_BYTES` | 5,242,880 bytes (5 MiB), internal S3 part/threshold size. |
| `SDK_BUFFER_BYTES` | 10,485,760 bytes (10 MiB), SDK API buffer setting, not total process RSS. |
| Parts in flight | 1 per upload. |
| Maximum configured upload size | 10,000 × 5 MiB = 52,428,800,000 bytes. Even declared lengths use the unknown-length SDK body so full EOF/trailing-byte validation runs. This is this profile's bound, not the S3 service's general maximum. |
| Admission | One shared semaphore for uploads and downloads. Excess requests fail `BUSY`; no unbounded queue. Upload holds its slot until the SDK future terminates, including cleanup after cancellation. Download holds its slot until body termination/close. |
| HTTP connections / pending acquisition | 8 / 16. |
| Connection / read / write timeouts | 5 s / 60 s / 60 s. |
| SDK attempt / API call timeouts | 60 s / 10 min. These do not replace an application deadline for an indefinitely stalled source. |
| Retry strategy | One attempt. No transparent source replay. |
| Endpoint / proxy | Path-style addressing; ambient environment/system-property HTTP proxy configuration disabled. Supply your own compatible client for an explicit proxy or another profile. |
| Checksums | AWS request/response checksum policy `WHEN_REQUIRED`; ETag still does not provide application integrity verification. |

Full downloads use HEAD then If-Match GET with an explicit full range to avoid SDK multipart download metadata changes. S3 metadata length, ETag, range and identity content encoding are validated before exposing the body; actual body length is checked during consumption. `limits.maxBytes/maxBufferBytes` do not impose a download policy. Apply a host-side download-size policy if needed, and set browser `maxBytes` independently.

Before EOF, local Java subscription cancellation signals a source failure while keeping the SDK future alive for its abort path. Cleanup is best effort: process crashes, network failures or denied AbortMultipartUpload can leave parts. Configure lifecycle cleanup. HTTP termination can instead be observed as normal EOF; for unknown length this may accept a received prefix. A cancelled browser promise remains unknown. Supply a known length when available and reconcile ambiguous objects before a new transfer. [Architecture](../ARCHITECTURE.md) · [source](../../s3/src/main/java/io/github/alex59/filestreams/s3/S3Clients.java).

## Browser API

Exports are defined in [index.ts](../../browser/src/index.ts). No runtime framework dependency is required.

### Options

| Field | UploadOptions | DownloadOptions | Meaning/default |
| --- | --- | --- | --- |
| `url: string \| URL` | required | required | HTTP adapter endpoint; `objectUrl` builds it safely. |
| `key: string` | required | — | Non-empty logical key; commit response must match it exactly. |
| `source: ReadableStream<Uint8Array>` | required | — | Unlocked one-shot byte source. |
| `destination: WritableStream<Uint8Array>` | — | required | Unlocked sink; operation owns writer, closes on success, aborts on failure. |
| `maxBytes: number` | required | required | Nonnegative safe integer, including zero. Upload checks before forwarding each chunk; download checks metadata before writing and actual bytes during reading. |
| `knownLength?: number` | optional | — | Nonnegative safe integer ≤ maxBytes; validates locally and sends X-Upload-Length. Prefer `file.size`. |
| `range?: ByteRange` | — | optional | `{start,end?}` or `{suffix}`; start/end nonnegative safe integers, end ≥ start, suffix > 0. Do not mix both forms. |
| `signal?: AbortSignal` | optional | optional | Aborts transport and owned streams; a pre-aborted signal does not start Fetch. Already written bytes/remote commit are not rolled back. |
| `headers?: HeadersInit` | optional | optional | Application headers. Upload rejects caller Content-Length, X-Upload-Length, Content-Range; download rejects caller Range (use the option). Upload defaults Content-Type to application/octet-stream. |
| `credentials?: RequestCredentials` | optional | optional | Defaults to `same-origin`. Cross-origin cookies require matching CORS/host policy. |
| `onProgress?` | `UploadProgress` callback | `DownloadProgress` callback | Observational; thrown errors/rejected callback promises are ignored and do not alter a transfer. |
| `fetch?: typeof fetch` | optional | optional | Defaults to global Fetch. A custom transport must preserve streaming/AbortSignal; it is not a buffering fallback. |

Both functions use CORS mode, disabled redirects and `cache: no-store`. Browser byte counts must be ≤ `Number.MAX_SAFE_INTEGER` (9,007,199,254,740,991), regardless of Java's larger long range. The library cannot bound caller-allocated chunks, upstream queues, browser/network/proxy buffers or arbitrary sink internals.

### Functions and values

| Export | Result and limits |
| --- | --- |
| `uploadStream(options): Promise<UploadCommit>` | Resolves only for HTTP 201 application/json after source EOF, matching `committed:true`, key, bytes and a nonblank ETag. Commit/error metadata is capped at 16 KiB. A source-read progress event does not confirm storage. |
| `UploadCommit` | Frozen `{committed:true, key:string, bytes:number, etag:string}`. |
| `UploadProgress` | Frozen discriminated union: `{phase:'reading', bytesRead, knownLength}` or `{phase:'committed', bytesRead, commit}`. No percentage is invented for unknown length. |
| `downloadTo(options): Promise<DownloadResult>` | Validates status, visible headers, ranges, identity encoding and actual length; awaits each write, then closes sink after EOF validation. No whole-file Blob fallback. |
| `DownloadResult` | Frozen `{complete:true, bytesWritten, etag, contentType:string|null, range:DownloadRange|undefined}`. Completion means sink close resolved, not physical fsync. |
| `DownloadProgress` | Frozen `{bytesWritten, responseBytes}` after completed sink writes. |
| `ByteRange` / `DownloadRange` | Request union above; response `{start,end,total}` with inclusive indices. Partial requests require exact matching 206/Content-Range; full requests reject stray Content-Range. |
| `objectUrl(objectsBaseUrl, key): URL` | Absolute base URL without query/fragment; rejects empty key, `.` and `..`. Encodes the logical key once as one URL segment. Do not pre-encode. Invalid inputs can throw native TypeError. It is not a presigned-S3 URL/session builder. |
| `streamCapabilities(): StreamCapabilities` | Frozen `{requestStreamSyntax, responseStreams, transportVerified:false}`. Makes no request. Check real browser/proxy behavior independently. |
| `TransferError(code,message,details?)` | Error with `name='TransferError'`, `code`, optional `status`, optional `uploadOutcome`, optional `cause`. Exports `TransferErrorCode` and `UploadOutcome` unions. Download errors do not carry an upload outcome. |

### Error interpretation

| TransferErrorCode | Meaning / action |
| --- | --- |
| `INVALID_ARGUMENT` | Invalid bounds/options/source state; fix arguments before creating a fresh transfer. |
| `UNSUPPORTED` | Missing Fetch/request-stream syntax; choose a supported environment explicitly. |
| `ABORTED` | Signal cancellation; inspect uploadOutcome and reconcile storage if unknown. |
| `LIMIT_EXCEEDED` | maxBytes exceeded or announced length too large; do not silently raise limits. |
| `LENGTH_MISMATCH` | Actual bytes differ from declared/response length. |
| `HTTP_ERROR` | Unexpected HTTP status; `status` carries it. A valid server `{committed:false,...}` marks upload `rejected`. |
| `PROTOCOL_ERROR` | Inconsistent/hidden response headers, wrong range, encoded body, invalid/oversized/mismatching commit JSON. |
| `SOURCE_ERROR` | Read failure or non-Uint8Array upload chunk. |
| `SINK_ERROR` | Destination write/close failed; network source is cancelled. |
| `NETWORK_ERROR` | Fetch/response transport failed; upload outcome may be unknown. |

Upload outcomes: `not-started` means Fetch was not attempted; `rejected` means a structured explicit server refusal; `unknown` means a request was attempted without a validated commit or trustworthy rejection. None asserts that an older object with the same key is absent. Malformed native browser inputs may throw TypeError before ownership (notably `objectUrl`, download Headers/getWriter); catch `unknown` and narrow with `instanceof TransferError`, as [describeFailure](../../browser/demo/transfer-example.ts) does.

## Errors and HTTP

`ObjectStreamHandler(ObjectStreams, StreamLimits)` implements WebHandler. `handle(exchange)` serves `/objects/{key}` only. `declaredLength(HttpHeaders)` parses a single decimal Content-Length and/or X-Upload-Length (1–19 digits within nonnegative long); duplicates, comma lists, signs and conflicting values fail. WebFlux releases pooled request buffers after copying each bounded chunk. JSON errors can only replace an uncommitted response.

| HTTP status | Code / body | Trigger |
| --- | --- | --- |
| 201 | `{committed:true,key,bytes,etag}` | Confirmed upload. |
| 200 / 206 | Raw byte stream + length/type/ETag/range headers | Full / partial download. |
| 400 | `INVALID_REQUEST`, `LENGTH_MISMATCH`, `BUFFER_TOO_LARGE`, `CANCELLED`, `SOURCE_REUSED`; committed=false | Metadata or source contract failure before HTTP response commit. |
| 413 | `TOO_LARGE`; committed=false | Byte-stream size bound. |
| 503 | `BUSY`; committed=false | Shared admission exhausted. |
| 404 | `NOT_FOUND`; committed=false | Missing storage object. Invalid endpoint/empty key instead returns empty 404. |
| 405 | Empty response, `Allow: GET, PUT` | Unsupported method; OPTIONS preflight belongs to the application's CORS filter. |
| 412 | `CHANGED`; committed=false | Object changed between S3 HEAD and conditional GET. |
| 416 | `INVALID_RANGE`; committed=false | Invalid/unsatisfiable range. Content-Range `bytes */total` only if total is known. |
| 502 | `STORAGE_FAILURE`; committed=null | Storage did not confirm outcome or unexpected backend failure. |

401/403, host-specific statuses and access rules are supplied by the application, not invented by this adapter. Authentication headers are not copied to S3 metadata. Late download failures terminate the response stream; a JSON error cannot replace bytes already sent. See [HTTP contract](../HTTP-CONTRACT.md).

## Ownership and AOP

One operation owns each acquired browser reader/writer until termination. It releases locks; source failure/abort cancels reads; destination failure aborts writes. Abort returns promptly even for an uncooperative pending write, whose side effect may finish later. On Java, never close an application S3 client immediately after returning an unsubscribed Mono; close it after all active work ends. `ObjectDownload` is not a shareable cached value.

No retry/cache/repeat aspect is installed. These operators can subscribe again or retain a resource-backed value. Timing an ordinary method call measures construction of a lazy Mono, not the transfer; successful `download()` emits an opened body before that body finishes. Observe the reactive terminal signal or the application HTTP filter chain instead. Authorization belongs before body access in host filters. If an application chooses AOP, it must preserve one subscription, cancellation, close semantics and the difference between local completion and remote outcome; Spring proxy/self-invocation/final-class constraints still apply. [Spring proxy reference](https://docs.spring.io/spring-framework/reference/core/aop/proxying.html) · [Reactor Mono reference](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html).

## Troubleshooting

| Symptom | Check / recovery |
| --- | --- |
| Maven/npm cannot resolve 0.1.0 | Build local artifacts and install the packed browser package; registry publication is not claimed. |
| Request probe passes, upload Fetch fails | Check negotiated HTTP version, TLS trust, redirects, CORS preflight and actual proxy buffering. There is no hidden Blob fallback. |
| PROTOCOL_ERROR with correct server bytes | Expose ETag/length/range/encoding headers through CORS; disable compression; verify exact commit JSON/status/key. |
| SOURCE_REUSED or closed download | Obtain a fresh source/download; remove retry/repeat/cache around resource-backed streams. |
| BUSY does not clear immediately after cancel | SDK cleanup may still hold its slot; ensure opened downloads are consumed/closed. Do not bypass admission. |
| BUFFER_TOO_LARGE | Reduce publisher/network chunk size or deliberately configure a compatible buffer bound; a file-size cap alone is insufficient. |
| Aborted unknown-length upload left a short object | Reconcile by application upload identity; send knownLength when known. The observed HTTP/2 fixture accepted 8,192 bytes; exact transport cause is not established. |
| Changed object during ranged download | Treat 412 as version change; do not concatenate ranges from different object versions. No If-Range/resume protocol is supplied. |
| Mixed Netty versions fail at runtime | Retain the exported Netty BOM and inspect dependency resolution; do not mix 4.1/4.2 codec families. |
| Multipart parts remain after process/network failure | Check AbortMultipartUpload permissions and lifecycle cleanup; client cancellation alone proves neither cleanup nor rollback. |
| Docker snapshot or JDK 27 unavailable locally | Record the missing check. Do not interpret previous historical evidence as a new run. |

[← Guide](en.md) · [Versions](../index.md) · [README](../../README.md) · [RU](reference-ru.md)
