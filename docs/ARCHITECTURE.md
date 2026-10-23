# Architecture and failure boundaries

The project solves continuous object transfer with explicit resource ownership. It does not implement application permissions, upload reservations, database transactions, event delivery, virus scanning or distributed commit.

## Boundaries

1. The browser provides a one-shot ReadableStream or a caller-owned WritableStream. Source-read progress is advisory; only the server acknowledgement represents storage commit.
2. WebFlux copies each incoming pooled DataBuffer into an owned heap buffer and releases the pooled buffer in a finally block. Discard hooks release queued DataBuffers. Outbound buffers pass through Spring's response lifecycle.
3. CheckedStream requests one upstream buffer at a time, preserves the caller's ByteBuffer position through a read-only view, checks size with overflow-safe long arithmetic and validates EOF. Caller buffers must remain immutable until transfer termination; there is no pooled reference-count ownership transfer in the core API.
4. S3ObjectStreams admits at most the configured number of uploads/downloads. Busy requests fail immediately. Downloads retain the permit until their body finishes or ObjectDownload.close is called.
5. The stock AWS multipart engine uses internal 5 MiB parts, a configured 10 MiB API buffer and at most one in-flight part per upload. Multipart is explicitly enabled. HTTP connection concurrency is eight, pending connection acquires sixteen. This is not a total process RSS cap: TLS, Netty pools, SDK metadata, application queues and the JVM add memory.

The default profile's unknown-length cap is 10,000 × 5 MiB; the default StreamLimits maximum is 16 GiB and the demo maximum 512 MiB. Source buffers default to at most 1 MiB. Known lengths are validated by the library but deliberately not given to the SDK: SDK truncation to a declared length must not bypass EOF/trailing-byte validation. Zero-length uploads still subscribe and validate their source.

## Commit and cancellation

```text
admitted → consume / validate → source EOF → SDK finalization → confirmed result
                 ↓ error/cancel           ↓ lost response or cancel
            SDK abort path                 outcome unknown
```

Cancellation before EOF first cancels the upstream source, then signals an error to the SDK. The SDK future is left alive so its multipart error path can issue AbortMultipartUpload. Cancelling that future prematurely can interrupt cleanup. Tests observe both ListMultipartUploads and HEAD after cancellation; a source-cancellation flag alone is insufficient evidence.

The SDK abort request is best effort. Process crashes, lost connectivity, denied AbortMultipartUpload or failed cleanup can leave parts behind. Configure a storage lifecycle rule for incomplete multipart uploads and monitor failures. A completed object is never deleted automatically to simulate rollback. If cancellation races with finalization, the object may exist even though the client saw no success.

There is no transparent retry of a consumed source. SDK retries are configured to one attempt; replay requires a newly obtained source and an application decision about ambiguous outcomes. A byte counter is not a commit ledger. SDK call timeouts bound individual calls, not the whole duration of an indefinitely stalled source: applications should impose a transfer deadline whose cancellation reaches the subscription.

## Download ownership

HEAD resolves one byte range and supplies the expected ETag. GET uses If-Match. A full-object GET explicitly requests its full range to avoid the multipart client's automatic split-download metadata behavior. The adapter validates length, ETag, Content-Range and identity encoding before exposing the body. Consumers must consume the one-shot ObjectDownload body or close it. WebFlux uses usingWhen across response writing so success, error and cancellation close the resource.

Backpressure limits what this library requests; it cannot repair an upstream Publisher that ignores demand, an eager application prebuffer, a proxy that buffers the request, or a writable sink that internally accumulates the file.

## Stock SDK decision

We retained the stock engine after real Garage boundary, source-failure, abort, digest and small-heap tests. No custom multipart state machine is introduced. Important observed configuration details are explicit multipartEnabled(true), two-part buffer capacity for unknown lengths, and full-range GET to preserve one download response. [Measurements and method](VERIFICATION.md).

Sources: [AWS async multipart](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/s3-async-client-multipart.html), [multipart buffer and concurrency configuration](https://docs.aws.amazon.com/java/api/latest/software/amazon/awssdk/services/s3/multipart/MultipartConfiguration.Builder.html), [Garage compression configuration](https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#compression_level).
