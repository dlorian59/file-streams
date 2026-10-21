# s3-file-streams

A planned set of Java and TypeScript modules for continuous file transfer through an application server to S3-compatible storage.

**Status: design and implementation planning. No installable release is available yet.**

## Intended flow

```text
Browser ReadableStream → HTTP → Java reactive stream → S3
Browser WritableStream ← HTTP ← Java reactive stream ← S3
```

The application-facing API uses one continuous stream per transfer. S3 multipart may be used internally with a documented buffer budget; callers will not manage file parts or upload sessions.

## Planned modules

| Module | Responsibility |
| --- | --- |
| Java core | Byte-stream contracts, resource ownership, cancellation and transfer results |
| Java S3 adapter | AWS SDK integration, completion and failed-upload cleanup |
| Java WebFlux adapter | HTTP request/response streaming and buffer lifecycle |
| Browser TypeScript core | ReadableStream/WritableStream, AbortSignal and progress |
| Optional React adapter | UI state and hooks over the independent core |

Names and package coordinates are provisional.

## Design goals

- Explicit memory budgets, including SDK/direct buffers and in-flight transfers.
- Cancellation propagated across the chain with resource cleanup.
- Distinct progress states for bytes read and storage commit.
- Streaming download to a caller-provided writable sink.
- Capability detection and clear unsupported-mode errors.

Browser streaming uploads and file-system sinks have compatibility limits. A strict streaming API must not silently collect the entire file into a Blob. Transparent replay of a consumed, non-rewindable upload stream is outside the initial contract.

## Implementation

See [the v0.1 plan](ROADMAP.md) and [open issues](https://github.com/59Alex/s3-file-streams/issues).

## References

- [AWS asynchronous S3 multipart support](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/s3-async-client-multipart.html)
- [Browser request streaming and its constraints](https://developer.chrome.com/docs/capabilities/web-apis/fetch-streaming-requests)
- [Spring WebFlux streaming multipart events](https://docs.spring.io/spring-framework/reference/web/webflux/controller/ann-methods/multipart-forms.html)

## License

[MIT](LICENSE).
