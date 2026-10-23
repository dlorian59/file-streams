# Implementation plan — 0.1.0

[← Back to repository README / Назад к README библиотеки](README.md)

Continuous browser ↔ Java ↔ S3 streaming is implemented and locally verified. Checked items below are backed by [verification evidence](docs/VERIFICATION.md); publishing and additional provider/proxy gates remain open.

## Contracts and implementation

- [x] Known/unknown length, restricted metadata, maximum size, cancellation, errors and completion contracts.
- [x] Buffer ownership and release rules at the browser, WebFlux, Reactor and SDK boundaries.
- [x] One raw HTTP body per upload, with internal multipart hidden from callers. Multipart/form-data is outside v0.1.
- [x] Evaluate the stock AWS async client and retain it after real boundary, abort, digest and memory checks.
- [x] Document one-shot source behavior: no automatic replay; fresh sources and application reconciliation are required.
- [x] Java core/S3/WebFlux independent of authorization, databases and event systems.
- [x] Demand-aware uploads/downloads, long counters, bounded transfer admission and size checks.
- [x] Confirm success only after source validation and storage acknowledgement; test incomplete-multipart cleanup after error/cancel.
- [x] Document commit/cancel races, failed abort and crash lifecycle cleanup.
- [x] Pooled DataBuffer release tests; validated download lengths, ETags, range semantics and status mapping.
- [x] Browser ReadableStream upload and WritableStream download with AbortSignal, progress and strict responses.
- [x] Distinguish bytes-read progress from confirmed commit; no whole-file Blob fallback.
- [ ] Optional React adapter, after the independent browser core stabilizes.

## End-to-end evidence

- [x] Real Garage 2.4.1 upload/download and SHA-256 round trips.
- [x] Empty, unknown length, exact 5 MiB boundaries and a **real 2 GiB + 1 byte object**.
- [x] Large generated object under a 96 MiB heap with heap/direct/RSS samples; disclose compressible payload and Garage compression.
- [x] Slow JVM download sink with bounded prefetch and separate memory observation.
- [x] Browser disconnect and source/storage failures; source termination, pooled-buffer release and matching incomplete-upload cleanup.
- [x] Direct browser HTTP/2 TLS → Java → Garage pipeline, with HTTP compression disabled.
- [x] Constructor-probe false positive demonstrated on HTTP1; unsupported Firefox upload rejected before reading source.
- [x] Record stock-SDK configuration, raw measurements and dependency-alignment fix; no custom-engine superiority claim.
- [ ] Browser heap profiling with a sustained slow destination and representative production files.
- [ ] Multiple concurrent transfers: capacity measurements under sustained load.
- [ ] Production reverse-proxy/CDN streaming, buffering and timeout verification.
- [ ] Independently test MinIO and AWS S3; Garage does not establish either one's compatibility.
- [ ] Inject lost CompleteMultipartUpload responses and denied/failed AbortMultipartUpload; lifecycle recovery validation.

## Release gate

- [x] Standalone local HTTP/2 demo and Maven consumer smoke example.
- [x] English/Russian README and versioned Markdown guides; Java 17/21/25/27 feature/support matrix.
- [x] Architecture, HTTP contract, source ownership, memory method and compatibility boundaries.
- [x] Locally passing Java runtime matrix, sources/Javadoc/MIT packages and isolated Maven consumer.
- [x] Browser unit tests and npm-pack consumer; actual Chromium integration evidence.
- [x] CI workflow for Java matrix with Garage and browser unit/package checks.
- [x] Standalone source with no private application configuration, corporate source history or business DTO dependencies.
- [ ] Verify hosted CI outcomes after publication of source changes.
- [x] Verify Maven Central namespace `io.github.59alex`.
- [ ] Verify npm package availability and configure release signing/provenance.
- [ ] Publish versioned Java/TypeScript registry artifacts and release changelog.
- [ ] Integrate released artifacts into an application through its normal review workflow.

No exactly-once guarantee, universal browser support, AWS/MinIO certification or general throughput benchmark is claimed.

[← Back to repository README / Назад к README библиотеки](README.md)
