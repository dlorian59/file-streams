# Implementation plan

Prove a continuous browser ↔ Java ↔ S3 pipeline with documented resource limits.

## 1. Contracts and feasibility

- [ ] Define known/unknown length, metadata, maximum size, cancellation, error and completion contracts.
- [ ] Specify ownership and release rules for every buffer boundary.
- [ ] Choose a raw HTTP body for the baseline upload; evaluate PartEvent separately if form metadata is required.
- [ ] Measure the stock AWS async client before choosing SDK-managed versus custom internal multipart.
- [ ] Define one-shot stream retry behavior; require a fresh source factory for replay where needed.
- [ ] Establish a supported-browser/HTTP/proxy matrix and a testable memory budget.

## 2. Java implementation

- [ ] Build core and S3 adapter independently of application authorization/database/event systems.
- [ ] Implement demand-aware uploads and downloads, long byte counters and bounded concurrency.
- [ ] Finalize only after confirmed S3 completion; abort incomplete multipart on failure/cancel where possible.
- [ ] Define behavior when cancellation races with successful commit, and document server lifecycle cleanup for orphan uploads.
- [ ] Add WebFlux mapping with DataBuffer release/discard tests.
- [ ] Validate download headers, content length, range semantics and error mapping.

## 3. Browser implementation

- [ ] Stream ReadableStream to HTTP with AbortSignal and capability detection.
- [ ] Stream HTTP response to caller-provided WritableStream.
- [ ] Keep source-read progress distinct from final object commit.
- [ ] Propagate sink errors and cancellation; do not silently fall back to whole-file buffering.
- [ ] Provide an optional React adapter after the core is stable.

## 4. End-to-end evidence

- [ ] Run upload/download against real S3-compatible storage with SHA-256 round-trip checks.
- [ ] Cover empty, unknown-length, greater-than-2-GiB and exact internal-buffer-boundary sizes.
- [ ] Transfer a large generated file under a small JVM heap; record heap, direct memory, RSS and browser observations.
- [ ] Slow the sink and demonstrate that retained memory follows the stated budget.
- [ ] Disconnect the browser and inject storage failures; verify stream termination, released buffers and incomplete-upload cleanup.
- [ ] Verify streaming through the documented HTTP/TLS/reverse-proxy configuration, including disabled request/response buffering.
- [ ] Compare against the stock AWS SDK using the same workload; publish raw results and environment.
- [ ] Document tested compatibility separately for MinIO and AWS S3; one does not prove the other.

## 5. Release gate

- [ ] Public standalone demo, English quickstart, API docs, resource-budget explanation and compatibility matrix.
- [ ] Review source provenance; no private service configuration or business DTO dependencies.
- [ ] CI and package consumer smoke tests.
- [ ] Verify Maven namespace and npm scope/package availability; configure release signing/provenance.
- [ ] Publish versioned Java and TypeScript artifacts with changelog.
- [ ] Integrate the released artifacts into a real application through its normal review workflow.

No performance numbers, exactly-once behavior or universal browser support are claimed yet.
