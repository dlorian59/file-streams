# Verification — 0.1.0

[← Back to documentation and versions / Назад к документации и версиям](index.md)

Executed locally on 24 October 2026 (Asia/Yekaterinburg). This records functional checks and sampled process memory, not a statistical benchmark or certification of other providers.

## Follow-up integration check — 4 November 2026

The project and active Maven/npm coordinates were renamed to `file-streams`; Java packages and streaming algorithms were preserved. Historical measurements below and their raw evidence remain the 24 October results. After the rename: Java build, the 20 non-storage tests, packaged Maven consumer, 27 browser tests, annotated TypeScript compilation and npm-pack consumer passed. The seven Garage tests and opt-in memory test were not rerun: Docker could not start the fixture because a parent snapshot was missing.

The companion Boot 4.1.2 starter was exercised with real Chromium HTTP/2/TLS and an explicitly bounded in-memory test storage, not S3. A 45-byte upload/commit/download matched byte-for-byte. Cancellation with a declared 1 MiB source produced client `ABORTED`/unknown and a subsequent 404. In the unknown-length cancellation case, the client likewise reported `ABORTED`/unknown, but the fixture retained 8,192 bytes. The transport-level cause is not established. This observation does not certify or disprove S3 cleanup; it makes the distinction between local source cancellation, observed HTTP EOF, and remote outcome explicit. Known size should be sent when available; unknown outcome never promises rollback.

## Environment and dependencies

Linux x86_64; Gradle 9.8.2; Java 17.0.21, 21.0.9, 25.0.1 and 27.0.1.0.2. All library classes target Java 17 (major 61), without preview features. Reactor 3.8.8, AWS SDK 2.57.3, Spring WebFlux 7.0.10, Reactor Netty 1.3.8, aligned Netty 4.2.19.Final, Jackson 3.2.4. The S3 module exports the Netty BOM: a mixed 4.1 codec / 4.2 codec-base graph failed the first combined demo and is not a supported configuration.

Storage: isolated single-node Garage 2.4.1, official image digest `sha256:9c96caa2612d3411acc5b0e6701fb238dbfba33e533a6d7d3d811a4b12d0d020`, region garage, path-style access, loopback only. MinIO official binary downloads were unavailable, so no MinIO result is claimed. AWS S3 has not been tested.

## Java matrix

| Runtime | Tests passed | Separately opt-in memory test | Evidence |
| --- | ---: | --- | --- |
| 17 | 27 | excluded from ordinary build | [Report](evidence/java17.txt) |
| 21 | 27 | excluded from ordinary build | [Report](evidence/java21.txt) |
| 25 | 27 | excluded from ordinary build | [Report](evidence/java25.txt) |
| 27 | 27 | excluded from ordinary build | [Report](evidence/java27.txt) |

Nine core tests check demand, buffer position, long counters above 2 GiB, bounds/length, one-shot reuse, active cancellation, ranges and download cleanup. Twelve S3 tests cover seven actual Garage scenarios plus five contract tests; six WebFlux tests cover pooled memory release, invalid metadata, length/buffer failures, range headers and unknown storage outcomes.

Storage scenarios include 0, 1, 65,537 bytes; 5 MiB minus one, exactly 5 MiB, plus one; and 12 MiB. Source failure and cancellation observe no remaining matching multipart uploads and HEAD 404. A storage failure releases admission. Download cancellation and closing an unread body release admission. A malformed acknowledgement terminates with an error instead of hanging the sink. A custom client that never consumes the source cannot report a successful UploadResult.

The follow-up resource audit adds four regressions: unfinished upload sources are cancelled on SDK failure; malformed GET metadata cancels the response and releases admission; closing an active download terminates the reader instead of leaving it waiting; truncated and oversized download bodies fail validation and release their permit. The existing byte-stream validator is shared by upload and download. The Java matrix, packaged Maven consumer and 27 browser unit tests plus npm-pack consumer were rerun after these changes. All 109 local Markdown links and anchors resolved; guides are repository Markdown, with no guidance website or Pages workflow.

## Memory and large object method

The opt-in MemoryProbeTest generates 64 KiB buffers on demand; byte at offset n is n modulo 251. It updates SHA-256 while sending and receiving without collecting the object. The SDK uses internal 5 MiB parts, API buffer 10 MiB and one in-flight part. Only one transfer is measured. JVM heap maximum is 96 MiB and direct-memory maximum 64 MiB. A 10 ms sampler reads MemoryMXBean heap usage, direct BufferPoolMXBean memory and Linux VmRSS.

Garage uses its default Zstd compression level 1 and content-addressed blocks. This highly repeating payload compresses and deduplicates strongly: after the earlier 256 MiB / 64 MiB / 2 GiB checks, the entire fixture occupied approximately 12 MiB on disk. Every logical byte still traversed the Java upload and download and participated in both digests. These times do not represent incompressible payloads, remote network latency, production disk throughput or browser heap usage.

Aligned-dependency measurements are in the raw text files below. The 2 GiB + 1 byte and slow-sink cases were rerun after the resource audit; the 256 MiB case records the earlier implementation check. Values are sampled used memory of the whole test JVM, not a proof of retained live-set size and not a per-transfer RSS guarantee.

| Workload | Sampled heap / direct / RSS (MiB) | Time, single run | Raw result |
| --- | --- | --- | --- |
| 256 MiB | 76.99 / 1.24 / 230.89 | 4.169 s | [Result](evidence/memory-268435456.txt) |
| **2 GiB + 1 byte**, real SHA-256 round trip | 50.33 / 0.04 / 240.89 | 15.649 s | [Result](evidence/memory-2147483649.txt) |
| 64 MiB, slow sink: 1 ms per emitted buffer, prefetch 1 | 44.31 / 0.04 / 216.00 | 11.688 s | [Result](evidence/memory-67108864.txt) |

The core also has a separate counter-only test above 2 GiB; that test is not substituted for the real storage result. No custom multipart implementation is benchmarked: the selected implementation is the stock AWS engine, evaluated before and after integration.

## Browser → Java → storage

Headless Chromium 156 passed seven actual cross-origin checks through HTTPS/HTTP2 on the Java demo and Garage: known 6 MiB + 17 bytes, unknown 1 MiB + 13 bytes, empty body, SHA-256 full downloads, a 53-byte range crossing the 5 MiB boundary, a 31-byte suffix, missing-object rejection before sink writes, and abort after 6 MiB source-read progress. [Raw browser report](evidence/browser-java-garage.json).

For that browser abort, a subsequent independent SDK query found zero matching incomplete multipart uploads and HEAD 404. [Server observation](evidence/browser-abort.txt). This is one observed pre-commit case; finalization races remain ambiguous.

The browser module separately passed 27 Node unit tests and an npm-pack consumer check. Its synthetic HTTP2 fixture passed ten transport checks including arrival before source EOF, invalid acknowledgements, network reset and sink errors. HTTP1/TLS request streaming was rejected despite a positive constructor probe. Firefox 156 reported unsupported request streams before reading/locking the source. Safari, HTTP3, CDNs and reverse proxies have not been tested. See [browser API and compatibility](../browser/README.md).

## Packaging and remaining gates

Java sources/Javadoc/MIT artifacts build and publish to an isolated local Maven repository. An independent Gradle consumer resolves only those packaged modules and runs without source-project dependencies. CI repeats the Java matrix with an isolated Garage fixture and browser unit/package checks; local success does not itself mean a GitHub Actions run has completed.

Remaining validation: real AWS S3 and MinIO, production reverse-proxy buffering and timeout settings, browser memory profiling under sustained slow sinks, multi-transfer memory capacity planning, complete-multipart response loss / permission-denied abort fault injection, release signing/provenance and registry publication. There is no universal browser claim or exactly-once claim.

[← Back to documentation and versions / Назад к документации и версиям](index.md)
