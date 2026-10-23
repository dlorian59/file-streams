# Verification — 0.1.0

Executed locally on 24 October 2026 (Asia/Yekaterinburg). This records functional checks and sampled process memory, not a statistical benchmark or certification of other providers.

## Environment and dependencies

Linux x86_64; Gradle 9.8.2; Java 17.0.21, 21.0.9, 25.0.1 and 27.0.1.0.2. All library classes target Java 17 (major 61), without preview features. Reactor 3.8.8, AWS SDK 2.57.3, Spring WebFlux 7.0.10, Reactor Netty 1.3.8, aligned Netty 4.2.19.Final, Jackson 3.2.4. The S3 module exports the Netty BOM: a mixed 4.1 codec / 4.2 codec-base graph failed the first combined demo and is not a supported configuration.

Storage: isolated single-node Garage 2.4.1, official image digest `sha256:9c96caa2612d3411acc5b0e6701fb238dbfba33e533a6d7d3d811a4b12d0d020`, region garage, path-style access, loopback only. MinIO official binary downloads were unavailable, so no MinIO result is claimed. AWS S3 has not been tested.

## Java matrix

| Runtime | Tests passed | Separately opt-in memory test | Evidence |
| --- | ---: | --- | --- |
| 17 | 23 | excluded from ordinary build | [Report](evidence/java17.txt) |
| 21 | 23 | excluded from ordinary build | [Report](evidence/java21.txt) |
| 25 | 23 | excluded from ordinary build | [Report](evidence/java25.txt) |
| 27 | 23 | excluded from ordinary build | [Report](evidence/java27.txt) |

Seven core tests check demand, buffer position, long counters above 2 GiB, bounds/length, one-shot reuse, active cancellation, ranges and download cleanup. Ten S3 tests cover seven actual Garage scenarios plus three contract tests; six WebFlux tests cover pooled memory release, invalid metadata, length/buffer failures, range headers and unknown storage outcomes.

Storage scenarios include 0, 1, 65,537 bytes; 5 MiB minus one, exactly 5 MiB, plus one; and 12 MiB. Source failure and cancellation observe no remaining matching multipart uploads and HEAD 404. A storage failure releases admission. Download cancellation and closing an unread body release admission. A malformed acknowledgement terminates with an error instead of hanging the sink. A custom client that never consumes the source cannot report a successful UploadResult.

## Memory and large object method

The opt-in MemoryProbeTest generates 64 KiB buffers on demand; byte at offset n is n modulo 251. It updates SHA-256 while sending and receiving without collecting the object. The SDK uses internal 5 MiB parts, API buffer 10 MiB and one in-flight part. Only one transfer is measured. JVM heap maximum is 96 MiB and direct-memory maximum 64 MiB. A 10 ms sampler reads MemoryMXBean heap usage, direct BufferPoolMXBean memory and Linux VmRSS.

Garage uses its default Zstd compression level 1 and content-addressed blocks. This highly repeating payload compresses and deduplicates strongly: after the earlier 256 MiB / 64 MiB / 2 GiB checks, the entire fixture occupied approximately 12 MiB on disk. Every logical byte still traversed the Java upload and download and participated in both digests. These times do not represent incompressible payloads, remote network latency, production disk throughput or browser heap usage.

Final aligned-dependency measurements are in the raw text files below. Values are sampled used memory of the whole test JVM, not a proof of retained live-set size and not a per-transfer RSS guarantee.

| Workload | Sampled heap / direct / RSS (MiB) | Time, single run | Raw result |
| --- | --- | --- | --- |
| 256 MiB | 76.99 / 1.24 / 230.89 | 4.169 s | [Result](evidence/memory-268435456.txt) |
| **2 GiB + 1 byte**, real SHA-256 round trip | 84.37 / 1.24 / 247.79 | 12.900 s | [Result](evidence/memory-2147483649.txt) |
| 64 MiB, slow sink: 1 ms per emitted buffer, prefetch 1 | 58.10 / 1.11 / 198.96 | 11.641 s | [Result](evidence/memory-67108864.txt) |

The core also has a separate counter-only test above 2 GiB; that test is not substituted for the real storage result. No custom multipart implementation is benchmarked: the selected implementation is the stock AWS engine, evaluated before and after integration.

## Browser → Java → storage

Headless Chromium 156 passed seven actual cross-origin checks through HTTPS/HTTP2 on the Java demo and Garage: known 6 MiB + 17 bytes, unknown 1 MiB + 13 bytes, empty body, SHA-256 full downloads, a 53-byte range crossing the 5 MiB boundary, a 31-byte suffix, missing-object rejection before sink writes, and abort after 6 MiB source-read progress. [Raw browser report](evidence/browser-java-garage.json).

For that browser abort, a subsequent independent SDK query found zero matching incomplete multipart uploads and HEAD 404. [Server observation](evidence/browser-abort.txt). This is one observed pre-commit case; finalization races remain ambiguous.

The browser module separately passed 27 Node unit tests and an npm-pack consumer check. Its synthetic HTTP2 fixture passed ten transport checks including arrival before source EOF, invalid acknowledgements, network reset and sink errors. HTTP1/TLS request streaming was rejected despite a positive constructor probe. Firefox 156 reported unsupported request streams before reading/locking the source. Safari, HTTP3, CDNs and reverse proxies have not been tested. See [browser API and compatibility](../browser/README.md).

## Packaging and remaining gates

Java sources/Javadoc/MIT artifacts build and publish to an isolated local Maven repository. An independent Gradle consumer resolves only those packaged modules and runs without source-project dependencies. CI repeats the Java matrix with an isolated Garage fixture and browser unit/package checks; local success does not itself mean a GitHub Actions run has completed.

Remaining validation: real AWS S3 and MinIO, production reverse-proxy buffering and timeout settings, browser memory profiling under sustained slow sinks, multi-transfer memory capacity planning, complete-multipart response loss / permission-denied abort fault injection, release signing/provenance and registry publication. There is no universal browser claim or exactly-once claim.
