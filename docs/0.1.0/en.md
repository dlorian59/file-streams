# s3-file-streams 0.1.0 — guide

[← Back to documentation and versions / Назад к документации и версиям](../index.md)

[EN](en.md) · [RU](ru.md) · [Versions](../index.md) · [Java 17 / 21 / 25 / 27](../JAVA-SUPPORT.md)

## Capabilities

- Continuous raw uploads: known or unknown total length; no application part API.
- Overflow-safe long counters, maximum object size and bounded source buffers.
- Stock AWS async multipart with bounded buffering and transfer admission.
- Distinct source progress, confirmed commit and unknown outcome.
- Streamed full or single-range downloads, explicit close/cancel.
- WebFlux pooled-buffer ownership, HTTP errors and header validation.
- Browser upload/download streams, abort and capability checks.

## 1. Install modules locally

```bash
./gradlew build publishToMavenLocal
```

In a consuming Gradle Kotlin DSL project:

```kotlin
repositories { mavenLocal(); mavenCentral() }
dependencies {
    implementation("io.github.59alex:s3-file-streams-s3:0.1.0")
    implementation("io.github.59alex:s3-file-streams-webflux:0.1.0")
}
```

Core is transitive. `s3` exposes the AWS client types and a Netty BOM to keep AWS and Reactor Netty modules aligned. Source and Javadoc artifacts and an MIT license are included. Public registry publication is a separate release step; local coordinates are not a claim that Maven Central already has the release.

## 2. Create the application-owned storage client

These snippets belong inside an application method with the imports shown; the client must outlive every asynchronous transfer. Closing it immediately after returning an unsubscribed Mono is incorrect.

```java
import io.github.alex59.filestreams.*;
import io.github.alex59.filestreams.s3.*;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

var client = S3Clients.bounded(
    URI.create("http://127.0.0.1:19390"), Region.of("garage"),
    DefaultCredentialsProvider.create());
var limits = new StreamLimits(16L * 1024 * 1024 * 1024, 1024 * 1024);
var files = new S3ObjectStreams(client, "my-bucket", limits, 2);
```

The factory enables stock multipart, sets internal parts to 5 MiB, API buffer to 10 MiB and part concurrency to one. The store admits two transfers in this example; excess requests fail with BUSY. Credentials/bucket are application configuration. The factory does not inherit ambient HTTP proxy settings; configure a custom AWS client if an explicit proxy is required. A custom client must enable multipart and preserve the documented buffer/retry limits.

## 3. Upload unknown or known length

```java
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;
import reactor.core.publisher.Flux;

// Replace this tiny example with a demand-aware Publisher<ByteBuffer>.
var source = Flux.just(ByteBuffer.wrap(new byte[] {1, 2, 3}));
var pending = files.upload(UploadRequest.unknownLength("example.bin"), source);
UploadResult result = pending.block(); // Only at a standalone application's boundary.
System.out.println(result.bytes());    // 3; returned after confirmed storage commit.
```

Known length uses the same continuous stream:

```java
var request = new UploadRequest("known.bin", OptionalLong.of(3),
    "application/octet-stream", Map.of("purpose", "example"));
files.upload(request, Flux.just(ByteBuffer.wrap(new byte[] {1, 2, 3}))).block();
```

Three expected bytes and two or four actual bytes fail. Declaring a length never permits silent truncation. Empty streams create empty objects after EOF validation. `maxBytes` and `maxBufferBytes` reject excessive input before it reaches finalization. Keep buffers immutable until transfer termination; the core preserves their position but does not deep-copy arbitrary caller memory.

## 4. Cancellation, deadlines and outcomes

```java
import java.time.Duration;

var freshSource = Flux.just(ByteBuffer.wrap(new byte[] {4, 5, 6}));
var subscription = files.upload(UploadRequest.unknownLength("deadline.bin"), freshSource)
    .timeout(Duration.ofMinutes(5))
    .subscribe(result -> System.out.println("Committed: " + result.key()),
               failure -> System.err.println("No confirmed acknowledgement"));
// On an application cancellation event:
subscription.dispose();
```

Use a fresh source for each new upload, as `freshSource` does here. A timeout/disposal cancels upstream and asks the SDK error path to clean up. Cancellation during finalization can still leave a committed object. No automatic delete is attempted. Inspect/reconcile the object by your upload identity before retrying; do not call retry() on a consumed stream. Configure incomplete-multipart lifecycle cleanup for crash/network/permission failures. ETags are not hashes.

## 5. Download and close

```java
import reactor.core.publisher.Mono;

Long bytes = Mono.usingWhen(files.download("example.bin", null),
    download -> download.body().map(ByteBuffer::remaining)
        .reduce(0L, (count, size) -> count + size),
    download -> Mono.fromRunnable(download::close)).block();
```

The example counts incrementally without aggregating the object. Replace the count with a nonblocking sink that honors demand. An opened download must be consumed or closed; it holds an admission permit. usingWhen covers asynchronous resource cleanup. Do not block an event-loop thread with a filesystem OutputStream.

## 6. Single byte ranges

```java
Mono.usingWhen(files.download("example.bin", "bytes=1-2"),
    download -> {
        System.out.println(download.range().responseHeader()); // bytes 1-2/3
        return download.body().then();
    }, download -> Mono.fromRunnable(download::close)).block();
```

Open-ended and suffix ranges work too. Multiple ranges, overflow, zero suffix and unsatisfiable offsets fail. The adapter resolves the range using HEAD, issues conditional GET and validates the response. Modification between the two operations fails instead of pairing old metadata with new bytes.

## 7. WebFlux raw-body adapter

```java
import io.github.alex59.filestreams.webflux.ObjectStreamHandler;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;

var handler = WebHttpHandlerBuilder
    .webHandler(new ObjectStreamHandler(files, limits))
    .build();
```

Mount this HttpHandler in your application server and put authentication/authorization before it. PUT `/objects/{key}` receives a raw stream; GET streams the response. Pooled request buffers are copied into bounded owned buffers and released. Metadata is intentionally restricted. The adapter does not accept multipart/form-data. See the exact [HTTP contract](../HTTP-CONTRACT.md) for 201, false/null failure outcomes, Range and CORS.

## 8. Browser streams

Build/package the browser module locally as described in [its API guide](../../browser/README.md); it includes runnable examples for upload, download, progress, abort and range options. The [functional demo](../../browser/demo/index.html) connects a browser stream to the Java server.

```bash
cd browser
npm ci
npm test
npm run test:package
npm run demo
```

Successful JavaScript feature detection is necessary but not sufficient. Actual HTTP version, TLS, browser and reverse proxy determine whether request streaming works. No buffered Blob fallback is hidden behind the streaming API. A writable destination can already contain partial data on cancellation.

## Local end-to-end demo

From repository root, with Docker and Java 17+:

```bash
scripts/start-garage.sh
./gradlew :demo:installDist
mkdir -p build/tls
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -keyout build/tls/key.pem -out build/tls/cert.pem \
  -subj /CN=localhost -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
DEMO_ORIGIN=https://127.0.0.1:9443 \
DEMO_TLS_CERT="$PWD/build/tls/cert.pem" DEMO_TLS_KEY="$PWD/build/tls/key.pem" \
  demo/build/install/demo/bin/demo
```

The demo uses public synthetic Garage fixture credentials, binds `https://127.0.0.1:19443`, allows only the selected origin and disables compression. It has no authentication. Trust only the locally generated development certificate in a disposable browser/test context. Run the browser demo on port 9443 and choose the Java integration check. Do not expose either fixture publicly. If the default Garage port/container name is occupied, set GARAGE_PORT/GARAGE_RUN_ID and S3_ENDPOINT explicitly.

Stop only the fixture created by the script:

```bash
docker rm -f "$(cat build/garage/container-id)"
```

## 9. Verification and troubleshooting

```bash
S3_TEST_ENDPOINT=http://127.0.0.1:19390 ./gradlew build
./gradlew publishToMavenLocal -Dmaven.repo.local="$PWD/build/local-repository"
./gradlew -p examples/consumer run
S3_TEST_ENDPOINT=http://127.0.0.1:19390 S3_MEMORY_BYTES=2147483649 \
  ./gradlew :s3:test --tests '*MemoryProbeTest' -PmemoryProbe
```

Run storage tests only against a disposable fixture. A 502 or lost response is not proof that an object is absent. A BUSY error means the bounded admission slots are occupied; reduce concurrency or close abandoned downloads. A package mix of Netty 4.1 and 4.2 can fail at runtime; retain the exported BOM and verify the final dependency graph. Incomplete multipart cleanup requires storage permissions and lifecycle support. [Measured evidence](../VERIFICATION.md) separates actual runs, compatibility assumptions and untested deployments.

[← Back to documentation and versions / Назад к документации и версиям](../index.md)
