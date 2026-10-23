package io.github.alex59.filestreams.s3;

import io.github.alex59.filestreams.*;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Continuous streams through the stock AWS async multipart engine. The supplied
 * client must enable multipart; use S3Clients.bounded for the measured profile.
 * This store does not close the application-owned client.
 */
public final class S3ObjectStreams implements ObjectStreams {
    private final S3AsyncClient client;
    private final String bucket;
    private final StreamLimits limits;
    private final Semaphore transfers;

    public S3ObjectStreams(S3AsyncClient client, String bucket, StreamLimits limits, int maxTransfers) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.limits = Objects.requireNonNull(limits, "limits");
        if (maxTransfers < 1) throw new IllegalArgumentException("maxTransfers must be positive");
        if (limits.maxBytes() > 10000L * S3Clients.PART_BYTES)
            throw new IllegalArgumentException("This unknown-length profile supports at most 10000 internal 5-MiB parts");
        this.transfers = new Semaphore(maxTransfers);
    }

    @Override public Mono<UploadResult> upload(UploadRequest request, Publisher<ByteBuffer> source) {
        return Mono.defer(() -> {
            CheckedStream checked = new CheckedStream(source, request.length(), limits);
            if (!transfers.tryAcquire()) return Mono.error(busy());
            return Mono.create(sink -> {
                // Do not turn source cancellation into a successful short EOF.
                // Keep the SDK future alive so its error path can abort multipart.
                sink.onCancel(checked::cancel);
                try {
                    var put = PutObjectRequest.builder().bucket(bucket).key(request.key())
                            .contentType(request.contentType()).metadata(request.metadata()).build();
                    client.putObject(put, body(checked)).whenComplete((response, failure) -> {
                        transfers.release();
                        if (failure != null) sink.error(unwrap(failure));
                        else {
                            try {
                                if (!checked.completed()) throw new IllegalStateException("Storage succeeded without consuming and validating the source; check multipart configuration");
                                sink.success(new UploadResult(true, request.key(), checked.bytes(), response.eTag()));
                            }
                            catch (Throwable invalidResponse) { sink.error(new IllegalStateException("Invalid storage acknowledgement", invalidResponse)); }
                        }
                    });
                } catch (Throwable error) {
                    transfers.release();
                    sink.error(error);
                }
            });
        });
    }

    private static AsyncRequestBody body(CheckedStream checked) {
        return new AsyncRequestBody() {
            // Deliberately unknown even for declared input length: EOF validation
            // must run before SDK finalization; known SDK lengths may truncate.
            @Override public Optional<Long> contentLength() { return Optional.empty(); }
            @Override public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
                checked.content().subscribe(subscriber);
            }
        };
    }

    @Override public Mono<ObjectDownload> download(String key, String rangeHeader) {
        return Mono.defer(() -> {
            if (!transfers.tryAcquire()) return Mono.error(busy());
            AtomicBoolean released = new AtomicBoolean();
            Runnable release = () -> { if (released.compareAndSet(false, true)) transfers.release(); };
            return Mono.fromFuture(() -> client.headObject(request -> request.bucket(bucket).key(key)))
                    .flatMap(head -> {
                        ByteRange range = rangeHeader == null ? null : ByteRange.parse(rangeHeader, head.contentLength());
                        long expected = range == null ? head.contentLength() : range.length();
                        var get = GetObjectRequest.builder().bucket(bucket).key(key).ifMatch(head.eTag());
                        // Explicit full range disables SDK multipart GET, preserving one response and accurate headers.
                        if (range != null) get.range(range.requestHeader());
                        else if (expected > 0) get.range("bytes=0-" + (expected - 1));
                        return Mono.fromFuture(() -> client.getObject(get.build(), AsyncResponseTransformer.<GetObjectResponse>toPublisher()))
                                .map(response -> {
                                    GetObjectResponse metadata = response.response();
                                    if (metadata.contentLength() != expected || metadata.eTag() == null || metadata.eTag().isBlank()
                                            || !Objects.equals(head.eTag(), metadata.eTag())
                                            || (metadata.contentEncoding() != null && !metadata.contentEncoding().equalsIgnoreCase("identity"))
                                            || (range != null && !range.responseHeader().equals(metadata.contentRange()))) {
                                        cancel(response);
                                        throw new IllegalStateException("S3 returned inconsistent download length/range");
                                    }
                                    return new ObjectDownload(expected,
                                            metadata.contentType() == null ? "application/octet-stream" : metadata.contentType(),
                                            metadata.eTag(), range, response, release);
                                })
                                .doOnDiscard(ResponsePublisher.class, response -> cancel(response));
                    }).onErrorMap(S3Exception.class, error -> error.statusCode() == 404
                            ? new ObjectUnavailable(ObjectUnavailable.Reason.NOT_FOUND, error)
                            : error.statusCode() == 412 ? new ObjectUnavailable(ObjectUnavailable.Reason.CHANGED, error) : error)
                    .doOnError(error -> release.run()).doOnCancel(release)
                    .doOnDiscard(ObjectDownload.class, ObjectDownload::close);
        });
    }

    private static void cancel(Publisher<ByteBuffer> response) {
        Flux.from(response).subscribe(new BaseSubscriber<>() {
            @Override protected void hookOnSubscribe(Subscription value) { value.cancel(); }
        });
    }
    private static StreamFailure busy() { return new StreamFailure(StreamFailure.Code.BUSY, "Concurrent transfer limit reached"); }
    private static Throwable unwrap(Throwable failure) {
        while (failure instanceof CompletionException && failure.getCause() != null) failure = failure.getCause();
        return failure;
    }
}
