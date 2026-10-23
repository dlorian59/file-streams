package io.github.alex59.filestreams.s3;

import static org.junit.jupiter.api.Assertions.*;

import io.github.alex59.filestreams.*;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class S3ContractTest {
    static S3AsyncClient fake(
            java.util.function.Function<AsyncRequestBody, CompletableFuture<PutObjectResponse>>
                    put) {
        return (S3AsyncClient)
                Proxy.newProxyInstance(
                        S3AsyncClient.class.getClassLoader(),
                        new Class<?>[] {S3AsyncClient.class},
                        (p, m, args) -> {
                            if (m.getName().equals("putObject"))
                                return put.apply((AsyncRequestBody) args[1]);
                            throw new UnsupportedOperationException(m.getName());
                        });
    }

    @Test
    void invalidStorageAcknowledgementTerminatesAndReleasesPermit() {
        var client =
                fake(
                        body ->
                                Flux.from(body)
                                        .then()
                                        .toFuture()
                                        .thenApply(v -> PutObjectResponse.builder().build()));
        var store = new S3ObjectStreams(client, "test", StreamLimits.defaults(), 1);
        for (int i = 0; i < 2; i++)
            StepVerifier.create(
                            store.upload(
                                    UploadRequest.unknownLength("key"),
                                    Flux.just(ByteBuffer.allocate(1))))
                    .expectErrorMatches(
                            e ->
                                    e instanceof IllegalStateException
                                            && e.getCause() instanceof IllegalArgumentException)
                    .verify(Duration.ofSeconds(1));
    }

    @Test
    void misconfiguredClientCannotAcknowledgeUnreadSource() {
        var client =
                fake(
                        body ->
                                CompletableFuture.completedFuture(
                                        PutObjectResponse.builder().eTag("etag").build()));
        var store = new S3ObjectStreams(client, "test", StreamLimits.defaults(), 1);
        StepVerifier.create(
                        store.upload(
                                UploadRequest.unknownLength("key"),
                                Flux.just(ByteBuffer.allocate(1))))
                .expectErrorMatches(
                        e ->
                                e instanceof IllegalStateException
                                        && e.getCause().getMessage().contains("without consuming"))
                .verify();
    }

    @Test
    void cancellationStopsSourceButKeepsSdkFutureAliveForAbortAndBlocksAdmissionUntilCleanup() {
        var sdkFuture = new CompletableFuture<PutObjectResponse>();
        AtomicBoolean cancelled = new AtomicBoolean();
        var client =
                fake(
                        body -> {
                            Flux.from(body).subscribe(b -> {}, e -> {});
                            return sdkFuture;
                        });
        var store = new S3ObjectStreams(client, "test", StreamLimits.defaults(), 1);
        var request = UploadRequest.unknownLength("key");
        var running =
                store.upload(
                                request,
                                Flux.<ByteBuffer>never().doOnCancel(() -> cancelled.set(true)))
                        .subscribe();
        running.dispose();
        assertTrue(cancelled.get());
        assertFalse(sdkFuture.isCancelled());
        StepVerifier.create(store.upload(request, Flux.empty()))
                .expectErrorMatches(e -> ((StreamFailure) e).code() == StreamFailure.Code.BUSY)
                .verify();
        sdkFuture.completeExceptionally(new IllegalStateException("abort complete"));
    }

    @Test
    void storageFailureCancelsAnUnfinishedSourceAndReleasesAdmission() {
        AtomicInteger cancelled = new AtomicInteger();
        var sdkFuture = new CompletableFuture<PutObjectResponse>();
        var client =
                fake(
                        body -> {
                            Flux.from(body).subscribe(value -> {}, error -> {});
                            return sdkFuture;
                        });
        var store = new S3ObjectStreams(client, "test", StreamLimits.defaults(), 1);
        var failure = new IllegalStateException("storage rejected upload");
        var pending =
                store.upload(
                        UploadRequest.unknownLength("key"),
                        Flux.<ByteBuffer>never().doOnCancel(cancelled::incrementAndGet));
        StepVerifier.create(pending)
                .then(() -> sdkFuture.completeExceptionally(failure))
                .expectErrorMatches(error -> error == failure)
                .verify(Duration.ofSeconds(1));
        assertEquals(1, cancelled.get());
        StepVerifier.create(store.upload(UploadRequest.unknownLength("next"), Flux.empty()))
                .expectErrorMatches(error -> error == failure)
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void missingDownloadLengthCancelsResponseAndReleasesAdmission() {
        AtomicInteger cancelled = new AtomicInteger();
        var client =
                (S3AsyncClient)
                        Proxy.newProxyInstance(
                                S3AsyncClient.class.getClassLoader(),
                                new Class<?>[] {S3AsyncClient.class},
                                (proxy, method, args) ->
                                        switch (method.getName()) {
                                            case "headObject" ->
                                                    CompletableFuture.completedFuture(
                                                            HeadObjectResponse.builder()
                                                                    .contentLength(1L)
                                                                    .eTag("etag")
                                                                    .build());
                                            case "getObject" ->
                                                    CompletableFuture.completedFuture(
                                                            new ResponsePublisher<>(
                                                                    GetObjectResponse.builder()
                                                                            .eTag("etag")
                                                                            .build(),
                                                                    SdkPublisher.adapt(
                                                                            Flux.<ByteBuffer>never()
                                                                                    .doOnCancel(
                                                                                            cancelled
                                                                                                    ::incrementAndGet))));
                                            default ->
                                                    throw new UnsupportedOperationException(
                                                            method.getName());
                                        });
        var store = new S3ObjectStreams(client, "test", StreamLimits.defaults(), 1);
        for (int attempt = 0; attempt < 2; attempt++)
            StepVerifier.create(store.download("key", null))
                    .expectError(IllegalStateException.class)
                    .verify(Duration.ofSeconds(1));
        assertEquals(2, cancelled.get());
    }
}
