package io.github.alex59.filestreams.s3;
import io.github.alex59.filestreams.*;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import static org.junit.jupiter.api.Assertions.*;
class S3ContractTest {
    static S3AsyncClient fake(java.util.function.Function<AsyncRequestBody,CompletableFuture<PutObjectResponse>> put) {
        return (S3AsyncClient)Proxy.newProxyInstance(S3AsyncClient.class.getClassLoader(),new Class<?>[]{S3AsyncClient.class},(p,m,args)-> {
            if(m.getName().equals("putObject")) return put.apply((AsyncRequestBody)args[1]);
            throw new UnsupportedOperationException(m.getName());
        });
    }
    @Test void invalidStorageAcknowledgementTerminatesAndReleasesPermit() {
        var client=fake(body->Flux.from(body).then().toFuture().thenApply(v->PutObjectResponse.builder().build()));
        var store=new S3ObjectStreams(client,"test",StreamLimits.defaults(),1);
        for(int i=0;i<2;i++) StepVerifier.create(store.upload(UploadRequest.unknownLength("key"),Flux.just(ByteBuffer.allocate(1))))
            .expectErrorMatches(e->e instanceof IllegalStateException && e.getCause() instanceof IllegalArgumentException).verify(Duration.ofSeconds(1));
    }
    @Test void misconfiguredClientCannotAcknowledgeUnreadSource() {
        var client=fake(body->CompletableFuture.completedFuture(PutObjectResponse.builder().eTag("etag").build()));
        var store=new S3ObjectStreams(client,"test",StreamLimits.defaults(),1);
        StepVerifier.create(store.upload(UploadRequest.unknownLength("key"),Flux.just(ByteBuffer.allocate(1))))
            .expectErrorMatches(e->e instanceof IllegalStateException && e.getCause().getMessage().contains("without consuming")).verify();
    }
    @Test void cancellationStopsSourceButKeepsSdkFutureAliveForAbortAndBlocksAdmissionUntilCleanup() {
        var sdkFuture=new CompletableFuture<PutObjectResponse>(); AtomicBoolean cancelled=new AtomicBoolean();
        var client=fake(body->{ Flux.from(body).subscribe(b->{},e->{}); return sdkFuture; });
        var store=new S3ObjectStreams(client,"test",StreamLimits.defaults(),1);
        var request=UploadRequest.unknownLength("key");
        var running=store.upload(request,Flux.<ByteBuffer>never().doOnCancel(()->cancelled.set(true))).subscribe();
        running.dispose(); assertTrue(cancelled.get()); assertFalse(sdkFuture.isCancelled());
        StepVerifier.create(store.upload(request,Flux.empty())).expectErrorMatches(e->((StreamFailure)e).code()==StreamFailure.Code.BUSY).verify();
        sdkFuture.completeExceptionally(new IllegalStateException("abort complete"));
    }
}
