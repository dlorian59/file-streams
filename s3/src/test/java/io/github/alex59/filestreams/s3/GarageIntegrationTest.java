package io.github.alex59.filestreams.s3;

import io.github.alex59.filestreams.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;
import static org.junit.jupiter.api.Assertions.*;

/** Real storage tests; deliberately opt in to an isolated, disposable bucket. */
@EnabledIfEnvironmentVariable(named="S3_TEST_ENDPOINT", matches=".+")
class GarageIntegrationTest {
    static S3AsyncClient client;
    static S3ObjectStreams store;
    static final String BUCKET = "stream-tests";
    static final Duration WAIT = Duration.ofSeconds(30);
    @BeforeAll static void connect() {
        client = S3Clients.bounded(URI.create(System.getenv("S3_TEST_ENDPOINT")), Region.of("garage"),
            StaticCredentialsProvider.create(AwsBasicCredentials.create(
                "GK11111111111111111111111111111111", "synthetic-local-test-secret-not-for-production")));
        store = new S3ObjectStreams(client, BUCKET, new StreamLimits(512L*1024*1024, 65536), 2);
    }
    @AfterAll static void closeClient() { client.close(); }
    static Flux<ByteBuffer> generated(long length) {
        return Flux.generate(() -> 0L, (offset,sink) -> {
            if (offset == length) { sink.complete(); return offset; }
            int n=(int)Math.min(65536,length-offset);
            byte[] data=new byte[n];
            for(int i=0;i<n;i++) data[i]=(byte)((offset+i)%251);
            sink.next(ByteBuffer.wrap(data)); return offset+n;
        });
    }
    static String key() { return "test-"+UUID.randomUUID(); }
    static void delete(String key) { client.deleteObject(r->r.bucket(BUCKET).key(key)).join(); }
    @Test void emptySmallAndInternalPartBoundariesRoundTrip() {
        for(long length:new long[]{0,1,65537,5L*1024*1024-1,5L*1024*1024,5L*1024*1024+1,12L*1024*1024}) {
            String key=key();
            try {
                UploadResult result=store.upload(UploadRequest.unknownLength(key),generated(length)).block(WAIT);
                assertEquals(length,result.bytes()); assertTrue(result.committed());
                ObjectDownload download=store.download(key,null).block(WAIT);
                assertEquals(length, download.length());
                AtomicLong offset=new AtomicLong();
                download.body().doOnNext(buffer -> {
                    while(buffer.hasRemaining()) {
                        long at=offset.getAndIncrement();
                        if(buffer.get()!=(byte)(at%251)) fail("Different byte at "+at);
                    }
                }).then().block(WAIT);
                assertEquals(length,offset.get());
            } finally { delete(key); }
        }
    }
    @Test void declaredLengthChecksBothDirectionsAndLeavesNoObject() {
        for(long declared:new long[]{7,9}) {
            String key=key();
            var request=new UploadRequest(key,OptionalLong.of(declared),"application/octet-stream",Map.of());
            assertThrows(RuntimeException.class,()->store.upload(request,generated(8)).block(WAIT));
            assertAbsent(key);
        }
    }
    @Test void rangeRoundTripAndUnsatisfiable() {
        String key=key();
        try {
            store.upload(UploadRequest.unknownLength(key),generated(100)).block(WAIT);
            ObjectDownload download=store.download(key,"bytes=10-19").block(WAIT);
            assertEquals(10,download.length());
            assertEquals("bytes 10-19/100",download.range().responseHeader());
            assertEquals(10,download.body().map(ByteBuffer::remaining).reduce(0,Integer::sum).block(WAIT));
            assertThrows(ByteRange.RangeNotSatisfiable.class,()->store.download(key,"bytes=100-").block(WAIT));
        } finally { delete(key); }
    }
    @Test void cancellationErrorsSourceAndStockSdkAbortsMultipart() throws Exception {
        String key=key();
        AtomicBoolean sourceCancelled=new AtomicBoolean();
        var source=generated(12L*1024*1024).concatWith(Flux.never()).doOnCancel(()->sourceCancelled.set(true));
        var subscription=store.upload(UploadRequest.unknownLength(key),source).subscribe();
        await(()->client.listMultipartUploads(r->r.bucket(BUCKET).prefix(key)).join().uploads().size()==1);
        subscription.dispose();
        await(sourceCancelled::get);
        await(()->client.listMultipartUploads(r->r.bucket(BUCKET).prefix(key)).join().uploads().isEmpty());
        assertAbsent(key);
    }
    @Test void sourceFailureAfterPartsAbortsMultipart() throws Exception {
        String key=key();
        assertThrows(RuntimeException.class,()->store.upload(UploadRequest.unknownLength(key),
            generated(12L*1024*1024).concatWith(Flux.error(new IllegalStateException("synthetic source failure")))).block(WAIT));
        await(()->client.listMultipartUploads(r->r.bucket(BUCKET).prefix(key)).join().uploads().isEmpty());
        assertAbsent(key);
    }
    @Test void storageFailureReleasesAdmissionPermit() {
        var missing=new S3ObjectStreams(client,"missing-"+UUID.randomUUID(),new StreamLimits(100,65536),1);
        for(int i=0;i<2;i++) {
            RuntimeException failure=assertThrows(RuntimeException.class,()->missing.upload(UploadRequest.unknownLength(key()),generated(8)).block(WAIT));
            assertFalse(failure instanceof StreamFailure && ((StreamFailure)failure).code()==StreamFailure.Code.BUSY);
        }
    }
    @Test void downloadCancellationAndUnconsumedCloseReleaseAdmission() {
        String key=key();
        var one=new S3ObjectStreams(client,BUCKET,new StreamLimits(20L*1024*1024,65536),1);
        try {
            one.upload(UploadRequest.unknownLength(key),generated(6L*1024*1024)).block(WAIT);
            var opened=one.download(key,null).block(WAIT);
            assertThrows(StreamFailure.class,()->one.download(key,null).block(WAIT));
            opened.close();
            var next=one.download(key,null).block(WAIT);
            next.body().take(1).then().block(WAIT);
            one.download(key,"bytes=0-0").block(WAIT).body().then().block(WAIT);
        } finally { delete(key); }
    }
    static void assertAbsent(String key) {
        CompletionException error=assertThrows(CompletionException.class,()->client.headObject(r->r.bucket(BUCKET).key(key)).join());
        assertInstanceOf(S3Exception.class,error.getCause(),()->"Unexpected HEAD failure: "+error.getCause());
        assertEquals(404,((S3Exception)error.getCause()).statusCode());
    }
    static void await(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
        while(!condition.getAsBoolean()) {
            if(System.nanoTime()>deadline) fail("Condition not met within 20 seconds");
            Thread.sleep(50);
        }
    }
}
