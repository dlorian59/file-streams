package io.github.alex59.filestreams.s3;
import io.github.alex59.filestreams.*;
import java.lang.management.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.*;
/** Reproducible, opt-in single-transfer observation; not a universal memory guarantee. */
@EnabledIfEnvironmentVariable(named="S3_MEMORY_BYTES",matches="[0-9]+")
class MemoryProbeTest {
    @Test void generatedObjectExceedsHeapAndRoundTripsWithoutAggregation() throws Exception {
        GarageIntegrationTest.connect();
        GarageIntegrationTest.store=new S3ObjectStreams(GarageIntegrationTest.client,GarageIntegrationTest.BUCKET,
            new StreamLimits(3L*1024*1024*1024,65536),2);
        long size=Long.parseLong(System.getenv("S3_MEMORY_BYTES"));
        String key=GarageIntegrationTest.key();
        AtomicLong heap=new AtomicLong(),direct=new AtomicLong(),rss=new AtomicLong(),demand=new AtomicLong();
        var sampler=Executors.newSingleThreadScheduledExecutor();
        sampler.scheduleAtFixedRate(()-> {
            heap.accumulateAndGet(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(),Math::max);
            for(var pool:ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
                if(pool.getName().equals("direct")) direct.accumulateAndGet(pool.getMemoryUsed(),Math::max);
            try { for(String line:Files.readAllLines(Path.of("/proc/self/status"))) if(line.startsWith("VmRSS:"))
                rss.accumulateAndGet(Long.parseLong(line.replaceAll("[^0-9]",""))*1024,Math::max);
            } catch(Exception ignored) { }
        },0,10,TimeUnit.MILLISECONDS);
        long started=System.nanoTime();
        try {
            MessageDigest sent=MessageDigest.getInstance("SHA-256"),received=MessageDigest.getInstance("SHA-256");
            var source=GarageIntegrationTest.generated(size).doOnNext(b->sent.update(b.duplicate()))
                .doOnRequest(n->demand.accumulateAndGet(n,Math::max));
            var result=GarageIntegrationTest.store.upload(UploadRequest.unknownLength(key),source).block(java.time.Duration.ofMinutes(3));
            assertEquals(size,result.bytes());
            var download=GarageIntegrationTest.store.download(key,null).block(GarageIntegrationTest.WAIT);
            AtomicLong count=new AtomicLong();
            var body=download.body();
            if ("1".equals(System.getenv("S3_SLOW_SINK")))
                body=body.concatMap(b->reactor.core.publisher.Mono.delay(java.time.Duration.ofMillis(1)).thenReturn(b),1);
            body.doOnNext(b->{ count.addAndGet(b.remaining()); received.update(b); }).then().block(java.time.Duration.ofMinutes(3));
            assertEquals(size,count.get()); assertArrayEquals(sent.digest(),received.digest());
        } finally {
            sampler.shutdownNow(); GarageIntegrationTest.delete(key); GarageIntegrationTest.closeClient();
        }
        System.out.printf("MEMORY_PROBE bytes=%d maxHeap=%d sampledHeap=%d sampledDirect=%d sampledRss=%d sourceMaxRequest=%d seconds=%.3f%n",
            size,Runtime.getRuntime().maxMemory(),heap.get(),direct.get(),rss.get(),demand.get(),(System.nanoTime()-started)/1e9);
    }
}
