package io.github.alex59.filestreams.webflux;
import io.github.alex59.filestreams.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.*;
import org.springframework.http.*;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.*;
import io.netty.buffer.PooledByteBufAllocator;
import static org.junit.jupiter.api.Assertions.*;
class HttpHandlerTest {
    static final StreamLimits LIMITS=new StreamLimits(8,8);
    static ObjectStreams uploadStore() {
        return new ObjectStreams() {
            public Mono<UploadResult> upload(UploadRequest request,Publisher<ByteBuffer> body) {
                var checked=new CheckedStream(body,request.length(),LIMITS);
                return checked.content().then(Mono.fromSupplier(()->new UploadResult(true,request.key(),checked.bytes(),"\"opaque\"")));
            }
            public Mono<ObjectDownload> download(String key,String range) { return Mono.error(new UnsupportedOperationException()); }
        };
    }
    @Test void rawBodyCopiesAndReleasesPooledMemoryThenAcknowledges() {
        var factory=new NettyDataBufferFactory(PooledByteBufAllocator.DEFAULT);
        var pooled=factory.allocateBuffer(4); pooled.write(new byte[]{1,2,3,4});
        var exchange=MockServerWebExchange.from(MockServerHttpRequest.put("/objects/example").header("X-Upload-Length","4").body(Flux.just(pooled)));
        new ObjectStreamHandler(uploadStore(),LIMITS).handle(exchange).block();
        assertEquals(0,pooled.getNativeBuffer().refCnt());
        assertEquals(HttpStatus.CREATED,exchange.getResponse().getStatusCode());
        String response=exchange.getResponse().getBodyAsString().block();
        assertTrue(response.contains("\"committed\":true")); assertTrue(response.contains("\"bytes\":4"));
    }
    @Test void oversizeAndShortBodyRejectAndRelease() {
        for(int size:new int[]{3,9}) {
            var pooled=new NettyDataBufferFactory(PooledByteBufAllocator.DEFAULT).allocateBuffer(size); pooled.write(new byte[size]);
            var exchange=MockServerWebExchange.from(MockServerHttpRequest.put("/objects/a").header("X-Upload-Length","4").body(Flux.just(pooled)));
            new ObjectStreamHandler(uploadStore(),LIMITS).handle(exchange).block();
            assertEquals(HttpStatus.BAD_REQUEST,exchange.getResponse().getStatusCode());
            assertEquals(0,pooled.getNativeBuffer().refCnt());
            assertTrue(exchange.getResponse().getBodyAsString().block().contains("\"committed\":false"));
        }
    }
    @Test void conflictingOrInvalidLengthsRejectBeforeSubscription() {
        for(String value:new String[]{"5","-1","9223372036854775808","1,2"}) {
            AtomicBoolean subscribed=new AtomicBoolean();
            var exchange=MockServerWebExchange.from(MockServerHttpRequest.put("/objects/a").header("Content-Length","4")
                .header("X-Upload-Length",value).body(Flux.<DataBuffer>empty().doOnSubscribe(s->subscribed.set(true))));
            new ObjectStreamHandler(uploadStore(),LIMITS).handle(exchange).block();
            assertEquals(HttpStatus.BAD_REQUEST,exchange.getResponse().getStatusCode()); assertFalse(subscribed.get());
        }
    }
    @Test void ambiguousStorageFailureDoesNotClaimRollbackOrExposeMessage() {
        var store=new ObjectStreams() {
            public Mono<UploadResult> upload(UploadRequest request,Publisher<ByteBuffer> body) { return Mono.error(new IllegalStateException("private storage detail")); }
            public Mono<ObjectDownload> download(String key,String range) { return Mono.error(new UnsupportedOperationException()); }
        };
        var exchange=MockServerWebExchange.from(MockServerHttpRequest.put("/objects/a").body("x"));
        new ObjectStreamHandler(store,LIMITS).handle(exchange).block();
        assertEquals(HttpStatus.BAD_GATEWAY,exchange.getResponse().getStatusCode());
        String json=exchange.getResponse().getBodyAsString().block();
        assertTrue(json.contains("\"committed\":null")); assertFalse(json.contains("private storage"));
    }
    @Test void downloadWritesValidatedRangeHeadersAndReleasesResource() {
        AtomicInteger released=new AtomicInteger();
        var store=new ObjectStreams() {
            public Mono<UploadResult> upload(UploadRequest request,Publisher<ByteBuffer> body) { return Mono.error(new UnsupportedOperationException()); }
            public Mono<ObjectDownload> download(String key,String range) {
                return Mono.fromSupplier(()->new ObjectDownload(2,"text/plain","\"etag\"",ByteRange.parse(range,4),
                    Flux.just(ByteBuffer.wrap("cd".getBytes(StandardCharsets.UTF_8))),released::incrementAndGet));
            }
        };
        var exchange=MockServerWebExchange.from(MockServerHttpRequest.get("/objects/a").header("Range","bytes=2-3"));
        new ObjectStreamHandler(store,LIMITS).handle(exchange).block();
        assertEquals(HttpStatus.PARTIAL_CONTENT,exchange.getResponse().getStatusCode());
        assertEquals("bytes 2-3/4",exchange.getResponse().getHeaders().getFirst("Content-Range"));
        assertEquals("cd",exchange.getResponse().getBodyAsString().block()); assertEquals(1,released.get());
    }
    @Test void invalidRangeReturns416AndTotal() {
        var store=new ObjectStreams() {
            public Mono<UploadResult> upload(UploadRequest r,Publisher<ByteBuffer> p) { return Mono.empty(); }
            public Mono<ObjectDownload> download(String key,String range) { return Mono.error(new ByteRange.RangeNotSatisfiable(5)); }
        };
        var exchange=MockServerWebExchange.from(MockServerHttpRequest.get("/objects/a").header("Range","bytes=9-"));
        new ObjectStreamHandler(store,LIMITS).handle(exchange).block();
        assertEquals(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE,exchange.getResponse().getStatusCode());
        assertEquals("bytes */5",exchange.getResponse().getHeaders().getFirst("Content-Range"));
    }
}
