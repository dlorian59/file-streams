package io.github.alex59.filestreams;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import static org.junit.jupiter.api.Assertions.*;
class CoreTest {
    static CheckedStream checked(Flux<ByteBuffer> source,long maximum) {
        return new CheckedStream(source,OptionalLong.empty(),new StreamLimits(maximum,1024*1024));
    }
    @Test void demandIsBoundedAndBufferPositionUntouched() {
        AtomicLong requested=new AtomicLong();
        var original=ByteBuffer.wrap(new byte[]{1,2,3}); original.position(1);
        var stream=checked(Flux.just(original).doOnRequest(n->requested.accumulateAndGet(n,Math::max)),8);
        StepVerifier.create(stream.content(),0).thenRequest(1).assertNext(b->{ assertTrue(b.isReadOnly()); b.get(); }).verifyComplete();
        assertEquals(1,original.position()); assertEquals(1,requested.get()); assertEquals(2,stream.bytes());
    }
    @Test void longCounterPassesTwoGiBWithoutAllocatingWholeObject() {
        ByteBuffer immutable=ByteBuffer.allocate(1024*1024).asReadOnlyBuffer();
        var stream=checked(Flux.range(0,2050).map(i->immutable.duplicate()),3L*1024*1024*1024);
        stream.content().then().block(); assertEquals(2050L*1024*1024,stream.bytes());
    }
    @Test void boundsAndLengthValidation() {
        StepVerifier.create(checked(Flux.just(ByteBuffer.allocate(9)),8).content()).expectErrorMatches(e->((StreamFailure)e).code()==StreamFailure.Code.TOO_LARGE).verify();
        for(long expected:new long[]{7,9}) {
            var stream=new CheckedStream(Flux.just(ByteBuffer.allocate(8)),OptionalLong.of(expected),new StreamLimits(10,8));
            StepVerifier.create(stream.content()).thenConsumeWhile(b->true).expectErrorMatches(e->((StreamFailure)e).code()==StreamFailure.Code.LENGTH_MISMATCH).verify();
        }
        assertThrows(IllegalArgumentException.class,()->new CheckedStream(Flux.empty(),OptionalLong.of(-1),StreamLimits.defaults()));
    }
    @Test void sourceIsOneShotAndCancelIsError() {
        var stream=checked(Flux.never(),10); stream.cancel();
        StepVerifier.create(stream.content()).expectErrorMatches(e->((StreamFailure)e).code()==StreamFailure.Code.CANCELLED).verify();
        StepVerifier.create(stream.content()).expectErrorMatches(e->((StreamFailure)e).code()==StreamFailure.Code.SOURCE_REUSED).verify();
    }
    @Test void cancellingActiveSourcePropagatesUpstream() {
        AtomicBoolean cancelled=new AtomicBoolean();
        var stream=checked(Flux.<ByteBuffer>never().doOnCancel(()->cancelled.set(true)),10);
        StepVerifier.create(stream.content()).then(stream::cancel)
            .expectErrorMatches(e->((StreamFailure)e).code()==StreamFailure.Code.CANCELLED).verify();
        assertTrue(cancelled.get());
    }
    @Test void rangesHandleSuffixOpenClippingAndOverflow() {
        assertEquals("bytes 2-9/10",ByteRange.parse("bytes=2-",10).responseHeader());
        assertEquals("bytes 7-9/10",ByteRange.parse("bytes=-3",10).responseHeader());
        assertEquals("bytes 0-9/10",ByteRange.parse("bytes=0-999",10).responseHeader());
        for(String value:new String[]{"bytes=10-","bytes=5-2","bytes=-0","bytes=0-1,4-5","bytes=99999999999999999999-","bytes=-","items=0-1"})
            assertThrows(ByteRange.RangeNotSatisfiable.class,()->ByteRange.parse(value,10));
        assertThrows(ByteRange.RangeNotSatisfiable.class,()->ByteRange.parse("bytes=0-0",0));
    }
    @Test void unconsumedAndCancelledDownloadsReleaseExactlyOnce() {
        for(boolean consume:new boolean[]{false,true}) {
            AtomicInteger cancelled=new AtomicInteger(),released=new AtomicInteger();
            var source=Flux.<ByteBuffer>never().doOnCancel(cancelled::incrementAndGet);
            var download=new ObjectDownload(0,"application/octet-stream","etag",null,source,released::incrementAndGet);
            if(consume) StepVerifier.create(download.body()).thenCancel().verify(); else download.close();
            download.close(); assertEquals(1,released.get()); assertEquals(1,cancelled.get());
            StepVerifier.create(download.body()).expectError(IllegalStateException.class).verify();
        }
    }
}
