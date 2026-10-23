import io.github.alex59.filestreams.*;
import io.github.alex59.filestreams.s3.*;
import io.github.alex59.filestreams.webflux.*;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import reactor.core.publisher.Flux;
/** Package consumer; no source-project dependencies and no storage credentials. */
public final class Consumer {
    public static void main(String[] args) {
        var stream=new CheckedStream(Flux.just(ByteBuffer.wrap(new byte[]{1,2,3})),OptionalLong.of(3),StreamLimits.defaults());
        stream.content().then().block();
        if(stream.bytes()!=3 || !stream.completed()) throw new AssertionError("stream validation failed");
        if(S3Clients.SDK_BUFFER_BYTES!=10L*1024*1024) throw new AssertionError("unexpected profile");
        if(ObjectStreamHandler.declaredLength(new org.springframework.http.HttpHeaders()).isPresent()) throw new AssertionError("unexpected length");
        System.out.println("Published-artifact consumer passed: core + S3 + WebFlux");
    }
}
