package io.github.alex59.filestreams;

import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/** Continuous byte-stream storage boundary independent of HTTP and authorization. */
public interface ObjectStreams {
    Mono<UploadResult> upload(UploadRequest request, Publisher<ByteBuffer> body);
    /** A null range requests the whole object; non-null values use strict single byte-range syntax. */
    Mono<ObjectDownload> download(String key, String range);

    /** Open the complete object; the caller still owns its body and cleanup. */
    default Mono<ObjectDownload> download(String key) {
        return download(key, null);
    }
}
