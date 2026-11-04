package io.github.alex59.filestreams.demo;

import io.github.alex59.filestreams.ObjectDownload;
import io.github.alex59.filestreams.ObjectStreams;
import io.github.alex59.filestreams.UploadRequest;
import io.github.alex59.filestreams.UploadResult;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

// EN: Framework-independent Java operations; ObjectStreamHandler already connects them to raw HTTP PUT/GET.
// RU: Java-операции без привязки к фреймворку; ObjectStreamHandler уже связывает их с HTTP PUT/GET.
public final class TransferExamples {
    private TransferExamples() {}

    // EN: Pass the request stream directly; only a validated storage acknowledgement completes this Mono.
    // RU: Передаём поток запроса напрямую; Mono завершается только после проверенного подтверждения хранилища.
    public static Mono<UploadResult> receive(ObjectStreams files, String key,
            OptionalLong length, Publisher<ByteBuffer> requestBody) {
        return files.upload(new UploadRequest(key, length, "application/octet-stream", Map.of()), requestBody);
    }

    // EN: ObjectDownload owns the response; consume its body or close it. The HTTP adapter handles this lifecycle.
    // RU: ObjectDownload владеет ответом: прочитайте тело или закройте его. HTTP-адаптер управляет этим жизненным циклом.
    public static Mono<ObjectDownload> download(ObjectStreams files, String key, String rangeHeader) {
        return files.download(key, rangeHeader);
    }
}
