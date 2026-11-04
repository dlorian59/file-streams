package io.github.alex59.filestreams.demo;

import io.github.alex59.filestreams.ObjectDownload;
import io.github.alex59.filestreams.ObjectStreams;
import io.github.alex59.filestreams.UploadRequest;
import io.github.alex59.filestreams.UploadResult;
import java.nio.ByteBuffer;
import java.time.Duration;
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

    // EN: A deadline cancels this subscription; it does not replay the source or promise remote rollback.
    // RU: Срок отменяет эту подписку; он не повторяет источник и не обещает удалённый откат.
    public static Mono<UploadResult> receiveBefore(ObjectStreams files, UploadRequest request,
            Publisher<ByteBuffer> requestBody, Duration deadline) {
        return files.upload(request, requestBody).timeout(deadline);
    }

    // EN: Consume incrementally and cover completion, failure and cancellation with one resource lifetime.
    // RU: Читаем постепенно и закрываем ресурс при завершении, ошибке и отмене.
    public static Mono<Long> countDownload(ObjectStreams files, String key, String rangeHeader) {
        return Mono.usingWhen(files.download(key, rangeHeader),
                opened -> opened.body().map(buffer -> (long) buffer.remaining()).reduce(0L, Long::sum),
                opened -> Mono.fromRunnable(opened::close),
                (opened, error) -> Mono.fromRunnable(opened::close),
                opened -> Mono.fromRunnable(opened::close));
    }

}
