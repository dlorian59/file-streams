import io.github.alex59.filestreams.ByteRange;
import io.github.alex59.filestreams.CheckedStream;
import io.github.alex59.filestreams.ObjectDownload;
import io.github.alex59.filestreams.StreamFailure;
import io.github.alex59.filestreams.StreamLimits;
import io.github.alex59.filestreams.UploadRequest;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Flux;

// EN: Runnable core examples need neither a server nor storage credentials.
// RU: Исполняемые примеры core не требуют сервера или credentials хранилища.
public final class CoreCapabilities {
    private CoreCapabilities() {}

    public static void run() {
        // EN: Java callers can attach application metadata; the raw HTTP adapter intentionally supplies an empty map.
        // RU: Java-код может передать metadata приложения; HTTP-адаптер намеренно использует пустую map.
        var request = new UploadRequest("reports/demo.bin", OptionalLong.of(3),
                "application/octet-stream", Map.of("category", "report"));
        var source = Flux.just(ByteBuffer.wrap(new byte[] {1, 2, 3}));
        var checked = new CheckedStream(source, request.length(), new StreamLimits(8, 4));
        checked.content().then().block();
        if (!checked.completed() || checked.bytes() != 3) throw new AssertionError("Unexpected byte count");

        // EN: A suffix resolves to inclusive offsets; invalid ranges expose the total for HTTP 416 handling.
        // RU: Суффикс превращается во включительные границы; ошибка диапазона содержит total для HTTP 416.
        var range = ByteRange.parse("bytes=-2", 3);
        if (range.start() != 1 || range.end() != 2 || range.length() != 2
                || !range.responseHeader().equals("bytes 1-2/3")) throw new AssertionError("Unexpected range");
        try {
            ByteRange.parse("bytes=3-", 3);
            throw new AssertionError("Unsatisfiable range accepted");
        } catch (ByteRange.RangeNotSatisfiable expected) {
            System.out.println("Invalid range total=" + expected.total());
        }

        // EN: Close an abandoned body; automatic completion cleanup and explicit close remain exactly once.
        // RU: Закрывайте оставленное тело; очистка после чтения и явный close выполняются ровно один раз.
        var releases = new AtomicInteger();
        try (var download = new ObjectDownload(2, request.contentType(), "opaque", range,
                Flux.just(ByteBuffer.wrap(new byte[] {2, 3})), releases::incrementAndGet)) {
            long read = download.body().map(buffer -> (long) buffer.remaining()).reduce(0L, Long::sum).block();
            if (read != download.length()) throw new AssertionError("Unexpected download length");
        }
        if (releases.get() != 1) throw new AssertionError("Unexpected cleanup count");

        // EN: A declared-length mismatch is a typed source failure, not a successful short upload.
        // RU: Несовпадение объявленной длины — типизированная ошибка источника, а не успешная короткая загрузка.
        try {
            new CheckedStream(Flux.just(ByteBuffer.wrap(new byte[] {1})), OptionalLong.of(2),
                    new StreamLimits(8, 4)).content().then().block();
            throw new AssertionError("Short source accepted");
        } catch (StreamFailure expected) {
            if (expected.code() != StreamFailure.Code.LENGTH_MISMATCH) throw expected;
        }
        System.out.println("Core examples passed: metadata, checked bytes, ranges, cleanup, typed failure");
    }
}
