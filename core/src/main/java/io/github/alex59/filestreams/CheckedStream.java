package io.github.alex59.filestreams;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * One-shot, demand-aware source validation. Emitted buffers must remain immutable
 * until transfer termination; their position is not changed by this wrapper.
 * Cancellation is an error signal to storage, never a successful partial EOF.
 */
public final class CheckedStream {
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final Sinks.One<Boolean> cancellation = Sinks.one();
    private final Flux<ByteBuffer> content;

    public CheckedStream(Publisher<ByteBuffer> source, OptionalLong expectedLength, StreamLimits limits) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(expectedLength, "expectedLength");
        Objects.requireNonNull(limits, "limits");
        if (expectedLength.isPresent() && expectedLength.getAsLong() < 0)
            throw new IllegalArgumentException("Expected length must be nonnegative");
        if (expectedLength.isPresent() && expectedLength.getAsLong() > limits.maxBytes())
            throw new StreamFailure(StreamFailure.Code.TOO_LARGE, "Declared length exceeds maximum object size");
        content = Flux.defer(() -> {
            if (!subscribed.compareAndSet(false, true))
                return Flux.error(new StreamFailure(StreamFailure.Code.SOURCE_REUSED, "Upload sources are one-shot"));
            return Flux.from(source).limitRate(1, 0).map(buffer -> {
                int size = buffer.remaining();
                if (size > limits.maxBufferBytes())
                    throw new StreamFailure(StreamFailure.Code.BUFFER_TOO_LARGE, "Source buffer exceeds configured maximum");
                long before = bytes.get();
                if (size > limits.maxBytes() - before)
                    throw new StreamFailure(StreamFailure.Code.TOO_LARGE, "Upload exceeds maximum object size");
                long total = before + size;
                if (expectedLength.isPresent() && total > expectedLength.getAsLong())
                    throw new StreamFailure(StreamFailure.Code.LENGTH_MISMATCH, "Upload is longer than declared");
                bytes.set(total);
                return buffer.asReadOnlyBuffer();
            }).takeUntilOther(cancellation.asMono())
                    .concatWith(Flux.defer(() -> cancelled.get()
                    ? Flux.error(new StreamFailure(StreamFailure.Code.CANCELLED, "Upload source cancelled"))
                    : expectedLength.isPresent() && bytes.get() != expectedLength.getAsLong()
                    ? Flux.error(new StreamFailure(StreamFailure.Code.LENGTH_MISMATCH, "Upload is shorter than declared"))
                    : Flux.empty()))
                    .doOnComplete(() -> completed.set(true));
        });
    }
    public Flux<ByteBuffer> content() { return content; }
    public long bytes() { return bytes.get(); }
    public boolean completed() { return completed.get(); }
    public void cancel() { cancelled.set(true); cancellation.tryEmitValue(true); }
}
