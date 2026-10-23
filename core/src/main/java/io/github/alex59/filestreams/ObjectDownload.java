package io.github.alex59.filestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

/** A single-use download. Consume its body or close it; never abandon an opened download. */
public final class ObjectDownload implements AutoCloseable {
    private final long length;
    private final String contentType;
    private final String etag;
    private final ByteRange range;
    private final Publisher<ByteBuffer> source;
    private final CheckedStream checked;
    private final Runnable cleanup;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public ObjectDownload(
            long length,
            String contentType,
            String etag,
            ByteRange range,
            Publisher<ByteBuffer> source,
            Runnable cleanup) {
        if (length < 0) throw new IllegalArgumentException("Download length must be nonnegative");
        Objects.requireNonNull(contentType, "contentType");
        Objects.requireNonNull(etag, "etag");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(cleanup, "cleanup");
        if (range != null && range.length() != length)
            throw new IllegalArgumentException("Range length does not match body");
        this.length = length;
        this.contentType = contentType;
        this.etag = etag;
        this.range = range;
        this.source = source;
        this.cleanup = cleanup;
        this.checked =
                new CheckedStream(
                        source,
                        OptionalLong.of(length),
                        new StreamLimits(length, Integer.MAX_VALUE));
    }

    public long length() {
        return length;
    }

    public String contentType() {
        return contentType;
    }

    public String etag() {
        return etag;
    }

    public ByteRange range() {
        return range;
    }

    public Flux<ByteBuffer> body() {
        return Flux.defer(
                () -> {
                    if (!started.compareAndSet(false, true))
                        return Flux.error(new IllegalStateException("Download body is one-shot"));
                    if (closed.get()) {
                        cancelUnsubscribedSource();
                        return Flux.error(new IllegalStateException("Download is closed"));
                    }
                    return checked.content()
                            .doFinally(
                                    signal -> {
                                        if (closed.compareAndSet(false, true)) cleanup.run();
                                    });
                });
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try {
            if (started.compareAndSet(false, true)) {
                cancelUnsubscribedSource();
            } else {
                checked.cancel();
            }
        } finally {
            cleanup.run();
        }
    }

    private void cancelUnsubscribedSource() {
        Flux.from(source)
                .subscribe(
                        new BaseSubscriber<>() {
                            @Override
                            protected void hookOnSubscribe(Subscription value) {
                                value.cancel();
                            }
                        });
    }
}
