package io.github.alex59.filestreams;

/** Named per-object bounds shared with the browser profiles; not heap or throughput guarantees. */
public enum StreamProfile {
    ATTACHMENTS(64L << 20),
    MEDIA(512L << 20),
    ARCHIVES(16L << 30);

    /** Maximum individual publisher buffer for every built-in profile (1 MiB). */
    public static final int DEFAULT_BUFFER_BYTES = 1 << 20;
    private final long maxBytes;

    StreamProfile(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public long maxBytes() {
        return maxBytes;
    }

    public StreamLimits limits() {
        return new StreamLimits(maxBytes, DEFAULT_BUFFER_BYTES);
    }
}
