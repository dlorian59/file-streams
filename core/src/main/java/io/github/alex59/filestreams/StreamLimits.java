package io.github.alex59.filestreams;

/** Per-transfer source limits; the storage adapter adds its documented SDK budget. */
public record StreamLimits(long maxBytes, int maxBufferBytes) {
    public StreamLimits {
        if (maxBytes < 0) throw new IllegalArgumentException("maxBytes must be nonnegative");
        if (maxBufferBytes < 1) throw new IllegalArgumentException("maxBufferBytes must be positive");
    }
    public static StreamLimits defaults() { return StreamProfile.ARCHIVES.limits(); }
}
