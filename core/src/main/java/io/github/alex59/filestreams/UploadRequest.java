package io.github.alex59.filestreams;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/** Known length is an assertion checked against EOF, not permission to truncate a stream. */
public record UploadRequest(String key, OptionalLong length, String contentType, Map<String, String> metadata) {
    public UploadRequest {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(length, "length");
        Objects.requireNonNull(contentType, "contentType");
        metadata = Map.copyOf(metadata);
        if (key.isEmpty() || key.getBytes(StandardCharsets.UTF_8).length > 1024 || key.indexOf('\0') >= 0)
            throw new IllegalArgumentException("key must contain 1..1024 UTF-8 bytes without NUL");
        if (length.isPresent() && length.getAsLong() < 0) throw new IllegalArgumentException("negative length");
        if (contentType.isBlank() || contentType.contains("\r") || contentType.contains("\n"))
            throw new IllegalArgumentException("invalid contentType");
    }
    /** A source with a known final size; short and oversized streams are rejected. */
    public static UploadRequest knownLength(String key, long bytes, String contentType) {
        return new UploadRequest(key, OptionalLong.of(bytes), contentType, Map.of());
    }

    public static UploadRequest unknownLength(String key) {
        return new UploadRequest(key, OptionalLong.empty(), "application/octet-stream", Map.of());
    }
}
