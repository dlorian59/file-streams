package io.github.alex59.filestreams;

/** Returned only after the storage service confirms commit. ETag is opaque, not a checksum. */
public record UploadResult(boolean committed, String key, long bytes, String etag) {
    public UploadResult {
        if (!committed || bytes < 0 || key == null || etag == null || etag.isBlank())
            throw new IllegalArgumentException("Only confirmed commits are UploadResults");
    }
}
