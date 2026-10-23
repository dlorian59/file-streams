package io.github.alex59.filestreams;

/** A source-contract failure before the complete source can be committed. */
public final class StreamFailure extends RuntimeException {
    public enum Code { TOO_LARGE, LENGTH_MISMATCH, BUFFER_TOO_LARGE, CANCELLED, SOURCE_REUSED, BUSY }
    private final Code code;
    public StreamFailure(Code code, String message) { super(message); this.code = code; }
    public Code code() { return code; }
}
