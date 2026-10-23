package io.github.alex59.filestreams;
/** A storage object was absent, or changed between its metadata lookup and opening its body. */
public final class ObjectUnavailable extends RuntimeException {
    public enum Reason { NOT_FOUND, CHANGED }
    private final Reason reason;
    public ObjectUnavailable(Reason reason,Throwable cause) {
        super(reason==Reason.NOT_FOUND?"Object not found":"Object changed while opening download",cause); this.reason=reason;
    }
    public Reason reason() { return reason; }
}
