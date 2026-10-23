package io.github.alex59.filestreams;

/** A single HTTP byte range resolved against a known object size. */
public record ByteRange(long start, long end, long total) {
    public ByteRange {
        if (start < 0 || end < start || total <= end) throw new IllegalArgumentException("invalid resolved range");
    }
    public long length() { return end - start + 1; }
    public String requestHeader() { return "bytes=" + start + "-" + end; }
    public String responseHeader() { return "bytes " + start + "-" + end + "/" + total; }

    public static ByteRange parse(String header, long total) {
        try {
            if (total <= 0 || header == null || header.length() > 100 || !header.startsWith("bytes=")) throw new IllegalArgumentException();
            String text = header.substring(6);
            if (!text.matches("[0-9]*-[0-9]*")) throw new IllegalArgumentException();
            int dash = text.indexOf('-');
            String first = text.substring(0, dash), last = text.substring(dash + 1);
            if (first.isEmpty()) {
                long suffix = Long.parseLong(last);
                if (suffix <= 0) throw new IllegalArgumentException();
                return new ByteRange(Math.max(0, total - suffix), total - 1, total);
            }
            long start = Long.parseLong(first);
            long end = last.isEmpty() ? total - 1 : Math.min(Long.parseLong(last), total - 1);
            return new ByteRange(start, end, total);
        } catch (IllegalArgumentException error) {
            throw new RangeNotSatisfiable(total);
        }
    }

    public static final class RangeNotSatisfiable extends RuntimeException {
        private final long total;
        public RangeNotSatisfiable(long total) { super("Unsupported or unsatisfiable single byte range"); this.total = total; }
        public long total() { return total; }
    }
}
