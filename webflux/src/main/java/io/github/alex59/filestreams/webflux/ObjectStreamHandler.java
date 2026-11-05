package io.github.alex59.filestreams.webflux;

import io.github.alex59.filestreams.*;

import org.springframework.core.io.buffer.*;
import org.springframework.http.*;
import org.springframework.web.server.*;
import org.springframework.web.util.UriUtils;

import reactor.core.publisher.*;

import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Raw-body /objects/{key} handler. Applications add authentication, authorization and CORS
 * separately.
 */
public final class ObjectStreamHandler implements WebHandler {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final ObjectStreams objects;
    private final StreamLimits limits;

    public ObjectStreamHandler(ObjectStreams objects, StreamLimits limits) {
        this.objects = Objects.requireNonNull(objects);
        this.limits = Objects.requireNonNull(limits);
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange) {
        return Mono.defer(
                        () -> {
                            String path = exchange.getRequest().getURI().getRawPath();
                            if (!path.startsWith("/objects/") || path.length() == 9) {
                                exchange.getResponse().setStatusCode(HttpStatus.NOT_FOUND);
                                return exchange.getResponse().setComplete();
                            }
                            String key;
                            try {
                                key = UriUtils.decode(path.substring(9), StandardCharsets.UTF_8);
                            } catch (IllegalArgumentException invalid) {
                                return Mono.error(new InvalidRequest());
                            }
                            return transfer(exchange, key);
                        })
                .onErrorResume(error -> failure(exchange, error));
    }

    /**
     * Serve an application-authorized key from a controller or custom route. The caller validates
     * access before delegation; the raw-body, commit, range and cleanup contracts remain unchanged.
     */
    public Mono<Void> handle(ServerWebExchange exchange, String key) {
        return Mono.defer(() -> transfer(exchange, key))
                .onErrorResume(error -> failure(exchange, error));
    }

    private Mono<Void> transfer(ServerWebExchange exchange, String key) {
        HttpMethod method = exchange.getRequest().getMethod();
        if (method == HttpMethod.PUT) return upload(exchange, key);
        if (method == HttpMethod.GET) return download(exchange, key);
        exchange.getResponse().setStatusCode(HttpStatus.METHOD_NOT_ALLOWED);
        exchange.getResponse().getHeaders().setAllow(Set.of(HttpMethod.GET, HttpMethod.PUT));
        return exchange.getResponse().setComplete();
    }

    private Mono<Void> upload(ServerWebExchange exchange, String key) {
        HttpHeaders headers = exchange.getRequest().getHeaders();
        UploadRequest request;
        try {
            OptionalLong length = declaredLength(headers);
            String type = headers.getFirst(HttpHeaders.CONTENT_TYPE);
            request =
                    new UploadRequest(
                            key,
                            length,
                            type == null ? "application/octet-stream" : type,
                            Map.of());
        } catch (IllegalArgumentException invalid) {
            return Mono.error(new InvalidRequest());
        }
        // Copy pooled network memory into an immutable, bounded SDK-owned view.
        Flux<ByteBuffer> source =
                exchange.getRequest()
                        .getBody()
                        .map(
                                buffer -> {
                                    try {
                                        int size = buffer.readableByteCount();
                                        if (size > limits.maxBufferBytes())
                                            throw new StreamFailure(
                                                    StreamFailure.Code.BUFFER_TOO_LARGE,
                                                    "Request buffer exceeds configured limit");
                                        byte[] owned = new byte[size];
                                        buffer.read(owned);
                                        return ByteBuffer.wrap(owned);
                                    } finally {
                                        DataBufferUtils.release(buffer);
                                    }
                                })
                        .doOnDiscard(DataBuffer.class, DataBufferUtils::release);
        return objects.upload(request, source)
                .flatMap(
                        result ->
                                json(
                                        exchange,
                                        HttpStatus.CREATED,
                                        Map.of(
                                                "committed",
                                                true,
                                                "key",
                                                result.key(),
                                                "bytes",
                                                result.bytes(),
                                                "etag",
                                                result.etag())));
    }

    private Mono<Void> download(ServerWebExchange exchange, String key) {
        List<String> ranges = exchange.getRequest().getHeaders().get(HttpHeaders.RANGE);
        if (ranges != null && ranges.size() != 1)
            return Mono.error(new ByteRange.RangeNotSatisfiable(-1));
        String range = ranges == null ? null : ranges.get(0);
        return Mono.usingWhen(
                objects.download(key, range),
                opened -> {
                    var response = exchange.getResponse();
                    response.setStatusCode(
                            opened.range() == null ? HttpStatus.OK : HttpStatus.PARTIAL_CONTENT);
                    HttpHeaders headers = response.getHeaders();
                    headers.setContentLength(opened.length());
                    headers.set(HttpHeaders.CONTENT_TYPE, opened.contentType());
                    headers.set(HttpHeaders.ETAG, opened.etag());
                    headers.set(HttpHeaders.ACCEPT_RANGES, "bytes");
                    if (opened.range() != null)
                        headers.set(HttpHeaders.CONTENT_RANGE, opened.range().responseHeader());
                    return response.writeWith(
                            opened.body()
                                    .map(buffer -> response.bufferFactory().wrap(buffer))
                                    .doOnDiscard(DataBuffer.class, DataBufferUtils::release));
                },
                opened -> Mono.fromRunnable(opened::close),
                (opened, error) -> Mono.fromRunnable(opened::close),
                opened -> Mono.fromRunnable(opened::close));
    }

    public static OptionalLong declaredLength(HttpHeaders headers) {
        OptionalLong ordinary = parseLength(headers, HttpHeaders.CONTENT_LENGTH);
        OptionalLong custom = parseLength(headers, "X-Upload-Length");
        if (ordinary.isPresent()
                && custom.isPresent()
                && ordinary.getAsLong() != custom.getAsLong())
            throw new IllegalArgumentException("Content-Length and X-Upload-Length disagree");
        return custom.isPresent() ? custom : ordinary;
    }

    private static OptionalLong parseLength(HttpHeaders headers, String name) {
        List<String> values = headers.get(name);
        if (values == null) return OptionalLong.empty();
        if (values.size() != 1 || !values.get(0).matches("[0-9]{1,19}"))
            throw new IllegalArgumentException("Invalid " + name);
        return OptionalLong.of(Long.parseLong(values.get(0)));
    }

    private Mono<Void> failure(ServerWebExchange exchange, Throwable error) {
        if (exchange.getResponse().isCommitted()) return Mono.error(error);
        Throwable specific = error;
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof StreamFailure || cause instanceof ByteRange.RangeNotSatisfiable) {
                specific = cause;
                break;
            }
        }
        HttpStatus status = HttpStatus.BAD_GATEWAY;
        String code = "STORAGE_FAILURE", message = "Storage did not confirm the transfer outcome";
        Boolean committed = null;
        if (specific instanceof StreamFailure failure) {
            code = failure.code().name();
            message = failure.getMessage();
            committed = false;
            status =
                    failure.code() == StreamFailure.Code.TOO_LARGE
                            ? HttpStatus.PAYLOAD_TOO_LARGE
                            : failure.code() == StreamFailure.Code.BUSY
                                    ? HttpStatus.SERVICE_UNAVAILABLE
                                    : HttpStatus.BAD_REQUEST;
        } else if (specific instanceof ByteRange.RangeNotSatisfiable range) {
            status = HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
            code = "INVALID_RANGE";
            message = range.getMessage();
            committed = false;
            if (range.total() >= 0)
                exchange.getResponse()
                        .getHeaders()
                        .set(HttpHeaders.CONTENT_RANGE, "bytes */" + range.total());
        } else if (specific instanceof ObjectUnavailable unavailable) {
            status =
                    unavailable.reason() == ObjectUnavailable.Reason.NOT_FOUND
                            ? HttpStatus.NOT_FOUND
                            : HttpStatus.PRECONDITION_FAILED;
            code = unavailable.reason().name();
            message = unavailable.getMessage();
            committed = false;
        } else if (specific instanceof InvalidRequest) {
            status = HttpStatus.BAD_REQUEST;
            code = "INVALID_REQUEST";
            message = "Invalid request metadata";
            committed = false;
        }
        // A null committed value is deliberate: response loss during finalization is ambiguous.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("committed", committed);
        body.put("code", code);
        body.put("message", message);
        exchange.getResponse().getHeaders().remove(HttpHeaders.CONTENT_RANGE);
        if (specific instanceof ByteRange.RangeNotSatisfiable range && range.total() >= 0)
            exchange.getResponse()
                    .getHeaders()
                    .set(HttpHeaders.CONTENT_RANGE, "bytes */" + range.total());
        return json(exchange, status, body);
    }

    private static Mono<Void> json(ServerWebExchange exchange, HttpStatus status, Object value) {
        byte[] bytes = JSON.writeValueAsBytes(value);
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        exchange.getResponse().getHeaders().setContentLength(bytes.length);
        return exchange.getResponse()
                .writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(bytes)));
    }

    private static final class InvalidRequest extends RuntimeException {}
}
