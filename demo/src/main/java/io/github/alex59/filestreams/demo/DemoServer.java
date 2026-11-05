package io.github.alex59.filestreams.demo;

import io.github.alex59.filestreams.*;
import io.github.alex59.filestreams.s3.*;
import io.github.alex59.filestreams.webflux.ObjectStreamHandler;

import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.cors.*;
import org.springframework.web.cors.reactive.*;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;

import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.net.URI;
import java.util.List;

/**
 * EN: Local demo only: bind to loopback, no authentication; keep access restricted.
 * RU: Локальная демонстрация без аутентификации: loopback, доступ должен быть ограничен.
 */
public final class DemoServer {
    private DemoServer() {}

    public static void main(String[] args) {
        var endpoint = URI.create(env("S3_ENDPOINT", "http://127.0.0.1:19390"));
        var credentials =
                StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                env("S3_ACCESS_KEY", "GK11111111111111111111111111111111"),
                                env(
                                        "S3_SECRET_KEY",
                                        "synthetic-local-test-secret-not-for-production")));
        try (var client =
                S3Clients.bounded(endpoint, Region.of(env("S3_REGION", "garage")), credentials)) {
            var limits = io.github.alex59.filestreams.StreamProfile.MEDIA.limits();
            var store = new S3ObjectStreams(client, env("S3_BUCKET", "stream-tests"), limits, 2);
            var cors = new CorsConfiguration();
            cors.setAllowedOrigins(List.of(env("DEMO_ORIGIN", "http://127.0.0.1:18080")));
            cors.setAllowedMethods(List.of("GET", "PUT", "OPTIONS"));
            cors.setAllowedHeaders(List.of("Content-Type", "X-Upload-Length", "Range"));
            cors.setExposedHeaders(
                    List.of(
                            "ETag",
                            "Content-Length",
                            "Content-Range",
                            "Accept-Ranges",
                            "Content-Encoding"));
            var source =
                    new org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource();
            source.registerCorsConfiguration("/**", cors);
            var handler =
                    WebHttpHandlerBuilder.webHandler(new ObjectStreamHandler(store, limits))
                            .filter(new CorsWebFilter(source))
                            .build();
            HttpServer server =
                    HttpServer.create()
                            .host("127.0.0.1")
                            .port(Integer.parseInt(env("DEMO_PORT", "19443")))
                            .compress(false)
                            .httpRequestDecoder(config -> config.maxChunkSize(65536));
            String certificate = System.getenv("DEMO_TLS_CERT"),
                    privateKey = System.getenv("DEMO_TLS_KEY");
            if (certificate != null && privateKey != null) {
                server =
                        server.protocol(HttpProtocol.H2, HttpProtocol.HTTP11)
                                .secure(
                                        spec ->
                                                spec.sslContext(
                                                        reactor.netty.http.Http2SslContextSpec
                                                                .forServer(
                                                                        new File(certificate),
                                                                        new File(privateKey))));
            }
            var bound = server.handle(new ReactorHttpHandlerAdapter(handler)).bindNow();
            System.out.println(
                    "Demo listening on "
                            + (certificate == null ? "http" : "https")
                            + "://127.0.0.1:"
                            + bound.port());
            Runtime.getRuntime().addShutdownHook(new Thread(bound::disposeNow));
            bound.onDispose().block();
        }
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }
}
