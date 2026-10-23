package io.github.alex59.filestreams.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.net.URI;
import java.time.Duration;

/** Stock AWS async multipart configuration. The caller owns and closes the returned client. */
public final class S3Clients {
    public static final long PART_BYTES = 5L * 1024 * 1024;
    public static final long SDK_BUFFER_BYTES = 2 * PART_BYTES;

    private S3Clients() {}

    public static S3AsyncClient bounded(
            URI endpoint, Region region, AwsCredentialsProvider credentials) {
        var builder =
                S3AsyncClient.builder()
                        .region(region)
                        .credentialsProvider(credentials)
                        .forcePathStyle(true)
                        .multipartEnabled(true)
                        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                        .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                        .multipartConfiguration(
                                config ->
                                        config.minimumPartSizeInBytes(PART_BYTES)
                                                .thresholdInBytes(PART_BYTES)
                                                .apiCallBufferSizeInBytes(SDK_BUFFER_BYTES)
                                                .parallelConfiguration(
                                                        parallel -> parallel.maxInFlightParts(1)))
                        .overrideConfiguration(
                                config ->
                                        config.retryStrategy(
                                                        StandardRetryStrategy.builder()
                                                                .maxAttempts(1)
                                                                .build())
                                                .apiCallAttemptTimeout(Duration.ofSeconds(60))
                                                .apiCallTimeout(Duration.ofMinutes(10)))
                        .httpClientBuilder(
                                NettyNioAsyncHttpClient.builder()
                                        .maxConcurrency(8)
                                        .proxyConfiguration(
                                                software.amazon.awssdk.http.nio.netty
                                                        .ProxyConfiguration.builder()
                                                        .useEnvironmentVariableValues(false)
                                                        .useSystemPropertyValues(false)
                                                        .build())
                                        .maxPendingConnectionAcquires(16)
                                        .connectionTimeout(Duration.ofSeconds(5))
                                        .readTimeout(Duration.ofSeconds(60))
                                        .writeTimeout(Duration.ofSeconds(60)));
        if (endpoint != null) builder.endpointOverride(endpoint);
        return builder.build();
    }
}
