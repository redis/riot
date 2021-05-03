package com.redislabs.riot;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.ScanSizeEstimator;
import picocli.CommandLine.Option;

import java.util.function.Supplier;

@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisReaderOptions {

    @Builder.Default
    @Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private long scanCount = KeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_SCAN_COUNT;
    @Builder.Default
    @Option(names = "--match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
    private String scanMatch = KeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_SCAN_MATCH;
    @Option(names = "--type", description = "SCAN TYPE option: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    private DataType scanType;
    @Builder.Default
    @Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int queueCapacity = KeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
    @Builder.Default
    @Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = KeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_THREADS;
    @Builder.Default
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int batchSize = KeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_CHUNK_SIZE;
    @Builder.Default
    @Option(names = "--sample-size", description = "Number of samples used to estimate dataset size (default: ${DEFAULT-VALUE}).", paramLabel = "<int>", hidden = true)
    private int sampleSize = 100;

    public <B extends KeyValueItemReader.KeyValueItemReaderBuilder> B configure(B builder) {
        builder.scanMatch(scanMatch);
        builder.scanCount(scanCount);
        if (scanType != null) {
            builder.scanType(scanType.name().toLowerCase());
        }
        return (B) configure((KeyValueItemReader.AbstractKeyValueItemReaderBuilder) builder);
    }

    public <B extends KeyValueItemReader.AbstractKeyValueItemReaderBuilder> B configure(B builder) {
        builder.threads(threads);
        builder.chunkSize(batchSize);
        builder.queueCapacity(queueCapacity);
        return builder;
    }

    public ScanSizeEstimator.EstimateOptions estimateOptions() {
        ScanSizeEstimator.EstimateOptions.EstimateOptionsBuilder builder = ScanSizeEstimator.EstimateOptions.builder().match(scanMatch).sampleSize(sampleSize);
        if (scanType != null) {
            builder.type(scanType.name().toLowerCase());
        }
        return builder.build();
    }

    public Supplier<Long> initialMaxSupplier(RedisOptions redisOptions) {
        return () -> {
            AbstractRedisClient client = redisOptions.client();
            ScanSizeEstimator.ScanSizeEstimatorBuilder builder = redisOptions.isCluster() ? ScanSizeEstimator.client((RedisClusterClient) client) : ScanSizeEstimator.client((RedisClient) client);
            ScanSizeEstimator estimator = builder.poolConfig(redisOptions.poolConfig()).build();
            try {
                return estimator.estimate(estimateOptions());
            } catch (Exception e) {
                log.warn("Could not estimate scan size", e);
                return null;
            } finally {
                RedisOptions.shutdown(client);
            }
        };
    }

}
