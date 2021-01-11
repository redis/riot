package com.redislabs.riot;

import lombok.Data;
import org.springframework.batch.item.redis.support.DatasetSizeEstimatorBuilder;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;
import picocli.CommandLine.Option;

@Data
public class RedisExportOptions {

    @Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private long scanCount = KeyValueItemReaderBuilder.DEFAULT_SCAN_COUNT;
    @Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
    private String scanMatch = KeyValueItemReaderBuilder.DEFAULT_KEY_PATTERN;
    @Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int queueCapacity = KeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
    @Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = KeyValueItemReaderBuilder.DEFAULT_THREAD_COUNT;
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int batchSize = KeyValueItemReaderBuilder.DEFAULT_CHUNK_SIZE;
    @Option(names = "--sample-size", description = "Number of samples used to estimate dataset size (default: ${DEFAULT-VALUE}).", paramLabel = "<int>", hidden = true)
    private int sampleSize = DatasetSizeEstimatorBuilder.DEFAULT_SAMPLE_SIZE;

}
