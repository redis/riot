package com.redislabs.riot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;
import picocli.CommandLine.Option;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisReaderOptions {

    @Builder.Default
    @Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private long scanCount = KeyValueItemReaderBuilder.DEFAULT_SCAN_COUNT;
    @Builder.Default
    @Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
    private String scanMatch = KeyValueItemReaderBuilder.DEFAULT_KEY_PATTERN;
    @Builder.Default
    @Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int queueCapacity = KeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
    @Builder.Default
    @Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = KeyValueItemReaderBuilder.DEFAULT_THREAD_COUNT;
    @Builder.Default
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int batchSize = KeyValueItemReaderBuilder.DEFAULT_CHUNK_SIZE;
    @Builder.Default
    @Option(names = "--sample-size", description = "Number of samples used to estimate dataset size (default: ${DEFAULT-VALUE}).", paramLabel = "<int>", hidden = true)
    private int sampleSize = KeyValueItemReaderBuilder.DEFAULT_SAMPLE_SIZE;

}
