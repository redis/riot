package com.redislabs.riot;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.item.redis.support.*;
import picocli.CommandLine.Option;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisReaderOptions {

    @Builder.Default
    @Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private long scanCount = AbstractScanKeyValueItemReaderBuilder.DEFAULT_SCAN_COUNT;
    @Builder.Default
    @Option(names = "--match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE}).", paramLabel = "<glob>")
    private String scanMatch = AbstractScanKeyValueItemReaderBuilder.DEFAULT_SCAN_MATCH;
    @Option(names = "--type", description = "SCAN TYPE option: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    private DataType scanType;
    @Builder.Default
    @Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int queueCapacity = AbstractKeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
    @Builder.Default
    @Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int threads = AbstractKeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_THREAD_COUNT;
    @Builder.Default
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int batchSize = AbstractKeyValueItemReader.KeyValueItemReaderBuilder.DEFAULT_CHUNK_SIZE;
    @Builder.Default
    @Option(names = "--sample-size", description = "Number of samples used to estimate dataset size (default: ${DEFAULT-VALUE}).", paramLabel = "<int>", hidden = true)
    private int sampleSize = ScanSizeEstimator.Options.DEFAULT_SAMPLE_SIZE;

    public <B extends AbstractScanKeyValueItemReaderBuilder> B configureScan(B builder) {
        builder.scanMatch(scanMatch);
        builder.scanCount(scanCount);
        if (scanType != null) {
            builder.scanType(scanType.code());
        }
        return configure(builder);
    }

    public <B extends AbstractKeyValueItemReader.KeyValueItemReaderBuilder> B configure(B builder) {
        builder.threadCount(threads);
        builder.chunkSize(batchSize);
        builder.queueCapacity(queueCapacity);
        return builder;
    }

    public ScanSizeEstimator.Options sizeEstimatorOptions() {
        return ScanSizeEstimator.Options.builder().sampleSize(sampleSize).match(scanMatch).type(scanType).build();
    }

}
