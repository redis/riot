package com.redislabs.riot;

import org.springframework.batch.item.redis.support.KeyItemReader.KeyItemReaderBuilder;
import org.springframework.batch.item.redis.support.KeyValueItemReaderBuilder;

import lombok.Getter;
import picocli.CommandLine.Option;

@Getter
public class RedisExportOptions {

    @Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long scanCount = KeyItemReaderBuilder.DEFAULT_SCAN_COUNT;
    @Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE})", paramLabel = "<glob>")
    private String scanMatch = KeyItemReaderBuilder.DEFAULT_SCAN_MATCH;
    @Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int queueCapacity = KeyValueItemReaderBuilder.DEFAULT_QUEUE_CAPACITY;
    @Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int readerThreads = KeyValueItemReaderBuilder.DEFAULT_THREAD_COUNT;
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int readerBatchSize = KeyValueItemReaderBuilder.DEFAULT_BATCH_SIZE;
}
