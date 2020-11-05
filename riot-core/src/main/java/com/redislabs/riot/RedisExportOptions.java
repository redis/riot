package com.redislabs.riot;

import lombok.Getter;
import picocli.CommandLine.Option;

@Getter
public class RedisExportOptions {

    @Option(names = "--scan-count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long scanCount = 1000;
    @Option(names = "--scan-match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE})", paramLabel = "<glob>")
    private String scanMatch = "*";
    @Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int queueCapacity = 10000;
    @Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int readerThreads = 1;
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int readerBatchSize = 50;
}
