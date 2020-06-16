package com.redislabs.riot.cli;

import lombok.Getter;
import org.springframework.batch.item.redis.support.ReaderOptions;
import picocli.CommandLine.Option;

public class ExportOptions {

    @Getter
    @Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long scanCount = ReaderOptions.DEFAULT_SCAN_COUNT;
    @Getter
    @Option(names = "--match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String scanMatch = ReaderOptions.DEFAULT_SCAN_MATCH;
    @Getter
    @Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int queueCapacity = 10000;
    @Getter
    @Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int threads = 1;
    @Getter
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int batchSize = 50;

}