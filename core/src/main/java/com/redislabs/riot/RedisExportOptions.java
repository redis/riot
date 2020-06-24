package com.redislabs.riot;

import lombok.Getter;
import org.springframework.batch.item.redis.support.RedisItemReaderBuilder;
import picocli.CommandLine;

public class RedisExportOptions {

    @Getter
    @CommandLine.Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long scanCount = RedisItemReaderBuilder.DEFAULT_SCAN_COUNT;
    @Getter
    @CommandLine.Option(names = "--match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String scanMatch = RedisItemReaderBuilder.DEFAULT_SCAN_MATCH;
    @Getter
    @CommandLine.Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int queueCapacity = 10000;
    @Getter
    @CommandLine.Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int threads = 1;
    @Getter
    @CommandLine.Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int batchSize = 50;
}
