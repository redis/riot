package com.redislabs.riot;

import lombok.Getter;
import org.springframework.batch.item.redis.support.RedisItemReaderBuilder;
import picocli.CommandLine;

@Getter
public class RedisExportOptions {

    @CommandLine.Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long scanCount = RedisItemReaderBuilder.DEFAULT_SCAN_COUNT;
    @CommandLine.Option(names = "--match", description = "SCAN MATCH pattern (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String scanMatch = RedisItemReaderBuilder.DEFAULT_SCAN_MATCH;
    @CommandLine.Option(names = "--reader-queue", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int queueCapacity = 10000;
    @CommandLine.Option(names = "--reader-threads", description = "Number of reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
    private int threads = 1;
    @CommandLine.Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int batchSize = 50;
    @CommandLine.Option(names = "--cursor-maxidle", description = "Cursor idle timeout", paramLabel = "<int>")
    private Long cursorMaxIdle;
    @CommandLine.Option(names = "--cursor-count", description = "Cursor count", paramLabel = "<int>")
    private Long cursorCount;

}
