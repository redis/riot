package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

public class ExportOptions {

    @Getter
    @Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long scanCount = 1000;
    @Getter
    @Option(names = "--match", description = "SCAN MATCH pattern", paramLabel = "<pattern>")
    private String scanMatch;
    @Getter
    @Option(names = "--reader-queue", description = "Capacity of the value-reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int queueCapacity = 10000;
    @Getter
    @Option(names = "--reader-threads", description = "Number of value-reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @Getter
    @Option(names = "--reader-batch", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int batchSize = 50;

}