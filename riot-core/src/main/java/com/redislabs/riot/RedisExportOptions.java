package com.redislabs.riot;

import lombok.Getter;
import picocli.CommandLine;

@Getter
public class RedisExportOptions {

	@CommandLine.Option(names = "--count", defaultValue = "1000", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long scanCount;
	@CommandLine.Option(names = "--pattern", defaultValue = "*", description = "SCAN pattern (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String scanMatch;
	@CommandLine.Option(names = "--reader-queue", defaultValue = "10000", description = "Capacity of the reader queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
	private int queueCapacity;
	@CommandLine.Option(names = "--reader-threads", defaultValue = "1", description = "Number of reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>", hidden = true)
	private int threads;
	@CommandLine.Option(names = "--reader-batch", defaultValue = "50", description = "Number of reader values to process at once (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int batchSize;
	@CommandLine.Option(names = "--cursor-maxidle", description = "Cursor idle timeout", paramLabel = "<int>")
	private Long cursorMaxIdle;
	@CommandLine.Option(names = "--cursor-count", description = "Cursor count", paramLabel = "<int>")
	private Long cursorCount;

}
