package com.redislabs.riot.cli.redis;

import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class ReplicateOptions {

	@Option(names = "--scan-count", description = "Number of elements to return for each scan call (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long count = 1000;
	@Option(names = "--scan-match", description = "Scan match pattern", paramLabel = "<pattern>")
	private String match;
	@Option(names = "--key-queue-size", description = "Capacity of the key queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int keyQueueSize = 10000;
	@Option(names = "--value-queue-size", description = "Capacity of the value queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int valueQueueSize = 10000;
	@Option(names = "--reader-threads", description = "Number of value reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = "--reader-batch", description = "Number of values in dump pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int batchSize = 50;
	@Option(names = "--reader-timeout", description = "Command timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private int timeout = 2;
	@Option(names = "--keyspace-channel", description = "Pub/sub channel for keyspace events (default: ${DEFAULT-VALUE}). Blank to disable", paramLabel = "<string>")
	private String channel = "__keyspace@0__:*";
	@Option(names = "--no-replace", description = "No REPLACE modifier with RESTORE command")
	private boolean noReplace;
	@Option(names = "--flush-rate", description = "Interval in millis between notification flushes (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long flushRate = 50;

}
