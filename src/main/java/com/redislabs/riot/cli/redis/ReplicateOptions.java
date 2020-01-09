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
	@Option(names = "--queue-size", description = "Capacity of the key/value queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int queueSize = 10000;
	@Option(names = "--scan-threads", description = "Number of value reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = "--scan-batch", description = "Number of values in dump pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int batchSize = 50;
	@Option(names = "--reader-timeout", description = "Command timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private int timeout = 2;
	@Option(names = "--keyspace-channel", description = "Pub/sub channel for keyspace events (default: ${DEFAULT-VALUE}). Blank to disable", paramLabel = "<string>")
	private String channel = "__keyspace@0__:*";
	@Option(names = "--listen", description = "Keep keyspace notification listener open after scan is finished (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean listen = true;
	@Option(names = "--replace", description = "Use REPLACE modifier with RESTORE command (default: ${DEFAULT-VALUE})", negatable = true)
	private boolean replace = true;

}
