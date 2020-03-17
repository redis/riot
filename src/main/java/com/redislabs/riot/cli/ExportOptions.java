package com.redislabs.riot.cli;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class ExportOptions {

	@Option(names = "--count", description = "SCAN COUNT option (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private long count = 1000;
	@Option(names = "--match", description = "SCAN MATCH pattern", paramLabel = "<pattern>")
	private String match;
	@Option(names = "--queue", description = "Capacity of the value queue (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int queue = 10000;
	@Option(names = "--reader-threads", description = "Number of value-reader threads (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = "--reader-pipeline", description = "Number of values in reader pipeline (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int pipeline = 50;
	@Option(names = "--reader-timeout", description = "Command timeout duration in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private int timeout = 2;

}
