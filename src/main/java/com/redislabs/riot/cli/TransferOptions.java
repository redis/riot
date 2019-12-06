package com.redislabs.riot.cli;

import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class TransferOptions {

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@Option(names = { "-m", "--max" }, description = "Max number of items to read", paramLabel = "<count>")
	private Integer count;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
	private Long sleep;
	@Option(names = "--show-unit", description = "Show unit name in progress bar")
	private boolean showUnit;

}
