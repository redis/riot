package com.redislabs.riot.cli;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class TransferOptions {

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@Option(names = { "-m", "--max" }, description = "Max number of items to read", paramLabel = "<count>")
	private Long maxItemCount;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
	private Long sleep;
	@Option(names = "--progress", description = "Progress reporting interval (default: ${DEFAULT-VALUE} ms)", paramLabel = "<ms>")
	private long progressRate = 300;
	@Option(names = "--max-wait", description = "Max duration to wait for transfer to complete", paramLabel = "<ms>")
	private Long maxWait;
}
