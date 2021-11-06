package com.redis.riot;

import lombok.Data;
import picocli.CommandLine;

@Data
public class TransferOptions {

	public enum Progress {
		ASCII, COLOR, BW, NONE
	}

	public enum SkipPolicy {
		ALWAYS, NEVER, LIMIT
	}

	@CommandLine.Option(names = "--progress-style", description = "Style for the progress bar: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
	private Progress progress = Progress.COLOR;
	@CommandLine.Option(names = "--progress-interval", description = "Progress update interval in milliseconds (default: ${DEFAULT-VALUE})", paramLabel = "<ms>", hidden = true)
	private long progressUpdateIntervalMillis = 300;
	@CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@CommandLine.Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int chunkSize = 50;
	@CommandLine.Option(names = "--skip-policy", description = "Policy to determine if some processing should be skipped: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<policy>")
	private SkipPolicy skipPolicy = SkipPolicy.LIMIT;
	@CommandLine.Option(names = "--skip-limit", description = "For LIMIT policy, max number of failed items to skip before considering the transfer has failed (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int skipLimit = 3;

}
