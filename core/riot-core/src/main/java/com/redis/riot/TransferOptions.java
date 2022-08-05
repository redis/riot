package com.redis.riot;

import picocli.CommandLine.Option;

public class TransferOptions {

	public enum Progress {
		ASCII, COLOR, BW, NONE
	}

	public enum SkipPolicy {
		ALWAYS, NEVER, LIMIT
	}

	@Option(names = "--progress", description = "Style of progress bar: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<style>")
	private Progress progress = Progress.COLOR;
	@Option(names = "--progress-interval", description = "Progress update interval in milliseconds (default: ${DEFAULT-VALUE})", paramLabel = "<ms>", hidden = true)
	private long progressUpdateIntervalMillis = 300;
	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int chunkSize = 50;
	@Option(names = "--skip-policy", description = "Policy to determine if some processing should be skipped: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private SkipPolicy skipPolicy = SkipPolicy.LIMIT;
	@Option(names = "--skip-limit", description = "For LIMIT policy, max number of failed items to skip before considering the transfer has failed (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int skipLimit = 3;

	public Progress getProgress() {
		return progress;
	}

	public long getProgressUpdateIntervalMillis() {
		return progressUpdateIntervalMillis;
	}

	public int getThreads() {
		return threads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public SkipPolicy getSkipPolicy() {
		return skipPolicy;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setProgress(Progress progress) {
		this.progress = progress;
	}

	public void setProgressUpdateIntervalMillis(long progressUpdateIntervalMillis) {
		this.progressUpdateIntervalMillis = progressUpdateIntervalMillis;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public void setSkipPolicy(SkipPolicy skipPolicy) {
		this.skipPolicy = skipPolicy;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	@Override
	public String toString() {
		return "TransferOptions [progress=" + progress + ", progressUpdateIntervalMillis="
				+ progressUpdateIntervalMillis + ", threads=" + threads + ", chunkSize=" + chunkSize + ", skipPolicy="
				+ skipPolicy + ", skipLimit=" + skipLimit + "]";
	}

}
