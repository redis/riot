package com.redis.riot;

import java.time.Duration;

import com.redis.riot.ProgressMonitor.Builder;

import picocli.CommandLine.Option;

public class TransferOptions {

	@Option(names = "--sleep", description = "Duration in ms to sleep before each item read (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long sleep;

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = 1;

	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int chunkSize = 50;

	@Option(names = "--skip-policy", description = "Policy to determine if some processing should be skipped: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private StepSkipPolicy skipPolicy = StepSkipPolicy.LIMIT;

	@Option(names = "--skip-limit", description = "LIMIT skip policy: max number of failed items before considering the transfer has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int skipLimit = 3;

	@Option(names = "--progress", description = "Style of progress bar: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}),", paramLabel = "<style>")
	private ProgressStyle progressStyle = ProgressStyle.BAR;

	@Option(names = "--progress-interval", description = "Progress update interval in milliseconds (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
	private long progressUpdateInterval = 30;

	public ProgressStyle getProgressStyle() {
		return progressStyle;
	}

	public long getProgressUpdateInterval() {
		return progressUpdateInterval;
	}

	public void setProgressStyle(ProgressStyle style) {
		this.progressStyle = style;
	}

	public void setProgressUpdateInterval(long millis) {
		this.progressUpdateInterval = millis;
	}

	public Builder progressMonitor() {
		return ProgressMonitor.style(progressStyle).updateInterval(Duration.ofMillis(progressUpdateInterval));
	}

	public boolean isProgressEnabled() {
		return getProgressStyle() != ProgressStyle.NONE;
	}

	public long getSleep() {
		return sleep;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

	public int getThreads() {
		return threads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public StepSkipPolicy getSkipPolicy() {
		return skipPolicy;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public void setSkipPolicy(StepSkipPolicy skipPolicy) {
		this.skipPolicy = skipPolicy;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	@Override
	public String toString() {
		return "TransferOptions [threads=" + threads + ", chunkSize=" + chunkSize + ", skipPolicy=" + skipPolicy
				+ ", skipLimit=" + skipLimit + ", sleep=" + sleep + "]";
	}

}
