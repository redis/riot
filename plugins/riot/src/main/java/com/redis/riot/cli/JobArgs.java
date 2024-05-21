package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.cli.AbstractRiotCommand.ProgressStyle;
import com.redis.riot.core.AbstractRiotCallable;

import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine.Option;

public class JobArgs {

	@Option(names = "--sleep", description = "Duration in ms to sleep after each batch write (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long sleep;

	@Option(names = "--threads", description = "Number of concurrent threads to use for batch processing (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = AbstractRiotCallable.DEFAULT_THREADS;

	@Option(names = "--batch", description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int chunkSize = AbstractRiotCallable.DEFAULT_CHUNK_SIZE;

	@Option(names = "--dry-run", description = "Enable dummy writes.")
	private boolean dryRun;

	@Option(names = "--skip-limit", description = "Max number of failed items before considering the transfer has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int skipLimit = AbstractRiotCallable.DEFAULT_SKIP_LIMIT;

	@Option(names = "--retry-limit", description = "Maximum number of times to try failed items. 0 and 1 both mean no retry. (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int retryLimit = AbstractRiotCallable.DEFAULT_RETRY_LIMIT;

	@Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<style>")
	private ProgressStyle progressStyle = ProgressStyle.ASCII;

	@Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
	private int progressUpdateInterval = 300;

	public ProgressBarStyle progressBarStyle() {
		switch (progressStyle) {
		case BAR:
			return ProgressBarStyle.COLORFUL_UNICODE_BAR;
		case BLOCK:
			return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
		default:
			return ProgressBarStyle.ASCII;
		}
	}

	public void configure(AbstractRiotCallable callable) {
		callable.setChunkSize(chunkSize);
		callable.setDryRun(dryRun);
		callable.setRetryLimit(retryLimit);
		callable.setSkipLimit(skipLimit);
		callable.setSleep(Duration.ofMillis(sleep));
		callable.setThreads(threads);
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

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

	public ProgressStyle getProgressStyle() {
		return progressStyle;
	}

	public void setProgressStyle(ProgressStyle progressStyle) {
		this.progressStyle = progressStyle;
	}

	public int getProgressUpdateInterval() {
		return progressUpdateInterval;
	}

	public void setProgressUpdateInterval(int progressUpdateInterval) {
		this.progressUpdateInterval = progressUpdateInterval;
	}

}
