package com.redis.riot.cli.common;

import org.springframework.util.Assert;

import com.redis.spring.batch.reader.ReaderOptions;

import picocli.CommandLine.Option;

public class JobOptions {

	public enum ProgressStyle {
		BLOCK, BAR, ASCII, LOG, NONE
	}

	public static final StepSkipPolicy DEFAULT_SKIP_POLICY = StepSkipPolicy.LIMIT;
	public static final int DEFAULT_CHUNK_SIZE = ReaderOptions.DEFAULT_CHUNK_SIZE;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_SKIP_LIMIT = 3;
	public static final int DEFAULT_PROGRESS_UPDATE_INTERVAL = 1000;
	public static final ProgressStyle DEFAULT_PROGRESS_STYLE = ProgressStyle.ASCII;

	@Option(names = "--sleep", description = "Duration in ms to sleep after each batch write (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long sleep;

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = DEFAULT_THREADS;

	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int chunkSize = DEFAULT_CHUNK_SIZE;

	@Option(names = "--skip-policy", description = "Policy to determine if some processing should be skipped: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private StepSkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

	@Option(names = "--skip-limit", description = "LIMIT skip policy: max number of failed items before considering the transfer has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int skipLimit = DEFAULT_SKIP_LIMIT;

	@Option(names = "--progress", description = "Style of progress bar: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}),", paramLabel = "<style>")
	private ProgressStyle progressStyle = DEFAULT_PROGRESS_STYLE;

	@Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
	private int progressUpdateInterval = DEFAULT_PROGRESS_UPDATE_INTERVAL;

	public ProgressStyle getProgressStyle() {
		return progressStyle;
	}

	public int getProgressUpdateInterval() {
		return progressUpdateInterval;
	}

	public void setProgressStyle(ProgressStyle style) {
		this.progressStyle = style;
	}

	public void setProgressUpdateInterval(int millis) {
		Assert.isTrue(millis > 0, "Update interval must be strictly greater than zero");
		this.progressUpdateInterval = millis;
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

	public StepSkipPolicy getSkipPolicy() {
		return skipPolicy;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	@Override
	public String toString() {
		return "JobOptions [sleep=" + sleep + ", threads=" + threads + ", chunkSize=" + chunkSize + ", skipPolicy="
				+ skipPolicy + ", skipLimit=" + skipLimit + ", progressStyle=" + progressStyle
				+ ", progressUpdateInterval=" + progressUpdateInterval + "]";
	}

}
