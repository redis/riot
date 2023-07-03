package com.redis.riot.cli.common;

import java.time.Duration;

import com.redis.spring.batch.reader.ReaderOptions;

import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine.Option;

public class JobOptions {

	public static final StepSkipPolicy DEFAULT_SKIP_POLICY = StepSkipPolicy.LIMIT;
	public static final int DEFAULT_CHUNK_SIZE = ReaderOptions.DEFAULT_CHUNK_SIZE;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_SKIP_LIMIT = 3;
	public static final Duration DEFAULT_PROGRESS_UPDATE_INTERVAL = Duration.ofMillis(300);

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
	private ProgressBarStyle progressBarStyle = StepProgressMonitor.DEFAULT_STYLE;

	@Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}). Use 0 for no progress bar", paramLabel = "<ms>", hidden = true)
	private long progressUpdateInterval = DEFAULT_PROGRESS_UPDATE_INTERVAL.toMillis();

	public ProgressBarStyle getProgressBarStyle() {
		return progressBarStyle;
	}

	public long getProgressUpdateInterval() {
		return progressUpdateInterval;
	}

	public void setProgressStyle(ProgressBarStyle style) {
		this.progressBarStyle = style;
	}

	public void setProgressUpdateInterval(long millis) {
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

	public void setProgressBarStyle(ProgressBarStyle progressBarStyle) {
		this.progressBarStyle = progressBarStyle;
	}

	@Override
	public String toString() {
		return "TransferOptions [threads=" + threads + ", chunkSize=" + chunkSize + ", skipPolicy=" + skipPolicy
				+ ", skipLimit=" + skipLimit + ", sleep=" + sleep + "]";
	}

}
