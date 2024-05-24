package com.redis.riot.core;

import org.springframework.retry.policy.MaxAttemptsRetryPolicy;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class JobArgs {

	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_THREADS = 1;

	@Option(names = "--job-name", description = "Job name.", paramLabel = "<string>", hidden = true)
	protected String name;

	@Option(names = "--sleep", description = "Duration in millis to wait after each batch write (default: no sleep).", paramLabel = "<ms>")
	private long sleep;

	@Option(names = "--threads", description = "Number of concurrent threads to use for batch processing (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = DEFAULT_THREADS;

	@Option(names = "--batch", description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int chunkSize = DEFAULT_CHUNK_SIZE;

	@Option(names = "--dry-run", description = "Enable dummy writes.")
	private boolean dryRun;

	@Option(names = "--skip-limit", description = "Max number of failed items before considering the transfer has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int skipLimit = DEFAULT_SKIP_LIMIT;

	@Option(names = "--retry-limit", description = "Max number of times to try failed items (default: ${DEFAULT-VALUE}). 0 and 1 both mean no retry.", paramLabel = "<int>")
	private int retryLimit = DEFAULT_RETRY_LIMIT;

	@ArgGroup(exclusive = false)
	private ProgressArgs progressArgs = new ProgressArgs();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public ProgressArgs getProgressArgs() {
		return progressArgs;
	}

	public void setProgressArgs(ProgressArgs args) {
		this.progressArgs = args;
	}

}
