package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;

import com.redis.spring.batch.common.FaultToleranceOptions;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import picocli.CommandLine.Option;

public class TransferOptions {

	public static final int DEFAULT_CHUNK_SIZE = 0;

	@Option(names = "--sleep", description = "Duration in ms to sleep before each item read (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long sleep;

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int threads = 1;

	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	private int chunkSize = DEFAULT_CHUNK_SIZE;

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

	public ProgressMonitor.Builder progressMonitor() {
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

	public FaultToleranceOptions faultToleranceOptions() {
		return FaultToleranceOptions.builder().skipPolicy(skipPolicy(skipPolicy, skipLimit)).skipLimit(skipLimit)
				.build();
	}

	public static SkipPolicy skipPolicy(StepSkipPolicy policy, int skipLimit) {
		switch (policy) {
		case ALWAYS:
			return new AlwaysSkipItemSkipPolicy();
		case NEVER:
			return new NeverSkipItemSkipPolicy();
		default:
			return new LimitCheckingItemSkipPolicy(skipLimit, skippableExceptions());
		}
	}

	private static Map<Class<? extends Throwable>, Boolean> skippableExceptions() {
		return Stream
				.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class)
				.collect(Collectors.toMap(Function.identity(), t -> true));
	}

}
