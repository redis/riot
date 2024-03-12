package com.redis.riot.cli;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.util.ClassUtils;

import com.redis.riot.core.AbstractJobRunnable;
import com.redis.riot.core.RiotStep;

import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine.Option;

abstract class AbstractJobCommand extends AbstractRiotCommand {

	public enum ProgressStyle {
		BLOCK, BAR, ASCII, LOG, NONE
	}

	@Option(names = "--sleep", description = "Duration in ms to sleep after each batch write (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	long sleep;

	@Option(names = "--threads", description = "Number of concurrent threads to use for batch processing (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	int threads = AbstractJobRunnable.DEFAULT_THREADS;

	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	int chunkSize = AbstractJobRunnable.DEFAULT_CHUNK_SIZE;

	@Option(names = "--dry-run", description = "Enable dummy writes.")
	boolean dryRun;

	@Option(names = "--skip-limit", description = "Max number of failed items before considering the transfer has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	int skipLimit = AbstractJobRunnable.DEFAULT_SKIP_LIMIT;

	@Option(names = "--retry-limit", description = "Maximum number of times to try failed items. 0 and 1 both mean no retry. (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int retryLimit = AbstractJobRunnable.DEFAULT_RETRY_LIMIT;

	@Option(names = "--progress", description = "Progress style: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<style>")
	ProgressStyle progressStyle = ProgressStyle.ASCII;

	@Option(names = "--progress-interval", description = "Progress update interval in millis (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>", hidden = true)
	int progressUpdateInterval = 300;

	String name;

	public void setName(String name) {
		this.name = name;
	}

	public void setProgressStyle(ProgressStyle style) {
		this.progressStyle = style;
	}

	private ProgressBarStyle progressBarStyle() {
		switch (progressStyle) {
		case BAR:
			return ProgressBarStyle.COLORFUL_UNICODE_BAR;
		case BLOCK:
			return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
		default:
			return ProgressBarStyle.ASCII;
		}
	}

	@Override
	protected AbstractJobRunnable executable() {
		AbstractJobRunnable executable = jobExecutable();
		if (name != null) {
			executable.setName(name);
		}
		executable.setChunkSize(chunkSize);
		executable.setDryRun(dryRun);
		executable.setRetryLimit(retryLimit);
		executable.setSkipLimit(skipLimit);
		executable.setSleep(Duration.ofMillis(sleep));
		executable.setThreads(threads);
		executable.setStepConfigurer(this::configureStep);
		return executable;
	}

	private void configureStep(RiotStep<?, ?> step) {
		if (progressStyle == ProgressStyle.NONE) {
			return;
		}
		ProgressBarBuilder progressBar = new ProgressBarBuilder();
		progressBar.setTaskName(taskName(step));
		progressBar.setStyle(progressBarStyle());
		progressBar.setUpdateIntervalMillis(progressUpdateInterval);
		progressBar.showSpeed();
		if (progressStyle == ProgressStyle.LOG) {
			Logger logger = LoggerFactory.getLogger(getClass());
			progressBar.setConsumer(new DelegatingProgressBarConsumer(logger::info));
		}
		ProgressStepExecutionListener listener = new ProgressStepExecutionListener(progressBar);
		listener.setExtraMessageSupplier(extraMessageSupplier(step));
		listener.setInitialMaxSupplier(initialMaxSupplier(step));
		step.setConfigurer(s -> {
			s.listener((StepExecutionListener) listener);
			s.listener((ItemWriteListener<?>) listener);
		});
	}

	protected String taskName(RiotStep<?, ?> step) {
		return ClassUtils.getShortName(getClass());
	}

	protected Supplier<String> extraMessageSupplier(RiotStep<?, ?> step) {
		return () -> ProgressStepExecutionListener.EMPTY_STRING;
	}

	protected Callable<Long> initialMaxSupplier(RiotStep<?, ?> step) {
		return () -> ProgressStepExecutionListener.UNKNOWN_SIZE;
	}

	protected abstract AbstractJobRunnable jobExecutable();

}
