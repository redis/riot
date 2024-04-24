package com.redis.riot.cli;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;

import com.redis.riot.core.AbstractRiotCallable;
import com.redis.riot.core.AbstractRedisCallable;

import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command
abstract class AbstractRiotCommand extends BaseCommand implements Callable<Integer> {

	public enum ProgressStyle {
		BLOCK, BAR, ASCII, LOG, NONE
	}

	@ParentCommand
	protected AbstractMainCommand parent;

	@Mixin
	LoggingMixin loggingMixin = new LoggingMixin();

	@Option(names = "--sleep", description = "Duration in ms to sleep after each batch write (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	long sleep;

	@Option(names = "--threads", description = "Number of concurrent threads to use for batch processing (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	int threads = AbstractRiotCallable.DEFAULT_THREADS;

	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE}).", paramLabel = "<size>")
	int chunkSize = AbstractRiotCallable.DEFAULT_CHUNK_SIZE;

	@Option(names = "--dry-run", description = "Enable dummy writes.")
	boolean dryRun;

	@Option(names = "--skip-limit", description = "Max number of failed items before considering the transfer has failed (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	int skipLimit = AbstractRiotCallable.DEFAULT_SKIP_LIMIT;

	@Option(names = "--retry-limit", description = "Maximum number of times to try failed items. 0 and 1 both mean no retry. (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int retryLimit = AbstractRiotCallable.DEFAULT_RETRY_LIMIT;

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
	public Integer call() throws Exception {
		loggingMixin.configureLogging();
		try (AbstractRedisCallable runnable = runnable()) {
			if (name != null) {
				runnable.setName(name);
			}
			runnable.setChunkSize(chunkSize);
			runnable.setDryRun(dryRun);
			runnable.setRetryLimit(retryLimit);
			runnable.setSkipLimit(skipLimit);
			runnable.setSleep(Duration.ofMillis(sleep));
			runnable.setThreads(threads);
			if (progressStyle != ProgressStyle.NONE) {
				runnable.addStepConfiguration(this::configureProgress);
			}
			runnable.setRedisClientOptions(parent.getRedisArgs().redisOptions());
			runnable.afterPropertiesSet();
			runnable.call();
		}
		return 0;
	}

	private void configureProgress(SimpleStepBuilder<?, ?> step, String stepName, ItemReader<?> reader,
			ItemWriter<?> writer) {
		ProgressBarBuilder progressBar = new ProgressBarBuilder();
		progressBar.setTaskName(taskName(stepName));
		progressBar.setStyle(progressBarStyle());
		progressBar.setUpdateIntervalMillis(progressUpdateInterval);
		progressBar.showSpeed();
		if (progressStyle == ProgressStyle.LOG) {
			Logger logger = LoggerFactory.getLogger(getClass());
			progressBar.setConsumer(new DelegatingProgressBarConsumer(logger::info));
		}
		ProgressStepExecutionListener listener = new ProgressStepExecutionListener(progressBar);
		listener.setExtraMessageSupplier(extraMessageSupplier(stepName, reader, writer));
		listener.setInitialMaxSupplier(initialMaxSupplier(stepName, reader));
		step.listener((StepExecutionListener) listener);
		step.listener((ItemWriteListener<?>) listener);
	}

	protected String taskName(String stepName) {
		return ClassUtils.getShortName(getClass());
	}

	protected Supplier<String> extraMessageSupplier(String stepName, ItemReader<?> reader, ItemWriter<?> writer) {
		return () -> ProgressStepExecutionListener.EMPTY_STRING;
	}

	protected LongSupplier initialMaxSupplier(String stepName, ItemReader<?> reader) {
		return () -> ProgressStepExecutionListener.UNKNOWN_SIZE;
	}

	protected abstract AbstractRedisCallable runnable();

}
