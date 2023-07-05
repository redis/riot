package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.listener.ChunkListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.TaskletStep;

import com.redis.riot.cli.common.JobOptions.ProgressStyle;
import com.redis.spring.batch.reader.ReaderOptions;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class StepProgressMonitor {

	public static final long DEFAULT_INITIAL_MAX = -1;
	public static final ProgressStyle DEFAULT_STYLE = ProgressStyle.ASCII;

	private ProgressStyle style = DEFAULT_STYLE;
	private String task;
	private Duration updateInterval = Duration.ofMillis(300);
	private int chunkSize = ReaderOptions.DEFAULT_CHUNK_SIZE;
	private LongSupplier initialMax = () -> DEFAULT_INITIAL_MAX;
	private Optional<Supplier<String>> extraMessage = Optional.empty();
	private boolean showSpeed;

	public StepProgressMonitor withStyle(ProgressStyle style) {
		this.style = style;
		return this;
	}

	public StepProgressMonitor withTask(String task) {
		this.task = task;
		return this;
	}

	public StepProgressMonitor withChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
		return this;
	}

	public StepProgressMonitor withUpdateInterval(Duration interval) {
		this.updateInterval = interval;
		return this;
	}

	public StepProgressMonitor withShowSpeed(boolean showSpeed) {
		this.showSpeed = showSpeed;
		return this;
	}

	public StepProgressMonitor withInitialMax(long max) {
		return withInitialMax(() -> max);
	}

	public StepProgressMonitor withInitialMax(LongSupplier max) {
		this.initialMax = max;
		return this;
	}

	public StepProgressMonitor withExtraMessage(Supplier<String> message) {
		this.extraMessage = Optional.of(message);
		return this;
	}

	public StepProgressMonitor withExtraMessage(String message) {
		return withExtraMessage(() -> message);
	}

	protected int updateIntervalMillis() {
		return Math.toIntExact(updateInterval.toMillis());
	}

	protected ProgressBarStyle progressBarStyle() {
		switch (style) {
		case BAR:
			return ProgressBarStyle.COLORFUL_UNICODE_BAR;
		case BLOCK:
			return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
		default:
			return ProgressBarStyle.ASCII;
		}
	}

	public static class ProgressReporter extends ChunkListenerSupport implements AutoCloseable {

		protected final ProgressBar progressBar;
		private final int chunkSize;

		public ProgressReporter(ProgressBar progressBar, int chunkSize) {
			this.progressBar = progressBar;
			this.chunkSize = chunkSize;
		}

		@Override
		public void afterChunk(ChunkContext context) {
			super.afterChunk(context);
			progressBar.stepBy(chunkSize);
		}

		@Override
		public void close() {
			progressBar.stepTo(progressBar.getMax());
			progressBar.close();
		}
	}

	public static class ExtraMessageProgressReporter extends ProgressReporter {

		private final Supplier<String> extraMessage;

		public ExtraMessageProgressReporter(ProgressBar progressBar, int chunkSize, Supplier<String> extraMessage) {
			super(progressBar, chunkSize);
			this.extraMessage = extraMessage;
		}

		@Override
		public void afterChunk(ChunkContext context) {
			super.afterChunk(context);
			progressBar.setExtraMessage(extraMessage.get());
		}

	}

	private class ReportingStepExecutionListener implements StepExecutionListener {

		private final TaskletStep step;
		private ProgressReporter reporter;

		public ReportingStepExecutionListener(TaskletStep step) {
			this.step = step;
		}

		@Override
		public void beforeStep(StepExecution stepExecution) {
			ProgressBarBuilder builder = new ProgressBarBuilder();
			builder.setStyle(progressBarStyle());
			builder.setInitialMax(initialMax.getAsLong());
			builder.setUpdateIntervalMillis(updateIntervalMillis());
			builder.setTaskName(task);
			if (showSpeed) {
				builder.showSpeed();
			}
			reporter = reporter(builder.build());
			step.registerChunkListener(reporter);
		}

		private ProgressReporter reporter(ProgressBar progressBar) {
			if (extraMessage.isPresent()) {
				return new ExtraMessageProgressReporter(progressBar, chunkSize, extraMessage.get());
			}
			return new ProgressReporter(progressBar, chunkSize);
		}

		@Override
		public synchronized ExitStatus afterStep(StepExecution stepExecution) {
			if (stepExecution.getStatus() != BatchStatus.FAILED) {
				reporter.close();
			}
			return null;
		}

	}

	public void register(TaskletStep step) {
		if (style == ProgressStyle.NONE) {
			return;
		}
		step.registerStepExecutionListener(new ReportingStepExecutionListener(step));
	}
}