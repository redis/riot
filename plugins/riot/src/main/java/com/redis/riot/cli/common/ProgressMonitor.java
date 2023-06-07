package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class ProgressMonitor implements StepExecutionListener, ItemWriteListener<Object> {

	public static final long DEFAULT_INITIAL_MAX = -1;

	private final ProgressBarStyle style;
	private final String task;
	private final Duration updateInterval;
	private final LongSupplier initialMax;
	protected ProgressBar progressBar;

	private ProgressMonitor(Builder builder) {
		this.style = builder.style;
		this.task = builder.task;
		this.updateInterval = builder.updateInterval;
		this.initialMax = builder.initialMax;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		ProgressBarBuilder builder = new ProgressBarBuilder();
		builder.setStyle(progressBarStyle());
		builder.setInitialMax(initialMax.getAsLong());
		builder.setUpdateIntervalMillis(Math.toIntExact(updateInterval.toMillis()));
		builder.setTaskName(task);
		builder.showSpeed();
		progressBar = builder.build();
	}

	private ProgressBarStyle progressBarStyle() {
		return style;
	}

	@Override
	public synchronized ExitStatus afterStep(StepExecution stepExecution) {
		if (stepExecution.getStatus() != BatchStatus.FAILED) {
			progressBar.close();
		}
		return null;
	}

	@Override
	public void beforeWrite(List<? extends Object> items) {
		// do nothing
	}

	@Override
	public void afterWrite(List<? extends Object> items) {
		progressBar.stepBy(items.size());
	}

	@Override
	public void onWriteError(Exception exception, List<? extends Object> items) {
		// do nothing
	}

	private static class ExtraMessageProgressMonitor extends ProgressMonitor {

		private final Supplier<String> extraMessage;

		public ExtraMessageProgressMonitor(Builder builder) {
			super(builder);
			this.extraMessage = builder.extraMessage.get();
		}

		@Override
		public void afterWrite(List<? extends Object> items) {
			super.afterWrite(items);
			progressBar.setExtraMessage(extraMessage.get());
		}

	}

	public static Builder style(ProgressBarStyle style) {
		return new Builder(style);
	}

	public static class Builder {

		private final ProgressBarStyle style;
		private String task;
		private Duration updateInterval = Duration.ofMillis(300);
		private LongSupplier initialMax = () -> DEFAULT_INITIAL_MAX;
		private Optional<Supplier<String>> extraMessage = Optional.empty();

		public Builder(ProgressBarStyle style) {
			this.style = style;
		}

		public Builder task(String task) {
			this.task = task;
			return this;
		}

		public Builder updateInterval(Duration updateInterval) {
			this.updateInterval = updateInterval;
			return this;
		}

		public Builder initialMax(LongSupplier initialMax) {
			this.initialMax = initialMax;
			return this;
		}

		public Builder initialMax(long initialMax) {
			return initialMax(() -> initialMax);
		}

		public Builder extraMessage(Supplier<String> extraMessage) {
			this.extraMessage = Optional.of(extraMessage);
			return this;
		}

		public ProgressMonitor build() {
			if (extraMessage.isPresent()) {
				return new ExtraMessageProgressMonitor(this);
			}
			return new ProgressMonitor(this);
		}

	}

}